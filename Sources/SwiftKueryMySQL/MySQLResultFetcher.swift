/**
 Copyright IBM Corporation 2017
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import SwiftKuery
import Foundation

#if os(Linux)
    import CmySQLlinux
#else
    import CmySQLosx
#endif

/// An implementation of query result fetcher.

public class MySQLResultFetcher: ResultFetcher {
    // private let encoding: String.Encoding?
    private var statement: UnsafeMutablePointer<MYSQL_STMT>
    private var bindPtr: UnsafeMutablePointer<MYSQL_BIND>?
    private let binds: [MYSQL_BIND]

    private let fieldNames: [String]
    private let copyBlobData: Bool

    private var row: [Any?]?
    private var hasMoreRows = true

    init?(statement: UnsafeMutablePointer<MYSQL_STMT>, copyBlobData: Bool) throws {

        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            throw MySQLResultFetcher.initError(statement)
        }

        guard let fields = mysql_fetch_fields(resultMetadata) else {
            mysql_free_result(resultMetadata)
            throw MySQLResultFetcher.initError(statement)
        }

        let numFields = Int(mysql_num_fields(resultMetadata))
        var binds = [MYSQL_BIND]()
        var fieldNames = [String]()

        for i in 0 ..< numFields {
            let field = fields[i]
            binds.append(MySQLConnection.getOutputBind(field))
            fieldNames.append(String(cString: field.name))
        }

        mysql_free_result(resultMetadata)

        let bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
        for i in 0 ..< binds.count {
            bindPtr[i] = binds[i]
        }

        guard mysql_stmt_bind_result(statement, bindPtr) == 0 else {
            throw MySQLResultFetcher.initError(statement, bindPtr: bindPtr, binds: binds)
        }

        guard mysql_stmt_execute(statement) == 0 else {
            throw MySQLResultFetcher.initError(statement, bindPtr: bindPtr, binds: binds)
        }

        self.statement = statement
        self.bindPtr = bindPtr
        self.binds = binds
        self.fieldNames = fieldNames
        self.copyBlobData = copyBlobData

        self.row = buildRow()
        if self.row == nil {
            close()
            return nil
        }
    }

    deinit {
        close()
    }

    private static func initError(_ statement: UnsafeMutablePointer<MYSQL_STMT>, bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil, binds: [MYSQL_BIND]? = nil) -> QueryError {

        defer {
            mysql_stmt_close(statement)
        }

        if let binds = binds {
            for bind in binds {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
            }

            if let bindPtr = bindPtr {
                bindPtr.deallocate(capacity: binds.count)
            }
        }

        return QueryError.databaseError(getError(statement))
    }

    private func close() {
        if let bindPtr = bindPtr {
            self.bindPtr = nil

            for bind in binds {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
            }
            bindPtr.deallocate(capacity: binds.count)

            mysql_stmt_close(statement)
        }
    }

    /// Fetch the next row of the query result. This function is blocking.
    ///
    /// - Returns: An array of values of type Any? representing the next row from the query result.
    public func fetchNext() -> [Any?]? {
        if let row = row { // row built in init
            self.row = nil
            return row
        }

        guard hasMoreRows else {
            return nil
        }

        if let row = buildRow() {
            return row
        } else {
            hasMoreRows = false
            close()
            return nil
        }
    }

    /// Fetch the next row of the query result. This function is non-blocking.
    ///
    /// - Parameter callback: A callback to call when the next row of the query result is ready.
    public func fetchNext(callback: ([Any?]?) ->()) {
        // For now
        callback(fetchNext())
    }

    /// Fetch the titles of the query result.
    ///
    /// - Returns: An array of column titles of type String.
    public func fetchTitles() -> [String] {
        return fieldNames
    }

    private func buildRow() -> [Any?]? {
        let fetchStatus = mysql_stmt_fetch(statement)
        if fetchStatus == MYSQL_NO_DATA {
            return nil
        }

        if fetchStatus == 1 {
            // use a logger or add throws to the fetchNext signature?
            print("Error fetching row: \(MySQLResultFetcher.getError(statement))")
            return nil
        }

        var row = [Any?]()
        for bind in binds {
            guard let buffer = bind.buffer else {
                row.append("bind buffer not set")
                continue
            }

            guard bind.is_null.pointee == 0 else {
                row.append(nil)
                continue
            }

            let type = bind.buffer_type
            switch type {
            case MYSQL_TYPE_TINY:
                row.append(buffer.load(as: CChar.self))
            case MYSQL_TYPE_SHORT:
                row.append(buffer.load(as: CShort.self))
            case MYSQL_TYPE_INT24,
                 MYSQL_TYPE_LONG:
                row.append(buffer.load(as: CInt.self))
            case MYSQL_TYPE_LONGLONG:
                row.append(buffer.load(as: CLongLong.self))
            case MYSQL_TYPE_FLOAT:
                row.append(buffer.load(as: CFloat.self))
            case MYSQL_TYPE_DOUBLE:
                row.append(buffer.load(as: CDouble.self))
            case MYSQL_TYPE_NEWDECIMAL,
                 MYSQL_TYPE_STRING,
                 MYSQL_TYPE_VAR_STRING:
                row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: String.Encoding.utf8, freeWhenDone: false))
            case MYSQL_TYPE_TINY_BLOB,
                 MYSQL_TYPE_BLOB,
                 MYSQL_TYPE_MEDIUM_BLOB,
                 MYSQL_TYPE_LONG_BLOB,
                 MYSQL_TYPE_BIT:
                if copyBlobData {
                    row.append(Data(bytes: buffer, count: getLength(bind)))
                } else {
                    row.append(Data(bytesNoCopy: buffer, count: getLength(bind), deallocator: Data.Deallocator.none))
                }
            case MYSQL_TYPE_TIME:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(pad(time.hour)):\(pad(time.minute)):\(pad(time.second))")
            case MYSQL_TYPE_DATE:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(time.year)-\(pad(time.month))-\(pad(time.day))")
            case MYSQL_TYPE_DATETIME,
                 MYSQL_TYPE_TIMESTAMP:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(time.year)-\(pad(time.month))-\(pad(time.day)) \(pad(time.hour)):\(pad(time.minute)):\(pad(time.second))")
            default:
                row.append("Unhandled enum_field_type: \(type.rawValue)")
            }
        }

        return row
    }

    private func getLength(_ bind: MYSQL_BIND) -> Int {
        return Int(bind.length.pointee > bind.buffer_length ? bind.buffer_length : bind.length.pointee)
    }

    private func pad(_ uInt: UInt32) -> String {
        return String(format: "%02u", uInt)
    }

    private static func getError(_ statement: UnsafeMutablePointer<MYSQL_STMT>) -> String {
        return String(cString: mysql_stmt_error(statement))
    }
}
