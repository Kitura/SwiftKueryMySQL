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
    private var preparedStatement: MySQLPreparedStatement
    private var bindPtr: UnsafeMutablePointer<MYSQL_BIND>?
    private let binds: [MYSQL_BIND]

    private let fieldNames: [String]
    private let charsetnr: [UInt32]

    private var hasMoreRows = true

    init(preparedStatement: MySQLPreparedStatement, resultMetadata: UnsafeMutablePointer<MYSQL_RES>) throws {
        guard let fields = mysql_fetch_fields(resultMetadata) else {
            throw MySQLResultFetcher.initError(preparedStatement)
        }

        let numFields = Int(mysql_num_fields(resultMetadata))
        var binds = [MYSQL_BIND]()
        var fieldNames = [String]()
        var charsetnr = [UInt32]()

        for i in 0 ..< numFields {
            let field = fields[i]
            binds.append(MySQLResultFetcher.getOutputBind(field))
            fieldNames.append(String(cString: field.name))
            charsetnr.append(field.charsetnr)
        }

        let bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
        for i in 0 ..< binds.count {
            bindPtr[i] = binds[i]
        }

        guard mysql_stmt_bind_result(preparedStatement.statement, bindPtr) == 0 else {
            throw MySQLResultFetcher.initError(preparedStatement, bindPtr: bindPtr, binds: binds)
        }

        guard mysql_stmt_execute(preparedStatement.statement) == 0 else {
            throw MySQLResultFetcher.initError(preparedStatement, bindPtr: bindPtr, binds: binds)
        }

        self.preparedStatement = preparedStatement
        self.bindPtr = bindPtr
        self.binds = binds
        self.fieldNames = fieldNames
        self.charsetnr = charsetnr
    }

    deinit {
        close()
    }

    private static func initError(_ preparedStatement: MySQLPreparedStatement, bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil, binds: [MYSQL_BIND]? = nil) -> QueryError {

        defer {
            preparedStatement.release()
        }

        if let binds = binds {
            for bind in binds {
                
                #if swift(>=4.1)
                bind.buffer.deallocate()
                bind.length.deallocate()
                bind.is_null.deallocate()
                bind.error.deallocate()
                #else
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
                #endif
            }

            if let bindPtr = bindPtr {
                #if swift(>=4.1)
                bindPtr.deallocate()
                #else
                bindPtr.deallocate(capacity: binds.count)
                #endif
            }
        }

        return QueryError.databaseError(MySQLConnection.getError(preparedStatement.statement!))
    }

    private func close() {
        if let bindPtr = bindPtr {
            self.bindPtr = nil

            for bind in binds {
                #if swift(>=4.1)
                bind.buffer.deallocate()
                bind.length.deallocate()
                bind.is_null.deallocate()
                bind.error.deallocate()
                #else
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
                #endif
            }
            #if swift(>=4.1)
            bindPtr.deallocate()
            #else
            bindPtr.deallocate(capacity: binds.count)
            #endif

            preparedStatement.release()
        }
    }

    /// Fetch the next row of the query result. This function is blocking.
    ///
    /// - Returns: An array of values of type Any? representing the next row from the query result.
    public func fetchNext() -> [Any?]? {
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

    private static func getOutputBind(_ field: MYSQL_FIELD) -> MYSQL_BIND {
        let size = getSize(field: field)

        var bind = MYSQL_BIND()
        bind.buffer_type = field.type
        bind.buffer_length = UInt(size)
        bind.is_unsigned = 0

        #if swift(>=4.1)
        bind.buffer = UnsafeMutableRawPointer.allocate(byteCount: size, alignment: 1)
        #else
        bind.buffer = UnsafeMutableRawPointer.allocate(bytes: size, alignedTo: 1)
        #endif
        
        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.error = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)

        return bind
    }

    private static func getSize(field: MYSQL_FIELD) -> Int {
        switch field.type {
        case MYSQL_TYPE_TINY:
            return MemoryLayout<Int8>.size
        case MYSQL_TYPE_SHORT:
            return MemoryLayout<Int16>.size
        case MYSQL_TYPE_INT24,
             MYSQL_TYPE_LONG:
            return MemoryLayout<Int32>.size
        case MYSQL_TYPE_LONGLONG:
            return MemoryLayout<Int64>.size
        case MYSQL_TYPE_FLOAT:
            return MemoryLayout<Float>.size
        case MYSQL_TYPE_DOUBLE:
            return MemoryLayout<Double>.size
        case MYSQL_TYPE_TIME,
             MYSQL_TYPE_DATE,
             MYSQL_TYPE_DATETIME,
             MYSQL_TYPE_TIMESTAMP:
            return MemoryLayout<MYSQL_TIME>.size
        default:
            return Int(field.length)
        }
    }

    private func buildRow() -> [Any?]? {
        let fetchStatus = mysql_stmt_fetch(preparedStatement.statement)
        if fetchStatus == MYSQL_NO_DATA {
            return nil
        }

        if fetchStatus == 1 {
            // use a logger or add throws to the fetchNext signature?
            print("ERROR: while fetching row: \(MySQLConnection.getError(preparedStatement.statement!))")
            return nil
        }

        var row = [Any?]()
        for (index, bind) in binds.enumerated() {
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
                row.append(buffer.load(as: Int8.self))
            case MYSQL_TYPE_SHORT:
                row.append(buffer.load(as: Int16.self))
            case MYSQL_TYPE_INT24,
                 MYSQL_TYPE_LONG:
                row.append(buffer.load(as: Int32.self))
            case MYSQL_TYPE_LONGLONG:
                row.append(buffer.load(as: Int64.self))
            case MYSQL_TYPE_FLOAT:
                row.append(buffer.load(as: Float.self))
            case MYSQL_TYPE_DOUBLE:
                row.append(buffer.load(as: Double.self))
            case MYSQL_TYPE_NEWDECIMAL,
                 MYSQL_TYPE_STRING,
                 MYSQL_TYPE_VAR_STRING:
                row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: .utf8, freeWhenDone: false))
            case MYSQL_TYPE_TINY_BLOB,
                 MYSQL_TYPE_BLOB,
                 MYSQL_TYPE_MEDIUM_BLOB,
                 MYSQL_TYPE_LONG_BLOB:
                if charsetnr[index] == 63 {
                  // Value 63 is used to denote binary data
                  // see https://dev.mysql.com/doc/refman/5.7/en/c-api-prepared-statement-type-conversions.html
                  row.append(Data(bytes: buffer, count: getLength(bind)))
                } else { 
                  // We are assuming that the returned data
                  // is encoded in UTF-8
                  row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: .utf8, freeWhenDone: false))
                }
            case MYSQL_TYPE_BIT:
                row.append(Data(bytes: buffer, count: getLength(bind)))
            case MYSQL_TYPE_TIME:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(pad(time.hour)):\(pad(time.minute)):\(pad(time.second))")
            case MYSQL_TYPE_DATE:
                let time = buffer.load(as: MYSQL_TIME.self)
                row.append("\(time.year)-\(pad(time.month))-\(pad(time.day))")
            case MYSQL_TYPE_DATETIME,
                 MYSQL_TYPE_TIMESTAMP:
                let time = buffer.load(as: MYSQL_TIME.self)
                let formattedDate = "\(time.year)-\(time.month)-\(time.day) \(time.hour):\(time.minute):\(time.second)"
                row.append(MySQLConnection.dateTimeFormatter.date(from: formattedDate))
            default:
                print("Using string for unhandled enum_field_type: \(type.rawValue)")
                row.append(String(bytesNoCopy: buffer, length: getLength(bind), encoding: .utf8, freeWhenDone: false))
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
}
