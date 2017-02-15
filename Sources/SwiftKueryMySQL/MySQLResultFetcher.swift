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
    private static let dateFormatter = getDateFormatter(format: "yyyy-MM-dd")
    private static let timeFormatter = getDateFormatter(format: "HH:mm:ss")
    private static let dateTimeFormatter = getDateFormatter(format: "yyyy-MM-dd HH:mm:ss")

    // private let encoding: String.Encoding?
    private var statement: UnsafeMutablePointer<MYSQL_STMT>?
    private let bindPtr: UnsafeMutablePointer<MYSQL_BIND>
    private let binds: [MYSQL_BIND]

    private let fieldNames: [String]

    private var row: [Any?]?
    private var hasMoreRows = true

    init?(statement: UnsafeMutablePointer<MYSQL_STMT>, bindPtr: UnsafeMutablePointer<MYSQL_BIND>, binds: [MYSQL_BIND], fieldNames: [String]) {
        self.statement = statement
        self.bindPtr = bindPtr
        self.binds = binds
        self.fieldNames = fieldNames

        self.row = buildRow()
        if row == nil {
            close()
            return nil
        }
    }

    deinit {
        close()
    }

    private func close() {
        guard statement != nil else {
            return
        }

        mysql_stmt_close(statement)
        statement = nil

        bindPtr.deallocate(capacity: binds.count)

        for bind in binds {
            bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: MemoryLayout<Int8>.alignment)
            bind.length.deallocate(capacity: 1)
            bind.is_null.deallocate(capacity: 1)
            bind.error.deallocate(capacity: 1)
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
        guard mysql_stmt_fetch(statement) == 0 else {
            return nil
        }

        var row = [Any?]()
        for i in 0 ..< binds.count {
            let bind = binds[i]
            guard let buffer = bind.buffer else { // UnsafeMutableRawPointer
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
                row.append(String(cString: buffer.assumingMemoryBound(to: CChar.self)))
                // row.append(String(cString: buffer.bindMemory(to: CChar.self, capacity: bind.length)))
            case MYSQL_TYPE_TINY_BLOB,
                 MYSQL_TYPE_BLOB,
                 MYSQL_TYPE_MEDIUM_BLOB,
                 MYSQL_TYPE_LONG_BLOB,
                 MYSQL_TYPE_BIT:
                row.append(nil) // TODO
            case MYSQL_TYPE_TIME,
                 MYSQL_TYPE_DATE,
                 MYSQL_TYPE_DATETIME,
                 MYSQL_TYPE_TIMESTAMP:
                row.append(nil) // MYSQL_TIME
            default:
                row.append("Unhandled enum_field_type: \(type.rawValue)")
            }
        }

        return row
    }

    static func getDateFormatter(format: String) -> DateFormatter {
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = format
        return dateFormatter
    }
}
