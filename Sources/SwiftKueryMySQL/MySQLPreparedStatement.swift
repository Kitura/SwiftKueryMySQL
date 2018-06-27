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

import Foundation
import SwiftKuery

#if os(Linux)
    import CmySQLlinux
#else
    import CmySQLosx
#endif

/// MySQL implementation for prepared statements.
public class MySQLPreparedStatement: PreparedStatement {
    private(set) var statement: UnsafeMutablePointer<MYSQL_STMT>?
    private let query: Query?

    private var binds = [MYSQL_BIND]()
    private var bindsCapacity = 0
    private var bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil
    private var mysql: UnsafeMutablePointer<MYSQL>?

    init(_ raw: String, query: Query? = nil, mysql: UnsafeMutablePointer<MYSQL>?) throws {
        guard let mysql = mysql else {
            throw QueryError.connection("Not connected, call connect() before execute()")
        }

        guard let statement = mysql_stmt_init(mysql) else {
            throw QueryError.connection(MySQLConnection.getError(mysql))
        }

        guard mysql_stmt_prepare(statement, raw, UInt(raw.utf8.count)) == 0 else {
            defer {
                mysql_stmt_close(statement)
            }
            throw QueryError.syntaxError(MySQLConnection.getError(statement))
        }

        self.mysql = mysql
        self.statement = statement
        self.query = query
    }

    deinit {
        release()
    }

    func release() {
        deallocateBinds()

        if let statement = self.statement {
            self.statement = nil
            mysql_stmt_close(statement)
        }
    }

    func execute(parameters: [Any?]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let statement = self.statement else {
            onCompletion(.error(QueryError.connection("PreparedStatement release() has already been called.")))
            return
        }

        if let parameters = parameters {
            if let bindPtr = bindPtr {
                if bindsCapacity != parameters.count {
                    onCompletion(.error(QueryError.databaseError("Each of multiple execute() calls must pass the same number of parameters.")))
                    return
                }
            } else { // true only for the first time execute() is called for this PreparedStatement
                bindsCapacity = parameters.count
                bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: bindsCapacity)
            }

            do {
                try allocateBinds(parameters: parameters)
            } catch {
                self.statement = nil
                mysql_stmt_close(statement)
                onCompletion(.error(error))
                return
            }
        }

        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            // non-query statement (insert, update, delete)

            guard mysql_stmt_execute(statement) == 0 else {
                handleError(onCompletion: onCompletion)
                return
            }

            do {
              if query != nil, let insertQuery = query as? Insert, insertQuery.returnID {
                guard let idColumn = insertQuery.table.columns.first(where: {$0.isPrimaryKey && $0.autoIncrement}) else {
                  throw QueryError.syntaxError("Could not retrieve ID Column in order to return the ID value")
                }

                try MySQLPreparedStatement("SELECT LAST_INSERT_ID() AS \(idColumn.name)", mysql: self.mysql).execute(onCompletion: onCompletion)
                return
              }
            } catch {
              onCompletion(.error(error))
            }

            let affectedRows = mysql_stmt_affected_rows(statement)
            onCompletion(.success("\(affectedRows) rows affected"))
            return
        }

        defer {
            mysql_free_result(resultMetadata)
        }

        do {
            let resultFetcher = try MySQLResultFetcher(preparedStatement: self, resultMetadata: resultMetadata)
            onCompletion(.resultSet(ResultSet(resultFetcher)))
        } catch {
            onCompletion(.error(error))
        }
    }

    private func handleError(onCompletion: @escaping ((QueryResult) -> ())) {
        guard let statement = self.statement else {
            return
        }
        self.statement = nil
        onCompletion(.error(QueryError.databaseError(MySQLConnection.getError(statement))))
        mysql_stmt_close(statement)
    }

    private func allocateBinds(parameters: [Any?]) throws {
        var cols: [Column]?
        switch query {
        case let insert as Insert:
            cols = insert.columns ?? insert.table.columns
        case let update as Update:
            cols = update.table.columns
        default:
            break
        }

        let columns: [Column]?
        if cols?.count == parameters.count {
            columns = cols
        } else {
            columns = nil
        }

        if binds.isEmpty { // first parameter set, create new bind and bind it to the parameter
            for (index, parameter) in parameters.enumerated() {
                var bind = MYSQL_BIND()
                setBind(&bind, parameter, columns?[index])
                binds.append(bind)
                bindPtr![index] = bind
            }
        } else { // bind was previously created, re-initialize value
            for (index, parameter) in parameters.enumerated() {
                var bind = binds[index]
                setBind(&bind, parameter, columns?[index])
                binds[index] = bind
                bindPtr![index] = bind
            }
        }

        guard mysql_stmt_bind_param(statement, bindPtr) == 0 else {
            throw QueryError.databaseError(MySQLConnection.getError(statement!))
        }
    }

    private func deallocateBinds() {
        guard let bindPtr = self.bindPtr else {
            return
        }

        self.bindPtr = nil

        for bind in binds {
            if bind.buffer != nil {
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
            }
            if bind.length != nil {
                bind.length.deallocate(capacity: 1)
            }
            if bind.is_null != nil {
                bind.is_null.deallocate(capacity: 1)
            }
        }
        bindPtr.deallocate(capacity: bindsCapacity)
        binds.removeAll()
    }

    private func setBind(_ bind: inout MYSQL_BIND, _ parameter: Any?, _ column: Column?) {
        if bind.is_null == nil {
            bind.is_null = UnsafeMutablePointer<Int8>.allocate(capacity: 1)
        }

        guard let parameter = parameter else {
            bind.buffer_type = MYSQL_TYPE_NULL
            bind.is_null.initialize(to: 1)
            return
        }

        bind.buffer_type = getType(parameter: parameter)
        bind.is_null.initialize(to: 0)
        bind.is_unsigned = 0

        switch parameter {
        case let string as String:
            initialize(string: string, &bind)
        case let date as Date:
            let formatter: DateFormatter
            switch column?.type {
            case is SQLDate.Type:
                formatter = MySQLConnection.dateFormatter
            case is Time.Type:
                formatter = MySQLConnection.timeFormatter
            default:
                formatter = MySQLConnection.dateTimeFormatter
            }
            let formattedDate = formatter.string(from: date)
            initialize(string: formattedDate, &bind)
        case let byteArray as [UInt8]:
            let typedBuffer = allocate(type: UInt8.self, capacity: byteArray.count, bind: &bind)
            typedBuffer.initialize(from: byteArray, count: byteArray.count)
        case let data as Data:
            let typedBuffer = allocate(type: UInt8.self, capacity: data.count, bind: &bind)
            data.copyBytes(to: typedBuffer, count: data.count)
        case let dateTime as MYSQL_TIME:
            initialize(dateTime, &bind)
        case let float as Float:
            initialize(float, &bind)
        case let double as Double:
            initialize(double, &bind)
        case let bool as Bool:
            initialize(bool, &bind)
        case let int as Int:
            initialize(int, &bind)
        case let int as Int8:
            initialize(int, &bind)
        case let int as Int16:
            initialize(int, &bind)
        case let int as Int32:
            initialize(int, &bind)
        case let int as Int64:
            initialize(int, &bind)
        case let uint as UInt:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt8:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt16:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt32:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let uint as UInt64:
            initialize(uint, &bind)
            bind.is_unsigned = 1
        case let unicodeScalar as UnicodeScalar:
            initialize(unicodeScalar, &bind)
            bind.is_unsigned = 1
        default:
            print("WARNING: Unhandled parameter \(parameter) (type: \(type(of: parameter))). Will attempt to convert it to a String")
            initialize(string: String(describing: parameter), &bind)
        }
    }

    private func allocate<T>(type: T.Type, capacity: Int, bind: inout MYSQL_BIND) -> UnsafeMutablePointer<T> {

        let length = UInt(capacity * MemoryLayout<T>.size)

        if bind.length == nil {
            bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        }
        bind.length.initialize(to: length)

        let typedBuffer: UnsafeMutablePointer<T>
        if let buffer = bind.buffer, bind.buffer_length >= length {
            typedBuffer = buffer.assumingMemoryBound(to: type)
        } else {
            if bind.buffer != nil {
                // deallocate existing smaller buffer
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
            }

            typedBuffer = UnsafeMutablePointer<T>.allocate(capacity: capacity)
            bind.buffer = UnsafeMutableRawPointer(typedBuffer)
            bind.buffer_length = length
        }

        return typedBuffer
    }

    private func initialize<T>(_ parameter: T, _ bind: inout MYSQL_BIND) {
        let typedBuffer = allocate(type: type(of: parameter), capacity: 1, bind: &bind)
        typedBuffer.initialize(to: parameter)
    }

    private func initialize(string: String, _ bind: inout MYSQL_BIND) {
        let utf8 = Array(string.utf8)
        let typedBuffer = allocate(type: UInt8.self, capacity: utf8.count, bind: &bind)
        typedBuffer.initialize(from: utf8, count: utf8.count)
    }

    private func getType(parameter: Any) -> enum_field_types {
        switch parameter {
        case is String,
             is Date:
            return MYSQL_TYPE_STRING
        case is Data,
             is [UInt8]:
            return MYSQL_TYPE_BLOB
        case is Int8,
             is UInt8,
             is Bool:
            return MYSQL_TYPE_TINY
        case is Int16,
             is UInt16:
            return MYSQL_TYPE_SHORT
        case is Int32,
             is UInt32,
             is UnicodeScalar:
            return MYSQL_TYPE_LONG
        case is Int,
             is UInt,
             is Int64,
             is UInt64:
            return MYSQL_TYPE_LONGLONG
        case is Float:
            return MYSQL_TYPE_FLOAT
        case is Double:
            return MYSQL_TYPE_DOUBLE
        case is MYSQL_TIME:
            return MYSQL_TYPE_DATETIME
        default:
            return MYSQL_TYPE_STRING
        }
    }
}
