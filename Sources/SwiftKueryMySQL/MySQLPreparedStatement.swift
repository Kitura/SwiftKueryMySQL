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

import CMySQL

/// MySQL implementation for prepared statements.
public class MySQLPreparedStatement: PreparedStatement {
    internal(set) var statement: UnsafeMutablePointer<MYSQL_STMT>?
    internal let query: Query?

    private var binds = [MYSQL_BIND]()
    internal var bindsCapacity = 0
    internal var bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil
    private var mysql: UnsafeMutablePointer<MYSQL>?

    init(query: Query? = nil, mysql: UnsafeMutablePointer<MYSQL>?, statement: UnsafeMutablePointer<MYSQL_STMT>?) {
        self.mysql = mysql
        self.statement = statement
        self.query = query
    }

    deinit {
        release { _ in
            return
        }
    }

    func release(onCompletion: @escaping ((QueryResult) -> ())) {
        deallocateBinds()

        if let statement = self.statement {
            self.statement = nil
            mysql_stmt_close(statement)
        }
        onCompletion(.successNoData)
    }

    internal func getError(_ statement: UnsafeMutablePointer<MYSQL_STMT>) -> String {
        return "ERROR \(mysql_stmt_errno(statement)): " + String(cString: mysql_stmt_error(statement))
    }

    internal func allocateBinds(parameters: [Any?]) -> Bool {
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

        guard mysql_stmt_bind_param(statement, bindPtr) == mysql_false() else {
            return false
        }
        return true
    }

    private func deallocateBinds() {
        guard let bindPtr = self.bindPtr else {
            return
        }

        self.bindPtr = nil

        for bind in binds {
            if bind.buffer != nil {
                #if swift(>=4.1)
                bind.buffer.deallocate()
                #else
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                #endif
            }
            if bind.length != nil {
                #if swift(>=4.1)
                bind.length.deallocate()
                #else
                bind.length.deallocate(capacity: 1)
                #endif
            }
            if bind.is_null != nil {
                #if swift(>=4.1)
                bind.is_null.deallocate()
                #else
                bind.is_null.deallocate(capacity: 1)
                #endif
            }
        }
        #if swift(>=4.1)
        bindPtr.deallocate()
        #else
        bindPtr.deallocate(capacity: bindsCapacity)
        #endif
        binds.removeAll()
    }

    private func setBind(_ bind: inout MYSQL_BIND, _ parameter: Any?, _ column: Column?) {
        if bind.is_null == nil {
            bind.is_null = UnsafeMutablePointer<mysql_bool>.allocate(capacity: 1)
        }

        guard let parameter = parameter else {
            bind.buffer_type = MYSQL_TYPE_NULL
            bind.is_null.initialize(to: mysql_true())
            return
        }

        bind.buffer_type = getType(parameter: parameter)
        bind.is_null.initialize(to: mysql_false())
        bind.is_unsigned = mysql_false()

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
            bind.is_unsigned = mysql_true()
        case let uint as UInt8:
            initialize(uint, &bind)
            bind.is_unsigned = mysql_true()
        case let uint as UInt16:
            initialize(uint, &bind)
            bind.is_unsigned = mysql_true()
        case let uint as UInt32:
            initialize(uint, &bind)
            bind.is_unsigned = mysql_true()
        case let uint as UInt64:
            initialize(uint, &bind)
            bind.is_unsigned = mysql_true()
        case let unicodeScalar as UnicodeScalar:
            initialize(unicodeScalar, &bind)
            bind.is_unsigned = mysql_true()
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
                #if swift(>=4.1)
                bind.buffer.deallocate()
                #else
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                #endif
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
