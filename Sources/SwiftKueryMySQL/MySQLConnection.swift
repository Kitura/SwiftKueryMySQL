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

/// An implementation of `SwiftKuery.Connection` protocol for MySQL.
public class MySQLConnection: Connection {

    private static let initOnce: () = {
        mysql_server_init(0, nil, nil) // this call is not thread-safe
    }()

    private let host: String
    private let user: String
    private let password: String
    private let database: String
    private let port: UInt32
    private let unixSocket: String?
    private let clientFlag: UInt
    private let characterSet: String
    private let copyBlobData: Bool

    private var connection: UnsafeMutablePointer<MYSQL>?

    /// The `QueryBuilder` with MySQL specific substitutions.
    public let queryBuilder: QueryBuilder = {
        let queryBuilder = QueryBuilder(anyOnSubquerySupported: true)
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.len : "LENGTH"])
        return queryBuilder
    }()

    /// Initialize an instance of MySQLConnection.
    ///
    /// - Parameter host: host name or IP address of server to connect to, defaults to localhost
    /// - Parameter user: MySQL login ID, defaults to current user
    /// - Parameter password: password for `user`, defaults to no password
    /// - Parameter database: default database to use if specified
    /// - Parameter port: port number for the TCP/IP connection if using a non-standard port
    /// - Parameter unixSocket: unix domain socket or named pipe to use for connecting to server instead of TCP/IP
    /// - Parameter clientFlag: MySQL client options
    /// - Parameter copyBlobData: Whether or not to copy bytes to Data objects in QueryResult (defaults to true).
    ///               When false, the underlying buffer is reused for blobs in each row which can be faster for large blobs.
    ///               Do NOT set to false if you use queryResult.asRows or if you keep a reference to returned blob data objects.
    ///               Set to false only if you use queryResult.asResultSet and finish processing row blob data before moving to the next row.
    public init(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil, copyBlobData: Bool = true) {

        MySQLConnection.initOnce

        self.host = host ?? ""
        self.user = user ?? ""
        self.password = password ?? ""
        self.database = database ?? ""
        self.port = UInt32(port ?? 0)
        self.unixSocket = unixSocket
        self.clientFlag = clientFlag
        self.characterSet = characterSet ?? "utf8"
        self.copyBlobData = copyBlobData
    }

    /// Initialize an instance of MySQLConnection.
    ///
    /// - Parameter url: A URL with the connection information. For example, mysql://user:password@host:port/database
    public convenience init(url: URL) {
        self.init(host: url.host, user: url.user, password: url.password,
                  database: url.lastPathComponent, port: url.port)
    }

    deinit {
        closeConnection()
    }

    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: (QueryError?) -> ()) {
        if connection == nil {
            connection = mysql_init(nil)
        }

        if mysql_real_connect(connection, host, user, password, database, port, unixSocket, clientFlag) != nil {
            if mysql_set_character_set(connection, characterSet) == 0 {
                print("Set characterSet to: \(characterSet)")
            } else {
                let defaultCharSet = String(cString: mysql_character_set_name(connection))
                print("Invalid characterSet: \(characterSet), using: \(defaultCharSet)")
            }
            onCompletion(nil) // success
        } else {
            onCompletion(QueryError.connection(getError()))
        }
    }

    /// Close the connection to the database.
    public func closeConnection() {
        if connection != nil {
            mysql_close(connection)
            connection = nil
        }
    }

    /// Return a String representation of the query.
    ///
    /// - Parameter query: The query.
    /// - Returns: A String representation of the query.
    /// - Throws: QueryError.syntaxError if query build fails.
    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }

    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        if let query = build(query: query, onCompletion: onCompletion) {
            executeQuery(query: query, onCompletion: onCompletion)
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        if let query = build(query: query, onCompletion: onCompletion) {
            executeQuery(query: query, parameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a query with named parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        if let query = build(query: query, onCompletion: onCompletion) {
            executeQuery(query: query, namedParameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, parameters: parameters, onCompletion: onCompletion)
    }

    /// Execute a raw query with named parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, namedParameters: parameters, onCompletion: onCompletion)
    }

    private func build(query: Query, onCompletion: @escaping ((QueryResult) -> ())) -> String? {
        do {
            return try query.build(queryBuilder: queryBuilder)
        } catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        } catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }

        return nil
    }

    private func getError(_ statement: UnsafeMutablePointer<MYSQL_STMT>) -> String {
        return String(cString: mysql_stmt_error(statement))
    }

    private func getError() -> String {
        return String(cString: mysql_error(connection))
    }

    private func handleError(_ statement: UnsafeMutablePointer<MYSQL_STMT>, onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.databaseError(getError(statement))))
        mysql_stmt_close(statement)
    }

    private func executeQuery(query: String, parameters: [Any]? = nil, namedParameters: [String:Any]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        if var parameters = parameters {
            withUnsafePointer(to: &parameters) { parametersPtr in
                executeImpl(query: query, parametersPtr: parametersPtr, onCompletion: onCompletion)
            }
        } else if var namedParameters = namedParameters {
            withUnsafePointer(to: &namedParameters) { namedParametersPtr in
                executeImpl(query: query, namedParametersPtr: namedParametersPtr, onCompletion: onCompletion)
            }
        } else {
            executeImpl(query: query, onCompletion: onCompletion)
        }
    }

    private func executeImpl(query: String, parametersPtr: UnsafePointer<[Any]>? = nil, namedParametersPtr: UnsafePointer<[String:Any]>? = nil, onCompletion: @escaping ((QueryResult) -> ())) {

        guard let statement = mysql_stmt_init(connection) else {
            onCompletion(.error(QueryError.connection(getError())))
            return
        }

        guard mysql_stmt_prepare(statement, query, UInt(query.utf8.count)) == 0 else {
            onCompletion(.error(QueryError.syntaxError(getError(statement))))
            mysql_stmt_close(statement)
            return
        }

        var binds = [MYSQL_BIND]()
        var bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil

        defer {
            for bind in binds {
                print("dealloc \(String(bytesNoCopy: bind.buffer, length: Int(bind.length.pointee), encoding: String.Encoding.utf8, freeWhenDone: false))")
                //print("dealloc \(bind.buffer.load(as: CInt.self))")
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
                bind.error.deallocate(capacity: 1)
            }

            if let bindPtr = bindPtr {
                bindPtr.deallocate(capacity: binds.count)
            }
        }

        if let parametersPtr = parametersPtr {
            for i in 0 ..< parametersPtr.pointee.count {
                var parameter = parametersPtr.pointee[i]
                binds.append(MySQLConnection.getBind(parameter: parameter, ptr: &parameter))
            }
            bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
            for i in 0 ..< binds.count {
                bindPtr![i] = binds[i]
                //print("alloc \(binds[i].buffer.load(as: CInt.self)) \(binds[i].buffer_length) \(binds[i].length.pointee)")
                print("alloc \(String(bytesNoCopy: binds[i].buffer, length: Int(binds[i].length.pointee), encoding: String.Encoding.utf8, freeWhenDone: false))")
                //let x = binds[i].buffer.assumingMemoryBound(to: CChar.self)
                //print("alloc \(String(cString: x))")
            }

            guard mysql_stmt_bind_param(statement, bindPtr) == 0 else {
                handleError(statement, onCompletion: onCompletion)
                return
            }
        }

        if let namedParametersPtr = namedParametersPtr {
        }

        do {
            if let resultFetcher = try MySQLResultFetcher(statement: statement, copyBlobData: copyBlobData) {
                onCompletion(.resultSet(ResultSet(resultFetcher)))
            } else {
                onCompletion(.successNoData)
            }
        } catch {
            onCompletion(.error(error))
        }
    }

    static func getBind(field: MYSQL_FIELD) -> MYSQL_BIND {
        let size = getSize(field: field)

        var bind = MYSQL_BIND()
        bind.buffer_type = field.type
        bind.buffer_length = UInt(size)
        bind.is_unsigned = 0

        bind.buffer = UnsafeMutableRawPointer.allocate(bytes: size, alignedTo: 1)
        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.error = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)

        return bind
    }

    static func getBind<T>(parameter: T?, ptr: UnsafeMutablePointer<T>) -> MYSQL_BIND {
        let (type, size) = getTypeAndSize(parameter: parameter)

        var bind = MYSQL_BIND()
        bind.buffer_type = type
        bind.buffer_length = UInt(size)
        bind.is_unsigned = 0

        if parameter != nil {
            bind.buffer = UnsafeMutableRawPointer(ptr)
        } else {
            bind.buffer = nil
        }

        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.length.initialize(to: UInt(size))

        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.is_null.initialize(to: (parameter == nil ? 1 : 0))

        bind.error = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)

        return bind
    }

    static func getSize(field: MYSQL_FIELD) -> Int {
        switch field.type {
        case MYSQL_TYPE_TINY:
            return MemoryLayout<CChar>.size
        case MYSQL_TYPE_SHORT:
            return MemoryLayout<CShort>.size
        case MYSQL_TYPE_INT24,
             MYSQL_TYPE_LONG:
            return MemoryLayout<CInt>.size
        case MYSQL_TYPE_LONGLONG:
            return MemoryLayout<CLongLong>.size
        case MYSQL_TYPE_FLOAT:
            return MemoryLayout<CFloat>.size
        case MYSQL_TYPE_DOUBLE:
            return MemoryLayout<CDouble>.size
        case MYSQL_TYPE_TIME,
             MYSQL_TYPE_DATE,
             MYSQL_TYPE_DATETIME,
             MYSQL_TYPE_TIMESTAMP:
            return MemoryLayout<MYSQL_TIME>.size
        default:
            return Int(field.length)
        }
    }

    static func getTypeAndSize<T>(parameter: T?) -> (enum_field_types, Int) {
        guard let parameter = parameter else {
            return (MYSQL_TYPE_NULL, 1)
        }

        switch parameter {
        case is CChar:
            return (MYSQL_TYPE_TINY, MemoryLayout<CChar>.size)
        case is CShort:
            return (MYSQL_TYPE_SHORT, MemoryLayout<CShort>.size)
        case is CInt,
             is Int:
            return (MYSQL_TYPE_LONG, MemoryLayout<CInt>.size)
        case is CLongLong:
            return (MYSQL_TYPE_LONGLONG, MemoryLayout<CLongLong>.size)
        case is CFloat:
            return (MYSQL_TYPE_FLOAT, MemoryLayout<CFloat>.size)
        case is CDouble:
            return (MYSQL_TYPE_DOUBLE, MemoryLayout<CDouble>.size)
        case is MYSQL_TIME:
            return (MYSQL_TYPE_DATETIME, MemoryLayout<MYSQL_TIME>.size)
        case is String:
            let size = (parameter as! String).characters.count
            return (MYSQL_TYPE_STRING, size)
        case is Data:
            let size = (parameter as! Data).count
            return (MYSQL_TYPE_BLOB, size)
        default:
            return (MYSQL_TYPE_NULL, 1)
        }
    }
}
