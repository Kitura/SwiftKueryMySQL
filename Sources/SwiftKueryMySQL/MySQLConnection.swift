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
/// Instances of MySQLConnection are NOT thread-safe and should not be shared between threads.
/// Use `MySQLThreadSafeConnection` to share connection instances between multiple threads.
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

    private var mysql: UnsafeMutablePointer<MYSQL>?
    private var inTransaction = false

    public var isConnected: Bool {
        return self.mysql != nil
    }

    /// The `QueryBuilder` with MySQL specific substitutions.
    public let queryBuilder: QueryBuilder = {
        let queryBuilder = QueryBuilder(addNumbersToParameters: false, anyOnSubquerySupported: true)
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.len : "LENGTH"])
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.identifierQuoteCharacter : "`"])
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
    /// - Parameter characterSet: MySQL character set to use for the connection
    public required init(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil) {

        MySQLConnection.initOnce

        self.host = host ?? ""
        self.user = user ?? ""
        self.password = password ?? ""
        self.database = database ?? ""
        self.port = UInt32(port ?? 0)
        self.unixSocket = unixSocket
        self.clientFlag = clientFlag
        self.characterSet = characterSet ?? "utf8"
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

    /// Create a MySQL connection pool.
    ///
    /// - Parameter host: host name or IP address of server to connect to, defaults to localhost
    /// - Parameter user: MySQL login ID, defaults to current user
    /// - Parameter password: password for `user`, defaults to no password
    /// - Parameter database: default database to use if specified
    /// - Parameter port: port number for the TCP/IP connection if using a non-standard port
    /// - Parameter unixSocket: unix domain socket or named pipe to use for connecting to server instead of TCP/IP
    /// - Parameter clientFlag: MySQL client options
    /// - Parameter characterSet: MySQL character set to use for the connection
    /// - Parameter poolOptions: A set of `ConnectionOptions` to pass to the MySQL server.
    /// - Returns: `ConnectionPool` of `MySQLConnection`.
    public static func createPool(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil, poolOptions: ConnectionPoolOptions) -> ConnectionPool {

        let connectionGenerator: () -> Connection? = {
            let connection = self.init(host: host, user: user, password: password, database: database, port: port, unixSocket: unixSocket, clientFlag: clientFlag, characterSet: characterSet)
            connection.connect { _ in }
            return connection.mysql != nil ? connection : nil
        }

        let connectionReleaser: (_ connection: Connection) -> () = { connection in
            connection.closeConnection()
        }

        return ConnectionPool(options: poolOptions, connectionGenerator: connectionGenerator, connectionReleaser: connectionReleaser)
    }

    /// Create a MySQL connection pool.
    ///
    /// - Parameter url: A URL with the connection information. For example, mysql://user:password@host:port/database
    /// - Parameter poolOptions: A set of `ConnectionOptions` to pass to the MySQL server.
    /// - Returns: `ConnectionPool` of `MySQLConnection`.
    public static func createPool(url: URL, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        return createPool(host: url.host, user: url.user, password: url.password, database: url.lastPathComponent, port: url.port, poolOptions: poolOptions)
    }

    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: (QueryError?) -> ()) {
        let mysql: UnsafeMutablePointer<MYSQL> = self.mysql ?? mysql_init(nil)

        if mysql_real_connect(mysql, host, user, password, database, port, unixSocket, clientFlag) != nil
            || mysql_errno(mysql) == UInt32(CR_ALREADY_CONNECTED) {

            if mysql_set_character_set(mysql, characterSet) != 0 {
                let defaultCharSet = String(cString: mysql_character_set_name(mysql))
                print("WARNING: Invalid characterSet: \(characterSet), using: \(defaultCharSet)")
            }

            self.mysql = mysql
            onCompletion(nil) // success
        } else {
            self.mysql = nil
            onCompletion(QueryError.connection(MySQLConnection.getError(mysql)))
            mysql_thread_end() // should be called for each mysql_init() call
        }
    }

    /// Close the connection to the database.
    public func closeConnection() {
        if let mysql = self.mysql {
            self.mysql = nil
            mysql_close(mysql)
            mysql_thread_end() // should be called for each mysql_init() call
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
    public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        if let query = build(query: query, onCompletion: onCompletion) {
            executeQuery(query: query, parametersArray: [parameters], onCompletion: onCompletion)
        }
    }

    /// Execute a query multiple times with multiple parameter sets.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: Multiple sets of parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parametersArray: [[Any?]], onCompletion: @escaping ((QueryResult) -> ())) {
        if let query = build(query: query, onCompletion: onCompletion) {
            executeQuery(query: query, parametersArray: parametersArray, onCompletion: onCompletion)
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
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, parametersArray: [parameters], onCompletion: onCompletion)
    }


    /// Execute a raw query multiple times with multiple parameter sets.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: Multiple sets of parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parametersArray: [[Any?]], onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, parametersArray: parametersArray, onCompletion: onCompletion)
    }

    /// NOT supported in MySQL
    /// Execute a query with named parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in MySQL")))
    }

    /// NOT supported in MySQL
    /// Execute a raw query with named parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in MySQL")))
    }

    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    public func startTransaction(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "START TRANSACTION", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to start the transaction", onCompletion: onCompletion)
    }

    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "COMMIT", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to commit the transaction", onCompletion: onCompletion)
    }

    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }

    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to create the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK TO \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to rollback to the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "RELEASE SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to release the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {

        guard let mysql = self.mysql else {
            onCompletion(.error(QueryError.connection("Not connected, call connect() first")))
            return
        }

        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists" : "No transaction exists"
            onCompletion(.error(QueryError.transactionError(error)))
            return
        }

        if mysql_query(mysql, command) == 0 {
            if changeTransactionState {
                self.inTransaction = !self.inTransaction
            }
            onCompletion(.successNoData)
        } else {
            onCompletion(.error(QueryError.databaseError("\(errorMessage): \(MySQLConnection.getError(mysql))")))
        }
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

    static func getError(_ statement: UnsafeMutablePointer<MYSQL_STMT>) -> String {
        return "ERROR \(mysql_stmt_errno(statement)): " + String(cString: mysql_stmt_error(statement))
    }

    static func getError(_ connection: UnsafeMutablePointer<MYSQL>) -> String {
        return "ERROR \(mysql_errno(connection)): " + String(cString: mysql_error(connection))
    }

    private func handleError(_ statement: UnsafeMutablePointer<MYSQL_STMT>, onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.databaseError(MySQLConnection.getError(statement))))
        mysql_stmt_close(statement)
    }

    func executeQuery(query: String, parametersArray: [[Any?]]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let mysql = self.mysql else {
            onCompletion(.error(QueryError.connection("Not connected, call connect() before execute()")))
            return
        }

        guard let statement = mysql_stmt_init(mysql) else {
            onCompletion(.error(QueryError.connection(MySQLConnection.getError(mysql))))
            return
        }

        guard mysql_stmt_prepare(statement, query, UInt(query.utf8.count)) == 0 else {
            onCompletion(.error(QueryError.syntaxError(MySQLConnection.getError(statement))))
            mysql_stmt_close(statement)
            return
        }

        var binds = [MYSQL_BIND]()
        var capacity = 0
        var bindPtr: UnsafeMutablePointer<MYSQL_BIND>? = nil

        defer {
            deallocateBinds(binds: &binds, bindPtr: bindPtr, capacity: capacity)
        }

        if let parametersArray = parametersArray, parametersArray.count > 0 {
            capacity = parametersArray[0].count
            bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: capacity)

            do {
                try allocateBinds(statement: statement, parameters: parametersArray[0], binds: &binds, bindPtr: &bindPtr!)
            } catch {
                mysql_stmt_close(statement)
                onCompletion(.error(error))
                return
            }
        }

        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            // non-query statement (insert, update, delete)
            guard mysql_stmt_execute(statement) == 0 else {
                handleError(statement, onCompletion: onCompletion)
                return
            }

            defer {
                mysql_stmt_close(statement)
            }

            var affectedRows = [UInt64]()
            affectedRows.append(mysql_stmt_affected_rows(statement))

            if let parametersArray = parametersArray, parametersArray.count > 1 {
                for i in 1 ..< parametersArray.count {
                    do {
                        try allocateBinds(statement: statement, parameters: parametersArray[i], binds: &binds, bindPtr: &bindPtr!)
                    } catch {
                        onCompletion(.error(error))
                        return
                    }

                    guard mysql_stmt_execute(statement) == 0 else {
                        handleError(statement, onCompletion: onCompletion)
                        return
                    }

                    affectedRows.append(mysql_stmt_affected_rows(statement))
                }
            }

            onCompletion(.success("\(affectedRows) rows affected"))
            return
        }

        defer {
            mysql_free_result(resultMetadata)
        }

        do {
            if let resultFetcher = try MySQLResultFetcher(statement: statement, resultMetadata: resultMetadata) {
                onCompletion(.resultSet(ResultSet(resultFetcher)))
            } else {
                onCompletion(.successNoData)
            }
        } catch {
            onCompletion(.error(error))
        }
    }

    private func allocateBinds(statement: UnsafeMutablePointer<MYSQL_STMT>, parameters: [Any?], binds: inout [MYSQL_BIND], bindPtr: inout UnsafeMutablePointer<MYSQL_BIND>) throws {

        if binds.isEmpty { // first parameter set, create new bind and bind it to the parameter
            for (index, parameter) in parameters.enumerated() {
                var bind = MYSQL_BIND()
                setBind(&bind, parameter)
                binds.append(bind)
                bindPtr[index] = bind
            }
        } else { // bind was previously created, re-initialize value
            for (index, parameter) in parameters.enumerated() {
                var bind = binds[index]
                setBind(&bind, parameter)
                binds[index] = bind
                bindPtr[index] = bind
            }
        }

        guard mysql_stmt_bind_param(statement, bindPtr) == 0 else {
            throw QueryError.databaseError(MySQLConnection.getError(statement))
        }
    }

    private func deallocateBinds(binds: inout [MYSQL_BIND], bindPtr: UnsafeMutablePointer<MYSQL_BIND>?, capacity: Int) {

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

        if let bindPtr = bindPtr {
            bindPtr.deallocate(capacity: capacity)
        }

        binds.removeAll()
    }

    private func setBind(_ bind: inout MYSQL_BIND, _ parameter: Any?) {
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
        case let byteArray as [UInt8]:
            let typedBuffer = allocate(type: UInt8.self, capacity: byteArray.count, bind: &bind)
            typedBuffer.initialize(from: byteArray)
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
        let utf8 = string.utf8
        let typedBuffer = allocate(type: UInt8.self, capacity: utf8.count, bind: &bind)
        typedBuffer.initialize(from: utf8)
    }

    private func getType(parameter: Any) -> enum_field_types {
        switch parameter {
        case is String:
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
