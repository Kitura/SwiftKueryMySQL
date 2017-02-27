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
    private let copyBlobData: Bool

    private var connection: UnsafeMutablePointer<MYSQL>?
    private var inTransaction = false

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
        mysql_thread_end()
    }

    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: (QueryError?) -> ()) {
        if connection == nil {
            connection = mysql_init(nil)
        }

        if mysql_real_connect(connection, host, user, password, database, port, unixSocket, clientFlag) != nil {
            if mysql_set_character_set(connection, characterSet) != 0 {
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

    /// NOT supported in MySQL
    /// Execute a query with named parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in MySQL")))
    }

    /// NOT supported in MySQL
    /// Execute a raw query with named parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
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

        guard let connection = connection else {
            onCompletion(.error(QueryError.connection("Not connected, call connect() first")))
            return
        }

        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists" : "No transaction exists"
            onCompletion(.error(QueryError.transactionError(error)))
            return
        }

        if mysql_query(connection, command) == 0 {
            if changeTransactionState {
                self.inTransaction = !self.inTransaction
            }
            onCompletion(.successNoData)
        } else {
            onCompletion(.error(QueryError.databaseError("\(errorMessage): \(getError())")))
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

    func executeQuery(query: String, parameters: [Any]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let connection = connection else {
            onCompletion(.error(QueryError.connection("Not connected, call connect() before execute()")))
            return
        }

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
                bind.buffer.deallocate(bytes: Int(bind.buffer_length), alignedTo: 1)
                bind.length.deallocate(capacity: 1)
                bind.is_null.deallocate(capacity: 1)
            }

            if let bindPtr = bindPtr {
                bindPtr.deallocate(capacity: binds.count)
            }
        }

        if let parameters = parameters {
            for parameter in parameters {
                binds.append(getInputBind(parameter))
            }
            bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
            for i in 0 ..< binds.count {
                bindPtr![i] = binds[i]
            }

            guard mysql_stmt_bind_param(statement, bindPtr) == 0 else {
                handleError(statement, onCompletion: onCompletion)
                return
            }
        }

        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            // non-query statement (insert, update, delete)
            guard mysql_stmt_execute(statement) == 0 else {
                handleError(statement, onCompletion: onCompletion)
                return
            }
            let affectedRows = mysql_stmt_affected_rows(statement)
            mysql_stmt_close(statement)
            onCompletion(.success("\(affectedRows) rows affected"))
            return
        }

        defer {
            mysql_free_result(resultMetadata)
        }

        do {
            if let resultFetcher = try MySQLResultFetcher(statement: statement, resultMetadata: resultMetadata, copyBlobData: copyBlobData) {
                onCompletion(.resultSet(ResultSet(resultFetcher)))
            } else {
                onCompletion(.successNoData)
            }
        } catch {
            onCompletion(.error(error))
        }
    }

    private func getInputBind<T>(_ parameter: T?) -> MYSQL_BIND {
        let size: Int
        let buffer: UnsafeMutableRawPointer?

        if let parameter = parameter {
            if parameter is String {
                let collection = (parameter as! String).utf8
                size = collection.count
                let typedBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: size)
                typedBuffer.initialize(from: collection)
                buffer = UnsafeMutableRawPointer(typedBuffer)
            } else if parameter is [UInt8] {
                let collection = parameter as! [UInt8]
                size = collection.count
                let typedBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: size)
                typedBuffer.initialize(from: collection)
                buffer = UnsafeMutableRawPointer(typedBuffer)
            } else if parameter is Data {
                let data = parameter as! Data
                size = data.count
                let typedBuffer = UnsafeMutablePointer<UInt8>.allocate(capacity: size)
                data.copyBytes(to: typedBuffer, count: size)
                buffer = UnsafeMutableRawPointer(typedBuffer)
            } else {
                size = MemoryLayout<T>.size
                let typedBuffer = UnsafeMutablePointer<T>.allocate(capacity: 1)
                typedBuffer.initialize(to: parameter)
                buffer = UnsafeMutableRawPointer(typedBuffer)
            }
        } else {
            size = 0
            buffer = nil
        }

        var bind = MYSQL_BIND()
        bind.buffer_type = getType(parameter: parameter)
        bind.buffer_length = UInt(size)
        bind.is_unsigned = (parameter is UnsignedInteger || parameter is UnicodeScalar ? 1 : 0)

        bind.buffer = buffer
        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.length.initialize(to: UInt(size))

        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.is_null.initialize(to: (parameter == nil ? 1 : 0))

        return bind
    }

    private func getType(parameter: Any?) -> enum_field_types {
        guard let parameter = parameter else {
            return MYSQL_TYPE_NULL
        }

        switch parameter {
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
        case is Integer:       // any other integer types
            return MYSQL_TYPE_LONGLONG
        case is Float:
            return MYSQL_TYPE_FLOAT
        case is Double:
            return MYSQL_TYPE_DOUBLE
        case is MYSQL_TIME:
            return MYSQL_TYPE_DATETIME
        case is String:
            return MYSQL_TYPE_STRING
        case is Data,
             is [UInt8]:
            return MYSQL_TYPE_BLOB
        default:
            print("Unhandled input parameter type: \(type(of: parameter))")
            return MYSQL_TYPE_NULL
        }
    }
}
