/**
 Copyright IBM Corporation 2017, 2018
 
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
import Dispatch

import CMySQL

/// An implementation of `SwiftKuery.Connection` protocol for MySQL.
/// Instances of MySQLConnection are NOT thread-safe and should not be shared between threads.
/// Use `MySQLThreadSafeConnection` to share connection instances between multiple threads.
public class MySQLConnection: Connection {

    private static let initOnce: () = {
        mysql_server_init(0, nil, nil) // this call is not thread-safe
    }()

    static let dateFormatter = getDateFormatter("yyyy-MM-dd")
    static let timeFormatter = getDateFormatter("HH:mm:ss")
    static let dateTimeFormatter = getDateFormatter("yyyy-MM-dd HH:mm:ss")
    static let queryBuilderDateFormatter = getDateFormatter("''yyyy-MM-dd HH:mm:ss''")

    private let host: String
    private let user: String
    private let password: String
    private let database: String
    private let port: UInt32
    private let unixSocket: String?
    private let clientFlag: UInt
    private let characterSet: String
    private let reconnect: Bool
    
    /// Connection timeout in milliseconds
    private var timeout: UInt = 0

    private var mysql: UnsafeMutablePointer<MYSQL>?
    private var inTransaction = false

    public var isConnected: Bool {
        return mysql != nil && mysql_ping(mysql) == 0
    }

    /// The `QueryBuilder` with MySQL specific substitutions.
    public let queryBuilder: QueryBuilder = {
        let queryBuilder = QueryBuilder(addNumbersToParameters: false,
                                        anyOnSubquerySupported: true, columnBuilder: MySQLColumnBuilder(),
                                        dropIndexRequiresOnTableName: true,
                                        dateFormatter: MySQLConnection.queryBuilderDateFormatter)

        queryBuilder.updateSubstitutions([
            QueryBuilder.QuerySubstitutionNames.len: "LENGTH",
            QueryBuilder.QuerySubstitutionNames.identifierQuoteCharacter: "`",
            QueryBuilder.QuerySubstitutionNames.namedParameter: "",
            QueryBuilder.QuerySubstitutionNames.float: "float"
            ])

        return queryBuilder
    }()

    private static func getDateFormatter(_ dateFormat: String) -> DateFormatter {
        let dateFormatter = DateFormatter()
        dateFormatter.dateFormat = dateFormat
        return dateFormatter
    }

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
    public required init(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil, reconnect: Bool = true) {

        MySQLConnection.initOnce

        self.host = host ?? ""
        self.user = user ?? ""
        self.password = password ?? ""
        self.database = database ?? ""
        self.port = UInt32(port ?? 0)
        self.unixSocket = unixSocket
        self.clientFlag = clientFlag
        self.characterSet = characterSet ?? "utf8"
        self.reconnect = reconnect
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
    /// - Parameter reconnect: Enable or disable automatic reconnection to the server if the connection is found to have been lost
    /// - Parameter poolOptions: A set of `ConnectionOptions` to pass to the MySQL server.
    /// - Returns: `ConnectionPool` of `MySQLConnection`.
    public static func createPool(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil, reconnect: Bool = true, poolOptions: ConnectionPoolOptions) -> ConnectionPool {

        let connectionGenerator: () -> Connection? = {
            let connection = self.init(host: host, user: user, password: password, database: database, port: port, unixSocket: unixSocket, clientFlag: clientFlag, characterSet: characterSet, reconnect: reconnect)
            connection.setTimeout(to: UInt(poolOptions.timeout))
            let error = connection.connectSync()
            return error != nil ? nil : connection
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
    public func connect(onCompletion: @escaping (QueryError?) -> ()) {
        DispatchQueue.global().async {
            let mysql: UnsafeMutablePointer<MYSQL> = self.mysql ?? mysql_init(nil)

            var reconnect: Int8 = self.reconnect ? 1 : 0
            withUnsafePointer(to: &reconnect) { ptr in
                if mysql_options(mysql, MYSQL_OPT_RECONNECT, ptr) != 0 {
                    print("WARNING: Error setting MYSQL_OPT_RECONNECT")
                }
            }

            var timeoutSec = self.timeout / 1000 //Convert to seconds used in MySQL
            withUnsafePointer(to: &timeoutSec) { ptr in
                if mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, ptr) != 0 {
                    print("WARNING: Error setting MYSQL_OPT_CONNECT_TIMEOUT")
                }
            }

            if mysql_real_connect(mysql, self.host, self.user, self.password, self.database, self.port, self.unixSocket, self.clientFlag) != nil
                || mysql_errno(mysql) == UInt32(CR_ALREADY_CONNECTED) {

                if mysql_set_character_set(mysql, self.characterSet) != 0 {
                    let defaultCharSet = String(cString: mysql_character_set_name(mysql))
                    print("WARNING: Invalid characterSet: \(self.characterSet), using: \(defaultCharSet)")
                }

                self.mysql = mysql
                onCompletion(nil) // success
            } else {
                self.mysql = nil
                onCompletion(QueryError.connection(self.getError(mysql)))
                mysql_thread_end() // should be called for each mysql_init() call
            }
        }
    }
    
    /// Establish a connection with the database.
    ///
    public func connectSync() -> QueryError? {
        var error: QueryError?
        let semaphore = DispatchSemaphore(value: 0)
        connect { err in
            error = err
            semaphore.signal()
        }
        semaphore.wait()
        guard let errorUnwrapped = error else {
            // Everything worked
            return nil
        }
        return errorUnwrapped
    }

    /// Set connection timeout
    ///
    /// - Parameter to: Timeout value in milliseconds
    public func setTimeout(to timeout: UInt) {
        self.timeout = timeout
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
        DispatchQueue.global().async {
            mysql_thread_init()
            self.prepareStatement(query) { stmt, error in
                guard let statement = stmt else {
                    if let error = error {
                        self.runCompletionHandler(.error(QueryError.databaseError(error.localizedDescription)), onCompletion: onCompletion)
                        mysql_thread_end()
                        return
                    }
                    self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
                    mysql_thread_end()
                    return
                }
                self.execute(preparedStatement: statement) { result in
                    if result.asResultSet == nil {
                        self.release(preparedStatement: statement) { _ in
                            self.runCompletionHandler(result, onCompletion: onCompletion)
                            return
                        }
                        return
                    }
                    self.runCompletionHandler(result, onCompletion: onCompletion)
                    return
                }
            }
            mysql_thread_end()
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()
            self.prepareStatement(query) { stmt, error in
                guard let statement = stmt else {
                    if let error = error {
                        self.runCompletionHandler(.error(QueryError.databaseError(error.localizedDescription)), onCompletion: onCompletion)
                        mysql_thread_end()
                        return
                    }
                    self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
                    mysql_thread_end()
                    return
                }
                self.execute(preparedStatement: statement, parameters: parameters) { result in
                    if result.asResultSet == nil {
                        self.release(preparedStatement: statement) { _ in
                            self.runCompletionHandler(result, onCompletion: onCompletion)
                            return
                        }
                        return
                    }
                    self.runCompletionHandler(result, onCompletion: onCompletion)
                    return
                }
            }
            mysql_thread_end()
        }
    }

    /// Execute a raw query.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()
            self.prepareStatement(raw) { stmt, error in
                guard let statement = stmt else {
                    if let error = error {
                        self.runCompletionHandler(.error(QueryError.databaseError(error.localizedDescription)), onCompletion: onCompletion)
                        mysql_thread_end()
                        return
                    }
                    self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
                    mysql_thread_end()
                    return
                }
                self.execute(preparedStatement: statement) { result in
                    if result.asResultSet == nil {
                        self.release(preparedStatement: statement) { _ in
                            self.runCompletionHandler(result, onCompletion: onCompletion)
                            return
                        }
                        return
                    }
                    self.runCompletionHandler(result, onCompletion: onCompletion)
                    return
                }
            }
            mysql_thread_end()
        }
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()
            self.prepareStatement(raw) { stmt, error in
                guard let statement = stmt else {
                    if let error = error {
                        self.runCompletionHandler(.error(QueryError.databaseError(error.localizedDescription)), onCompletion: onCompletion)
                        mysql_thread_end()
                        return
                    }
                    self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
                    mysql_thread_end()
                    return
                }
                self.execute(preparedStatement: statement, parameters: parameters) { result in
                    if result.asResultSet == nil {
                        self.release(preparedStatement: statement) { _ in
                            self.runCompletionHandler(result, onCompletion: onCompletion)
                            return
                        }
                        return
                    }
                    self.runCompletionHandler(result, onCompletion: onCompletion)
                    return
                }
            }
            mysql_thread_end()
        }
    }

    /// NOT supported in MySQL
    /// Execute a raw query with named parameters.
    ///
    /// - Parameter raw: A String with the raw query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            onCompletion(.error(QueryError.unsupported("Named parameters with raw queries are not supported in MySQL")))
        }
    }

    /// Prepare statement.
    ///
    /// - Parameter query: The query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    public func prepareStatement(_ query: Query, onCompletion: @escaping ((PreparedStatement?, QueryError?) -> ())) {
        var mySQLQuery: String
        do {
            mySQLQuery = try query.build(queryBuilder: queryBuilder)
        } catch let error {
            runCompletionHandler(nil, QueryError.syntaxError("Unable to prepare statement: \(error.localizedDescription)"), onCompletion: onCompletion)
            return
        }
        prepareStatement(mySQLQuery, query: query, onCompletion: onCompletion)
    }

    public func prepareStatement(_ raw: String, onCompletion: @escaping ((PreparedStatement?, QueryError?) -> ())) {
        prepareStatement(raw, query: nil, onCompletion: onCompletion)
    }

    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    private func prepareStatement(_ raw: String, query: Query? = nil, onCompletion: @escaping ((PreparedStatement?, QueryError?) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()

            guard let mysql = self.mysql else {
                self.runCompletionHandler(nil, QueryError.connection("Connection not connected"), onCompletion: onCompletion)
                return
            }

            guard let statement = mysql_stmt_init(mysql) else {
                let error = self.getError(mysql)
                self.runCompletionHandler(nil, QueryError.databaseError(error), onCompletion: onCompletion)
                mysql_thread_end()
                return
            }

            guard mysql_stmt_prepare(statement, raw, UInt(raw.utf8.count)) == 0 else {
                let error = "ERROR \(mysql_stmt_errno(statement)): " + String(cString: mysql_stmt_error(statement))
                mysql_stmt_close(statement)
                self.runCompletionHandler(nil, QueryError.databaseError(error), onCompletion: onCompletion)
                mysql_thread_end()
                return
            }

            let stmt = MySQLPreparedStatement(query: query, mysql: mysql, statement: statement)
            self.runCompletionHandler(stmt, nil, onCompletion: onCompletion)
            mysql_thread_end()
            return
        }
    }

    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()
            let mysqlStmt = preparedStatement as! MySQLPreparedStatement
            mysqlStmt.release { result in
                self.runCompletionHandler(result, onCompletion: onCompletion)
                return
            }
            mysql_thread_end()
        }
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ()))  {
        DispatchQueue.global().async {
            mysql_thread_init()
            let mysqlStmt = preparedStatement as! MySQLPreparedStatement
            self.executePreparedStatement(statement: mysqlStmt, parameters: nil, onCompletion: onCompletion)
            mysql_thread_end()
        }
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            mysql_thread_init()
            let mysqlStmt = preparedStatement as! MySQLPreparedStatement
            self.executePreparedStatement(statement: mysqlStmt, parameters: parameters, onCompletion: onCompletion)
            mysql_thread_end()
        }
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            onCompletion(.error(QueryError.unsupported("Named parameters in prepared statemennts are not supported in MySQL")))
        }
    }

    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    public func startTransaction(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "START TRANSACTION", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to start the transaction", onCompletion: onCompletion)
        }
    }

    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "COMMIT", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to commit the transaction", onCompletion: onCompletion)
        }
    }

    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
        }
    }

    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to create the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "ROLLBACK TO \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to rollback to the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "RELEASE SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to release the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

/*    func prepareStatement(_ query: Query, onCompletion: @escaping ((QueryResult) -> ())) -> MySQLPreparedStatement? {
        do {
            return try prepareStatement(query) as? MySQLPreparedStatement
        } catch {
            onCompletion(.error(error))
            return nil
        }
    }

    func prepareStatement(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) -> MySQLPreparedStatement? {
        do {
            return try prepareStatement(raw) as? MySQLPreparedStatement
        } catch {
            onCompletion(.error(error))
            return nil
        }
    }*/

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
            onCompletion(.error(QueryError.databaseError("\(errorMessage): \(getError(mysql))")))
        }
    }

    func getError(_ connection: UnsafeMutablePointer<MYSQL>) -> String {
        return "ERROR \(mysql_errno(connection)): " + String(cString: mysql_error(connection))
    }

    func executePreparedStatement(statement: MySQLPreparedStatement, parameters: [Any?]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let statementPtr = statement.statement else {
            onCompletion(.error(QueryError.connection("PreparedStatement release() has already been called.")))
            return
        }

        if let parameters = parameters {
            if let _ = statement.bindPtr {
                if statement.bindsCapacity != parameters.count {
                    runCompletionHandler(.error(QueryError.databaseError("Each of multiple execute() calls must pass the same number of parameters.")), onCompletion: onCompletion)
                    return
                }
            } else { // true only for the first time execute() is called for this PreparedStatement
                statement.bindsCapacity = parameters.count
                statement.bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: statement.bindsCapacity)
            }

            guard statement.allocateBinds(parameters: parameters) == true else {
                let errorResult = QueryResult.error(QueryError.databaseError(statement.getError(statementPtr)))
                statement.statement = nil
                mysql_stmt_close(statementPtr)
                runCompletionHandler((errorResult),onCompletion: onCompletion)
                return
            }
        }

        guard let resultMetadata = mysql_stmt_result_metadata(statementPtr) else {
            // non-query statement (insert, update, delete)

            guard mysql_stmt_execute(statementPtr) == 0 else {
                statement.statement = nil
                let error = statement.getError(statementPtr)
                mysql_stmt_close(statementPtr)
                runCompletionHandler(.error(QueryError.databaseError(error)), onCompletion: onCompletion)
                return
            }

            if statement.query != nil, let insertQuery = statement.query as? Insert, insertQuery.returnID {

                guard let idColumn = insertQuery.table.columns.first(where: {$0.isPrimaryKey && $0.autoIncrement}) else {
                    runCompletionHandler(.error(QueryError.syntaxError("Could not retrieve ID Column in order to return the ID value")), onCompletion: onCompletion)
                    return
                }

                // Close current statement before executing another.
                statement.statement = nil
                mysql_stmt_close(statementPtr)
                prepareStatement("SELECT LAST_INSERT_ID() AS \(idColumn.name)") { stmt, error in
                    guard let statement = stmt else {
                        if let error = error {
                            self.runCompletionHandler(.error(QueryError.databaseError(error.localizedDescription)), onCompletion: onCompletion)
                            return
                        }
                        self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
                        return
                    }
                    self.execute(preparedStatement: statement) { result in
                        if result.asResultSet == nil {
                            self.release(preparedStatement: statement) { _ in
                                self.runCompletionHandler(result, onCompletion: onCompletion)
                                return
                            }
                        }
                        self.runCompletionHandler(result, onCompletion: onCompletion)
                        return
                    }
                }
                return
            }

            let affectedRows = mysql_stmt_affected_rows(statementPtr)
            runCompletionHandler(.success("\(affectedRows) rows affected"), onCompletion: onCompletion)
            return
        }

        let resultFetcher = MySQLResultFetcher(preparedStatement: statement, resultMetadata: resultMetadata)
        guard resultFetcher.initialize() else {
            let error = QueryError.databaseError(statement.getError(statementPtr))
            statement.release { _ in
                self.runCompletionHandler(.error(error), onCompletion: onCompletion)
            }
            return
        }
        runCompletionHandler(.resultSet(ResultSet(resultFetcher)), onCompletion: onCompletion)
        return
    }
}

class MySQLColumnBuilder: ColumnCreator {
    func buildColumn(for column: Column, using queryBuilder: QueryBuilder) -> String? {
        guard let type = column.type else {
            return nil
        }

        var result = column.name
        let identifierQuoteCharacter = queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.identifierQuoteCharacter.rawValue]
        if !result.hasPrefix(identifierQuoteCharacter) {
            result = identifierQuoteCharacter + result + identifierQuoteCharacter + " "
        }

        var typeString = type.create(queryBuilder: queryBuilder)
        if let length = column.length {
            typeString += "(\(length))"
        }
        if column.autoIncrement && typeCanBeAutoIncrement(typeString) {
            result += typeString + " AUTO_INCREMENT"
        } else {
            result += typeString
        }

        if column.isPrimaryKey {
            result += " PRIMARY KEY"
        }
        if column.isNotNullable {
            result += " NOT NULL"
        }
        if column.isUnique {
            result += " UNIQUE"
        }
        if let defaultValue = column.defaultValue {
            var packedType: String
            do {
                packedType = try packType(defaultValue, queryBuilder: queryBuilder)
            } catch {
                return nil
            }
            result += " DEFAULT " + packedType
        }
        if let checkExpression = column.checkExpression {
            result += checkExpression.contains(column.name) ? " CHECK (" + checkExpression.replacingOccurrences(of: column.name, with: "\"\(column.name)\"") + ")" : " CHECK (" + checkExpression + ")"
        }
        if let collate = column.collate {
            result += " COLLATE \"" + collate + "\""
        }
        return result
    }

    func packType(_ item: Any, queryBuilder: QueryBuilder) throws -> String {
        switch item {
        case let val as String:
            return "'\(val)'"
        case let val as Bool:
            return val ? queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.booleanTrue.rawValue]
                : queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.booleanFalse.rawValue]
        case let val as Parameter:
            return try val.build(queryBuilder: queryBuilder)
        case let value as Date:
            if let dateFormatter = queryBuilder.dateFormatter {
                return dateFormatter.string(from: value)
            }
            return "'\(String(describing: value))'"
        default:
            return String(describing: item)
        }
    }

    func typeCanBeAutoIncrement(_ type: String) -> Bool {
        switch type {
        case  "integer", "smallint", "tinyint", "mediumint", "bigint":
            return true
        case "float", "double":
            return true
        default:
            return false
        }
    }
}

