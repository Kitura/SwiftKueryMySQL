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

    private var connection: UnsafeMutablePointer<MYSQL>?

    /// The `QueryBuilder` with MySQL specific substitutions.
    public let queryBuilder = MySQLConnection.createQuryBuilder()

    /// Initialize an instance of MySQLConnection.
    ///
    /// - Parameter host: host name or IP address of server to connect to, defaults to localhost
    /// - Parameter user: MySQL login ID, defaults to current user
    /// - Parameter password: password for `user`, defaults to no password
    /// - Parameter database: default database to use if specified
    /// - Parameter port: port number for the TCP/IP connection if using a non-standard port
    /// - Parameter unixSocket: unix domain socket or named pipe to use for connecting to server instead of TCP/IP
    /// - Parameter clientFlag: MySQL client options
    public init(host: String? = nil, user: String? = nil, password: String? = nil, database: String? = nil, port: Int? = nil, unixSocket: String? = nil, clientFlag: UInt = 0, characterSet: String? = nil) {

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

    private static func createQuryBuilder() -> QueryBuilder {
        let queryBuilder = QueryBuilder(anyOnSubquerySupported: true)
        queryBuilder.updateSubstitutions([QueryBuilder.QuerySubstitutionNames.len : "LENGTH"])
        return queryBuilder
    }

    /// Return a String representation of the query.
    ///
    /// - Parameter query: The query.
    /// - Returns: A String representation of the query.
    /// - Throws: QueryError.syntaxError if query build fails.
    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let mysqlQuery = try query.build(queryBuilder: queryBuilder)
            executeQueryWithParameters(query: mysqlQuery, parameters: parameters, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }
    }
    
    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        do {
            let mysqlQuery = try query.build(queryBuilder: queryBuilder)
            executeQuery(query: mysqlQuery, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
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
        executeQueryWithParameters(query: raw, parameters: parameters, onCompletion: onCompletion)
    }

    private func getError() -> String {
        return String(cString: mysql_error(connection))
    }

    private func executeQuery(query: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let statement = mysql_stmt_init(connection) else {
            onCompletion(.error(QueryError.databaseError(getError())))
            return
        }

        guard mysql_stmt_prepare(statement, query, UInt(query.utf8.count)) == 0 else {
            onCompletion(.error(QueryError.syntaxError(getError())))
            mysql_stmt_close(statement)
            return
        }

        guard let resultMetadata = mysql_stmt_result_metadata(statement) else {
            onCompletion(.error(QueryError.databaseError(getError())))
            mysql_stmt_close(statement)
            return
        }

        guard let mySqlFields = mysql_fetch_fields(resultMetadata) else {
            onCompletion(.error(QueryError.databaseError(getError())))
            mysql_free_result(resultMetadata)
            mysql_stmt_close(statement)
            return
        }

        let numFields = Int(mysql_num_fields(resultMetadata))
        var binds = [MYSQL_BIND]()
        var fieldNames = [String]()

        for i in 0 ..< numFields {
            let field = mySqlFields[i]
            binds.append(MySQLConnection.getBind(field: field))
            fieldNames.append(String(cString: field.name))
        }

        mysql_free_result(resultMetadata)

        let bindPtr = UnsafeMutablePointer<MYSQL_BIND>.allocate(capacity: binds.count)
        for i in 0 ..< binds.count {
            bindPtr[i] = binds[i]
        }

        guard mysql_stmt_bind_result(statement, bindPtr) == 0 else {
            onCompletion(.error(QueryError.databaseError(getError())))
            mysql_stmt_close(statement)
            return
        }

        guard mysql_stmt_execute(statement) == 0 else {
            onCompletion(.error(QueryError.databaseError(getError())))
            mysql_stmt_close(statement)
            return
        }

        guard let resultFetcher = MySQLResultFetcher(statement: statement, bindPtr: bindPtr, binds: binds, fieldNames: fieldNames) else {
            onCompletion(.successNoData)
            mysql_stmt_close(statement)
            return
        }

        onCompletion(.resultSet(ResultSet(resultFetcher)))
    }

    private static func getBind(field: MYSQL_FIELD) -> MYSQL_BIND {
        var bind = MYSQL_BIND()
        bind.buffer_type = field.type
        bind.buffer_length = field.length
        bind.is_unsigned = 0

        bind.buffer = UnsafeMutableRawPointer.allocate(bytes: Int(field.length), alignedTo: MemoryLayout<Int8>.alignment)
        bind.length = UnsafeMutablePointer<UInt>.allocate(capacity: 1)
        bind.is_null = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)
        bind.error = UnsafeMutablePointer<my_bool>.allocate(capacity: 1)

        return bind
    }

    private func executeQueryWithParameters(query: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
/*
        var parameterData = [UnsafePointer<Int8>?]()
        // At the moment we only create string parameters. Binary parameters should be added.
        for parameter in parameters {
            let value = AnyCollection("\(parameter)".utf8CString)
            let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(value.count))
            for (index, byte) in value.enumerated() {
                pointer[index] = byte
            }
            parameterData.append(pointer)
        }
        _ = parameterData.withUnsafeBufferPointer { buffer in
            PQsendQueryParams(connection, query, Int32(parameters.count), nil, buffer.isEmpty ? nil : buffer.baseAddress, nil, nil, 0)
        }
        PQsetSingleRowMode(connection)
        processQueryResult(query: query, onCompletion: onCompletion)
*/
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in MySQL")))
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        onCompletion(.error(QueryError.unsupported("Named parameters are not supported in MySQL")))
    }
}
