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

/// A thread-safe implementation of `SwiftKuery.Connection` protocol for MySQL.
/// Instances of MySQLThreadSafeConnection can be safely shared between multiple threads.
public class MySQLThreadSafeConnection: MySQLConnection {
    private let lock = NSRecursiveLock()
    // Need a reentrant mutex as locked functions can callback other locked functions.

    private func threadSafe<T>(execute work: () throws -> T) rethrows -> T {
        lock.lock()
        defer {
            lock.unlock()
        }
        return try work()
    }

    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public override func connect(onCompletion: (QueryError?) -> ()) {
        threadSafe {
            super.connect(onCompletion: onCompletion)
        }
    }

    /// Close the connection to the database.
    public override func closeConnection() {
        threadSafe {
            super.closeConnection()
        }
    }

    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    override public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.execute(query: query, onCompletion: onCompletion)
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    override public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.execute(query: query, parameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    override public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.execute(raw, onCompletion: onCompletion)
        }
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    override public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.execute(raw, parameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Prepare statement.
    ///
    /// - Parameter query: The query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    override public func prepareStatement(_ query: Query) throws -> PreparedStatement {
        return try threadSafe {
            try super.prepareStatement(query)
        }
    }

    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    override public func prepareStatement(_ raw: String) throws -> PreparedStatement  {
        return try threadSafe {
            try super.prepareStatement(raw)
        }
    }

    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    override public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.release(preparedStatement: preparedStatement, onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    override public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ()))  {
        threadSafe {
            super.execute(preparedStatement: preparedStatement, onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    override public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.execute(preparedStatement: preparedStatement, parameters: parameters, onCompletion: onCompletion)
        }
    }

    override func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.executeTransaction(command: command, inTransaction: inTransaction, changeTransactionState: changeTransactionState, errorMessage: errorMessage, onCompletion: onCompletion)
        }
    }
}
