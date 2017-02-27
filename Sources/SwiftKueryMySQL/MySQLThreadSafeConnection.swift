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

    override func executeQuery(query: String, parameters: [Any]? = nil, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.executeQuery(query: query, parameters: parameters, onCompletion: onCompletion)
        }
    }

    override func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {
        threadSafe {
            super.executeTransaction(command: command, inTransaction: inTransaction, changeTransactionState: changeTransactionState, errorMessage: errorMessage, onCompletion: onCompletion)
        }
    }
}
