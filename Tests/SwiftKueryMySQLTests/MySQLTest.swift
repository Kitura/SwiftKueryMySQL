/**
 * Copyright IBM Corporation 2017
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import XCTest
import Dispatch

import SwiftKuery
import SwiftKueryMySQL

class MySQLTest: XCTestCase {
    func performTest(line: Int = #line, asyncTasks: @escaping (Connection) -> Void...) {
        let connection = createConnection()
        defer {
            connection.closeConnection()
        }

        var connectError: QueryError? = nil
        connection.connect() { error in
            if let error = error {
                connectError = error
                return
            }

            // use a concurrent queue so we can test connection is thread-safe
            let queue = DispatchQueue(label: "Test tasks queue", attributes: .concurrent)
            queue.suspend() // don't start executing tasks when queued

            for (index, asyncTask) in asyncTasks.enumerated() {
                let exp = expectation(description: "\(type(of: self)):\(line)[\(index)]")
                queue.async() {
                    asyncTask(connection)
                    exp.fulfill()
                }
            }

            queue.resume() // all tasks are queued, execute them
        }

        if let error = connectError {
            XCTFail(error.description)
        } else {
            // wait for all async tasks to finish
            waitForExpectations(timeout: 10) { error in
                XCTAssertNil(error)
            }
        }
    }

    private func createConnection() -> Connection {
        let host = read(fileName: "host.txt")
        let port = Int(read(fileName: "port.txt"))!
        let username = read(fileName: "username.txt")
        let password = read(fileName: "password.txt")
        let database = read(fileName: "database.txt")

        // Create connection with URL
        // return MySQLConnection(url: URL(string: "mysql://\(username):\(password)@\(host):\(port)/\(database)")!)

        return MySQLConnection(host: host, user: username, password: password, database: database, port: port)
    }
}
