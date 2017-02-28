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
import Foundation

import SwiftKuery
@testable import SwiftKueryMySQL

class MySQLTest: XCTestCase {
    func performTest(copyBlobData: Bool = true, line: Int = #line, asyncTasks: @escaping (MySQLConnection) -> Void...) {
        guard let connection = createConnection(taskCount: asyncTasks.count, copyBlobData: copyBlobData) else {
            return
        }

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

    private func createConnection(taskCount: Int, copyBlobData: Bool) -> MySQLConnection? {
        do {
            let connectionFile = #file.replacingOccurrences(of: "MySQLTest.swift", with: "connection.json")
            let data = Data(referencing: try NSData(contentsOfFile: connectionFile))
            let json = try JSONSerialization.jsonObject(with: data)

            if let dictionary = json as? [String: String] {
                let host = dictionary["host"]
                let username = dictionary["username"]
                let password = dictionary["password"]
                let database = dictionary["database"]
                var port: Int? = nil
                if let portString = dictionary["port"] {
                    port = Int(portString)
                }

                let randomBinary: UInt32
                #if os(Linux)
                    randomBinary = UInt32(random() % 2)
                #else
                    randomBinary = arc4random_uniform(2)
                #endif

                if !copyBlobData || randomBinary == 0 {
                    if taskCount > 1 {
                        return MySQLThreadSafeConnection(host: host, user: username, password: password, database: database, port: port, copyBlobData: copyBlobData)
                    } else {
                        return MySQLConnection(host: host, user: username, password: password, database: database, port: port, copyBlobData: copyBlobData)
                    }
                } else {
                    var urlString = "mysql://"
                    if let username = username, let password = password {
                        urlString += "\(username):\(password)@"
                    }
                    urlString += host ?? "localhost"
                    if let port = port {
                        urlString += ":\(port)"
                    }
                    if let database = database {
                        urlString += "/\(database)"
                    }

                    if let url = URL(string: urlString) {
                        if taskCount > 1 {
                            return MySQLThreadSafeConnection(url: url)
                        } else {
                            return MySQLConnection(url: url)
                        }
                    }
                    XCTFail("Invalid URL format: \(urlString)")
                }
            } else {
                XCTFail("Invalid format for connection.json contents: \(json)")
            }
        } catch {
            XCTFail(error.localizedDescription)
        }

        return nil
    }
}
