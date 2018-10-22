/**
 Copyright IBM Corporation 2018
 
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

import XCTest
import Foundation
import Dispatch
import SwiftKuery

#if os(Linux)
let tableTimings = "tableTimingsLinux"
#else
let tableTimings = "tableTimingsOSX"
#endif

class TestTimingWindows: XCTestCase {

    static var allTests: [(String, (TestTimingWindows) -> () throws -> Void)] {
        return [
            ("testExecutePassingQueryWithNoParameters", testExecutePassingQueryWithNoParameters),
            ("testExecutePassingQueryWithParameters", testExecutePassingQueryWithParameters),
            ("testExecutePassingRawQueryWithNoParameters", testExecutePassingRawQueryWithNoParameters),
            ("testExecutePassingRawQueryWithParameters",testExecutePassingRawQueryWithParameters),
        ]
    }

    class MyTable : Table {
        let a = Column("a", Varchar.self, length: 10, defaultValue: "qiwi", collate: "utf8_general_ci")
        let b = Column("b", Int32.self)

        let tableName = tableTimings
    }

    // public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
    func testExecutePassingQueryWithNoParameters() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }

            cleanUp(table: t.tableName, connection: connection) { _ in
                t.create(connection: connection) { result in
                    if let error = result.asError {
                        XCTFail("Error in CREATE TABLE: \(error)")
                        return
                    }

                    let insertCount = 1000
                    let query = Insert(into: t, values: "apple", 10)
                    self.executeInsertQuery(count: insertCount, query: query, connection: connection) { result in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        cleanUp(table: t.tableName, connection: connection) { _ in
                            semaphore.signal()
                        }
                    } // self.executeInsertQuery
                } // t.create
            } // cleanup
            semaphore.wait()
            expectation.fulfill()
        })
    }

    func executeInsertQuery(count index: Int, query: Query, connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        connection.execute(query: query) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            if index >= 0 {
                self.executeInsertQuery(count: index - 1, query: query, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    // public func execute(query: Query, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
    func testExecutePassingQueryWithParameters() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }

            cleanUp(table: t.tableName, connection: connection) { _ in
                t.create(connection: connection) { result in
                    if let error = result.asError {
                        XCTFail("Error in CREATE TABLE: \(error)")
                        return
                    }

                    let insertCount = 1000
                    let query = Insert(into: t, values: Parameter(), Parameter())
                    self.executeInsertQueryWithParameters(count: insertCount, query: query, parameters: ["apple", 10], connection: connection) { result in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        cleanUp(table: t.tableName, connection: connection) { _ in
                            semaphore.signal()
                        }
                    } // self.executeInsertQuery
                } // t.create
            } // cleanup
            semaphore.wait()
            expectation.fulfill()
        })
    }

    func executeInsertQueryWithParameters(count index: Int, query: Query, parameters: [Any?], connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        connection.execute(query: query, parameters: parameters) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            if index >= 0 {
                self.executeInsertQueryWithParameters(count: index - 1, query: query, parameters: parameters, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    // public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
    func testExecutePassingRawQueryWithNoParameters() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }

            cleanUp(table: t.tableName, connection: connection) { _ in
                t.create(connection: connection) { result in
                    if let error = result.asError {
                        XCTFail("Error in CREATE TABLE: \(error)")
                        return
                    }

                    let insertCount = 1000
                    let raw = "insert into " + t.tableName + " values(\"apple\", 10)"
                    self.executeRawInsertQuery(count: insertCount, raw: raw, connection: connection) { result in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        cleanUp(table: t.tableName, connection: connection) { _ in
                            semaphore.signal()
                        }
                    } // self.executeInsertQuery
                } // t.create
            } // cleanup
            semaphore.wait()
            expectation.fulfill()
        })
    }

    func executeRawInsertQuery(count index: Int, raw: String, connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        connection.execute(raw) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            if index >= 0 {
                self.executeRawInsertQuery(count: index - 1, raw: raw, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    // public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
    func testExecutePassingRawQueryWithParameters() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }

            cleanUp(table: t.tableName, connection: connection) { _ in
                t.create(connection: connection) { result in
                    if let error = result.asError {
                        XCTFail("Error in CREATE TABLE: \(error)")
                        return
                    }

                    let insertCount = 1000
                    let raw = "insert into " + t.tableName + " values(?, ?)"
                    self.executeRawInsertQueryWithParameters(count: insertCount, raw: raw, parameters: ["apple", 10], connection: connection) { result in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        cleanUp(table: t.tableName, connection: connection) { _ in
                            semaphore.signal()
                        }
                    } // self.executeInsertQuery
                } // t.create
            } // cleanup
            semaphore.wait()
            expectation.fulfill()
        })
    }

    func executeRawInsertQueryWithParameters(count index: Int, raw: String, parameters: [Any?], connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        connection.execute(raw, parameters: parameters) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            if index >= 0 {
                self.executeRawInsertQueryWithParameters(count: index - 1, raw: raw, parameters: parameters, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }
}
