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

import XCTest
import Foundation
import SwiftKuery

#if os(Linux)
let tableInsert = "tableInsertLinux"
let tableInsert2 = "tableInsert2Linux"
let tableInsert3 = "tableInsert3Linux"
#else
let tableInsert = "tableInsertOSX"
let tableInsert2 = "tableInsert2OSX"
let tableInsert3 = "tableInsert3OSX"
#endif

class TestInsert: XCTestCase {

    static var allTests: [(String, (TestInsert) -> () throws -> Void)] {
        return [
            ("testInsert", testInsert),
            ("testInsertID", testInsertID)
        ]
    }

    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")

        let tableName = tableInsert
    }

    class MyTable2 : Table {
        let a = Column("a")
        let b = Column("b")

        let tableName = tableInsert2
    }

    class MyTable3 : Table {
        let a = Column("a", autoIncrement: true, primaryKey: true)
        let b = Column("b")

        let tableName = tableInsert3
    }

    func testInsert() {
        let t = MyTable()
        let t2 = MyTable2()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (a varchar(40), b integer)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                    executeRawQuery("CREATE TABLE " +  packName(t2.tableName) + " (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i1 = Insert(into: t, values: "apple", 10)
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let i2 = Insert(into: t, valueTuples: (t.a, "apricot"), (t.b, "3"))
                            executeQuery(query: i2, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                let i3 = Insert(into: t, columns: [t.a, t.b], values: ["banana", 17])
                                executeQuery(query: i3, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "INSERT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                    let i4 = Insert(into: t, rows: [["apple", 17], ["banana", -7], ["banana", 27]])
                                    executeQuery(query: i4, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "INSERT failed")
                                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                        let i5 = Insert(into: t, rows: [["apple", 5], ["banana", 10], ["banana", 3]])
                                        executeQuery(query: i5, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "INSERT failed")
                                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                            let i6 = Insert(into: t2, Select(from: t).where(t.a == "apple"))
                                            executeQuery(query: i6, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "INSERT failed")
                                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                                let s1 = Select(from: t)
                                                executeQuery(query: s1, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                    XCTAssertEqual(rows?.count, 9, "INSERT returned wrong number of rows: \(String(describing: rows?.count)) instead of 9")

                                                    let drop = Raw(query: "DROP TABLE", table: t)
                                                    executeQuery(query: drop, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")

                                                        cleanUp(table: t.tableName, connection: connection) { _ in
                                                            cleanUp(table: t2.tableName, connection: connection) { _ in
                                                                expectation.fulfill()
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    func testInsertID(){
        let t3 = MyTable3()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                executeRawQuery("CREATE TABLE " +  packName(t3.tableName) + " (a integer NOT NULL AUTO_INCREMENT , b integer, PRIMARY KEY (a))", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                    let i = Insert(into: t3, valueTuples: [(t3.b, 5)], returnID: true)
                    executeQuery(query: i, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        XCTAssertNotNil(rows, "INSERT returned no rows")
                        XCTAssertEqual(rows?.count, 1, "INSERT returned wrong number of rows: \(String(describing: rows?.count)) instead of 1")
                        XCTAssertEqual(rows?[0][0] as? Int64, 1, "Incorrect autoIncrement ID value returned")
                        if let resultSet = result.asResultSet {
                            let titles = resultSet.titles
                            XCTAssertEqual(titles[0], "a", "Incorrect id column name: \(titles[0]) instead of a")
                        } else {
                            XCTFail("Unable to retrieve column names")
                        }

                        let i = Insert(into: t3, valueTuples: [(t3.b, 8)], returnID: true)
                        executeQuery(query: i, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            XCTAssertNotNil(rows, "INSERT returned no rows")
                            XCTAssertEqual(rows?.count, 1, "INSERT returned wrong number of rows: \(String(describing: rows?.count)) instead of 1")
                            XCTAssertEqual(rows?[0][0] as? Int64, 2, "Incorrect autoIncrement ID value returned")
                            if let resultSet = result.asResultSet {
                                let titles = resultSet.titles
                                XCTAssertEqual(titles[0], "a", "Incorrect id column name: \(titles[0]) instead of a")
                            } else {
                                XCTFail("Unable to retrieve column names")
                            }

                            let drop = Raw(query: "DROP TABLE", table: t3)
                            executeQuery(query: drop, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "DROP TABLE failed")
                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")

                                cleanUp(table: t3.tableName, connection: connection) { _ in
                                    expectation.fulfill()
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
