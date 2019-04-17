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
import SwiftKuery

#if os(Linux)
let tableUpdate = "tableUpdateLinux"
#else
let tableUpdate = "tableUpdateOSX"
#endif

class TestUpdate: XCTestCase {

    static var allTests: [(String, (TestUpdate) -> () throws -> Void)] {
        return [
            ("testUpdateAndDelete", testUpdateAndDelete),
            ("testUpdateNilValue", testUpdateNilValue),
        ]
    }

    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")

        let tableName = tableUpdate
    }

    func testUpdateAndDelete () {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { _ in

                    executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i1 = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows?.count, 6, "SELECT returned wrong number of rows: \(String(describing: rows?.count)) instead of 6")

                                let u1 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                    .where(t.a == "banana")
                                executeQuery(query: u1, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                    let u2 = Update(t, set: [(t.a, "peach"), (t.b, 2)])
                                        .where(t.a == "apple")
                                    executeQuery(query: u2, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "UPDATE failed")
                                        XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")
                                        XCTAssert((result.asValue as! String).contains("2"), "UPDATE affected wrong number of rows: \(result.asValue!)")

                                        let s2 = Select(t.a, t.b, from: t)
                                            .where(t.a == "banana")
                                        executeQuery(query: s2, connection: connection) { result, rows in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            XCTAssertEqual(rows?.count, 0, "SELECT should not return any rows")

                                            let d1 = Delete(from: t)
                                                .where(t.b == "2")
                                            executeQuery(query: d1, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "DELETE failed")
                                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")
                                                XCTAssert((result.asValue as! String).contains("5"), "DELETE affected wrong number of rows: \(result.asValue!)")

                                                executeQuery(query: s1, connection: connection) { result, rows in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                    XCTAssertEqual(rows?.count, 1, "SELECT returned wrong number of rows: \(String(describing: rows?.count)) instead of 1")

                                                    let d2 = Delete(from: t)
                                                    executeQuery(query: d2, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "DELETE failed")
                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError!)")

                                                        executeQuery(query: s1, connection: connection) { result, rows in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            XCTAssertEqual(rows?.count, 0, "SELECT should not return any rows")

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
        })
    }

    func testUpdateNilValue() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { _ in

                    executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let insert = Insert(into: t, rows: [["apple", 10], ["apricot", 3], ["banana", 17], ["apple", 17], ["banana", -7], ["banana", 27]])
                        executeQuery(query: insert, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let update = Update(t, set:[(t.a, nil)], where: t.a == "apple")
                            executeQuery(query: update, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "UPDATE failed")
                                XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                let select = Select(from: t)
                                executeQuery(query: select, connection: connection) { result, rows in
                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                    XCTAssertNotNil(rows, "Expected rows but none returned")
                                    for row in rows! {
                                        XCTAssertNotEqual(row[0] as? String, "apple", "Row returned with \"apple\" instead of expected value \"nil\"")
                                    }

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
