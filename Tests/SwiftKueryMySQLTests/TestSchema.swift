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

import XCTest
import SwiftKuery
import Foundation

@testable import SwiftKueryMySQL

#if os(Linux)
let tableNameSuffix = "Linux"
#else
let tableNameSuffix = "OSX"
#endif

class TestSchema: XCTestCase {

    static var allTests: [(String, (TestSchema) -> () throws -> Void)] {
        return [
            ("testCreateTable", testCreateTable),
            ("testForeignKeys", testForeignKeys),
            ("testPrimaryKeys", testPrimaryKeys),
            ("testTypes", testTypes),
            ("testAutoIncrement", testAutoIncrement),
        ]
    }

    class MyTable: Table {
        let a = Column("a", Varchar.self, length: 10, defaultValue: "qiwi", collate: "utf8_general_ci")
        let b = Column("b", Int32.self, autoIncrement: true, primaryKey: true)
        let c = Column("c", Double.self, defaultValue: 4.95, check: "c > 0")

        let tableName = "MyTable" + tableNameSuffix
    }

    class MyNewTable: Table {
        let a = Column("a", Varchar.self, length: 10, defaultValue: "qiwi")
        let b = Column("b", Int32.self, autoIncrement: true, primaryKey: true)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)

        let tableName = "MyNewTable" + tableNameSuffix
    }

    func testCreateTable() {
        let t = MyTable()
        let tNew = MyNewTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { _ in
                    cleanUp(table: tNew.tableName, connection: connection) { _ in
                        t.create(connection: connection) { result in
                            if let error = result.asError {
                                XCTFail("Error in CREATE TABLE: \(error)")
                                return
                            }

                            let i1 = Insert(into: t, columns: [t.a, t.b], values: ["apple", 5])
                            executeQuery(query: i1, connection: connection) { result, rows in
                                XCTAssertNil(result.asError, "Error in INSERT")

                                let s1 = Select(from: t)
                                executeQuery(query: s1, connection: connection) { result, rows in
                                    XCTAssertNil(result.asError, "Error in SELECT")
                                    XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                    XCTAssertNotNil(rows, "SELECT returned no rows")

                                    if let resultSet = result.asResultSet {
                                        XCTAssertEqual(resultSet.titles.count, 3, "SELECT returned wrong number of titles")
                                        XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                        XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                        XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                    }

                                    XCTAssertEqual(rows?.count, 1, "SELECT returned wrong number of rows")
                                    if let row = rows?.first {
                                        XCTAssertEqual(row.count, 3, "SELECT returned wrong number of columns")
                                        XCTAssertEqual(row[0] as? String, "apple", "Wrong value in row 0 column 0")
                                        XCTAssertEqual(row[1] as? Int32, 5, "Wrong value in row 0 column 1")
                                        XCTAssertEqual(row[2] as? Double, 4.95, "Wrong value in row 0 column 2")
                                    }

                                    var index = Index("idx_err", on: t, columns: [tNew.a, desc(t.b)])
                                    index.create(connection: connection) { result in
                                        if let error = result.asError {
                                            XCTAssertEqual("\(error)", "Index contains columns that do not belong to its table.")
                                        } else {
                                            XCTFail("CREATE INDEX should return an error")
                                        }

                                        index = Index("idx_ok", unique: true, on: t, columns: [t.a, desc(t.b)])
                                        index.create(connection: connection) { result in
                                            XCTAssertNil(result.asError, "Error in CREATE INDEX")

                                            index.drop(connection: connection) { result in
                                                XCTAssertNil(result.asError, "Error in DROP INDEX")

                                                let migration = Migration(from: t, to: tNew, using: connection)
                                                migration.alterTableName() { result in
                                                    XCTAssertNil(result.asError, "Error in Migration")

                                                    migration.alterTableAdd(column: tNew.d) { result in
                                                        XCTAssertNil(result.asError, "Error in Migration")

                                                        let s2 = Select(from: tNew)
                                                        executeQuery(query: s2, connection: connection) { result, rows in
                                                            XCTAssertNil(result.asError, "Error in SELECT")
                                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                            XCTAssertNotNil(rows, "SELECT returned no rows")

                                                            if let resultSet = result.asResultSet {
                                                                XCTAssertEqual(resultSet.titles.count, 4, "SELECT returned wrong number of titles")
                                                                XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                                                XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                                                XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                                                XCTAssertEqual(resultSet.titles[3], "d", "Wrong column name for column 3")
                                                            }

                                                            XCTAssertEqual(rows?.count, 1, "SELECT returned wrong number of rows")
                                                            if let row = rows?.first {
                                                                XCTAssertEqual(row.count, 4, "SELECT returned wrong number of columns")
                                                                XCTAssertEqual(row[0] as? String, "apple", "Wrong value in row 0 column 0")
                                                                XCTAssertEqual(row[1] as? Int32, 5, "Wrong value in row 0 column 1")
                                                                XCTAssertEqual(row[2] as? Double, 4.95, "Wrong value in row 0 column 2")
                                                                XCTAssertEqual(row[3] as? Int32, 123, "Wrong value in row 0 column 3")
                                                            }
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

    class Table1: Table {
        let a = Column("a", Varchar.self, length: 10, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self, primaryKey: true)
        let c = Column("c", Double.self, defaultValue: 4.95)

        let tableName = "Table1" + tableNameSuffix
    }

    class Table2: Table {
        let a = Column("a", Varchar.self, length: 10, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)

        let tableName = "Table2" + tableNameSuffix
    }

    class Table3: Table {
        let a = Column("a", Varchar.self, length: 10, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)

        let tableName = "Table3" + tableNameSuffix
    }

    func testPrimaryKeys() {
        let t1 = Table1()
        let t2 = Table2()
        let t3 = Table3()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t1.tableName, connection: connection) { _ in
                    cleanUp(table: t2.tableName, connection: connection) { _ in
                        cleanUp(table: t3.tableName, connection: connection) { _ in
                            t1.create(connection: connection) { result in
                                if let error = result.asError {
                                    XCTAssertEqual("\(error)", "Conflicting definitions of primary key. ")
                                } else {
                                    XCTFail("CREATE TABLE with conflicting primary keys didn't fail")
                                }

                                t2.primaryKey(t2.c, t2.d).create(connection: connection) { result in
                                    if let error = result.asError {
                                        XCTAssertEqual("\(error)", "Conflicting definitions of primary key. ")
                                    } else {
                                        XCTFail("CREATE TABLE with conflicting primary keys didn't fail")
                                    }

                                    t3.primaryKey(t3.c, t3.d).create(connection: connection) { result in
                                        XCTAssertNil(result.asError, "Error in CREATE TABLE with valid primary keys")

                                        expectation.fulfill()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    class Table4: Table {
        let a = Column("a", Varchar.self, length: 10)
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self)

        let tableName = "Table4" + tableNameSuffix
    }

    class Table5: Table {
        let e = Column("e", Varchar.self, length: 10, primaryKey: true)
        let f = Column("f", Int32.self)

        let tableName = "Table5" + tableNameSuffix
    }

    func testForeignKeys() {
        let t4 = Table4()
        let t5 = Table5()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t5.tableName, connection: connection) { _ in
                    cleanUp(table: t4.tableName, connection: connection) { _ in

                        t4.primaryKey(t4.a, t4.b).create(connection: connection) { result in
                            if let error = result.asError {
                                XCTFail("Error in CREATE TABLE: \(error)")
                                return
                            }

                            t5.foreignKey([t5.e, t5.f], references: [t4.a, t4.b]).create(connection: connection) { result in
                                XCTAssertNil(result.asError, "Error in CREATE TABLE with foreign key")

                                expectation.fulfill()
                            }
                        }
                    }
                }
            }
        })
    }

    class TypesTable: Table {
        let a = Column("a", Varchar.self, length: 30, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Varchar.self, length: 10)
        let c = Column("c", Char.self, length: 10)

        let d = Column("d", Int16.self)
        let e = Column("e", Int32.self)
        let f = Column("f", Int64.self)

        let g = Column("g", Float.self)
        let h = Column("h", Double.self)

        let i = Column("i", SQLDate.self)
        let j = Column("j", Time.self)
        let k = Column("k", Timestamp.self)

        let tableName = "TypesTable" + tableNameSuffix
    }

    func testTypes() {
        let t = TypesTable()
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { _ in
                    t.create(connection: connection) { result in
                        if let error = result.asError {
                            XCTFail("Error in CREATE TABLE: \(error)")
                            return
                        }

                        // These tests require strict SQL mode
                        executeRawQuery("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'", connection: connection) { result, rows in
                            XCTAssertNil(result.asError, "Error in SET SESSION sql_mode to STRICT_TRANS_TABLES")

                            let date = Date()
                            let i1 = Insert(into: t, values: "apple", "passion fruit", "peach", 123456789, 123456789, 123456789, -0.53, 123.4567, date, date, date)
                            executeQuery(query: i1, connection: connection) { result, rows in
                                if let error = result.asError {
                                    XCTAssertEqual("\(error)", "ERROR 1406: Data too long for column 'b' at row 1")
                                } else {
                                    XCTFail("No error in INSERT of too long value into varchar column.")
                                }

                                let i2 = Insert(into: t, values: "apple", "banana", "peach", 123456789, 123456789, 123456789, -0.53, 123.4567, date, date, date)
                                executeQuery(query: i2, connection: connection) { result, rows in
                                    if let error = result.asError {
                                        XCTAssertEqual("\(error)", "ERROR 1264: Out of range value for column 'd' at row 1")
                                    } else {
                                        XCTFail("No error in INSERT of too long value into smallint column.")
                                    }

                                    let i3 = Insert(into: t, values: "apple", "banana", "peach", 1234, 123456789, 123456789, -0.53, 123.4567, date, date, date)
                                    executeQuery(query: i3, connection: connection) { result, rows in
                                        XCTAssertNil(result.asError, "Error in INSERT")

                                        let s1 = Select(from: t)
                                        executeQuery(query: s1, connection: connection) { result, rows in
                                            XCTAssertNil(result.asError, "Error in SELECT")
                                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                            XCTAssertNotNil(rows, "SELECT returned no rows")

                                            XCTAssertEqual(rows?.count, 1, "SELECT returned wrong number of rows")
                                            if let row = rows?.first {
                                                XCTAssertEqual(row.count, 11, "SELECT returned wrong number of columns")
                                                XCTAssertEqual(row[0] as? String, "apple", "Wrong value in row 0 column 0")
                                                XCTAssertEqual(row[1] as? String, "banana", "Wrong value in row 0 column 1")
                                                XCTAssertEqual(row[2] as? String, "peach", "Wrong value in row 0 column 2")
                                                XCTAssertEqual(row[3] as? Int16, 1234, "Wrong value in row 0 column 3")
                                                XCTAssertEqual(row[4] as? Int32, 123456789, "Wrong value in row 0 column 4")
                                                XCTAssertEqual(row[5] as? Int64, 123456789, "Wrong value in row 0 column 5")
                                                XCTAssertEqual(row[6] as? Float, -0.53, "Wrong value in row 0 column 6")
                                                XCTAssertEqual(row[7] as? Double, 123.4567, "Wrong value in row 0 column 7")
                                                XCTAssertEqual(row[8] as? String, MySQLConnection.dateFormatter.string(from: date), "Wrong value in row 0 column 8")
                                                XCTAssertEqual(row[9] as? String, MySQLConnection.timeFormatter.string(from: date), "Wrong value in row 0 column 9")

                                                if let timestamp = row[10] as? Date {
                                                    XCTAssertEqual(Int(timestamp.timeIntervalSince1970), Int(date.timeIntervalSince1970), "Wrong value in row 0 column 10")
                                                } else {
                                                    XCTFail("Cast to Date failed for '\(row[10] ?? "nil")' in row 0 column 10")
                                                }
                                            }
                                            expectation.fulfill()
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

    class AutoIncrement1: Table {
        let a = Column("a", String.self)
        let b = Column("b", Int32.self, autoIncrement: true, primaryKey: true)

        let tableName = "AutoIncrement1" + tableNameSuffix
    }

    class AutoIncrement2: Table {
        let a = Column("a", String.self, primaryKey: true)
        let b = Column("b", Int32.self, autoIncrement: true)

        let tableName = "AutoIncrement2" + tableNameSuffix
    }

    class AutoIncrement3: Table {
        let a = Column("a", String.self)
        let b = Column("b", String.self, autoIncrement: true, primaryKey: true)

        let tableName = "AutoIncrement3" + tableNameSuffix
    }

    func testAutoIncrement() {
        let t1 = AutoIncrement1()
        let t2 = AutoIncrement2()
        let t3 = AutoIncrement3()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t1.tableName, connection: connection) { result in
                    cleanUp(table: t2.tableName, connection: connection) { result in
                        cleanUp(table: t3.tableName, connection: connection) { result in

                            t1.create(connection: connection) { result in
                                XCTAssertEqual(result.success, true, "CREATE TABLE failed for \(t1.tableName)")

                                t2.create(connection: connection) { result in
                                    XCTAssertEqual(result.success, false, "CREATE TABLE non primary key auto increment column didn't fail")

                                    t3.create(connection: connection) { result in
                                        XCTAssertEqual(result.success, false, "CREATE TABLE non integer auto increment column didn't fail")

                                        expectation.fulfill()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
