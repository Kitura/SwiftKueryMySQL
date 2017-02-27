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

class TestColumnTypes: MySQLTest {

    static var allTests: [(String, (TestColumnTypes) -> () throws -> Void)] {
        return [
            ("testColumnTypes", testColumnTypes),
            ("testBlobs", testBlobs),
            ("testBlobsNoCopyBytes", testBlobsNoCopyBytes)
        ]
    }

    class MyTable : Table {
        let tableName = "TestColumnTypes"
    }

    func testColumnTypes() {
        performTest(asyncTasks: { connection in
            let t = MyTable()
            defer {
                cleanUp(table: t.tableName, connection: connection) { _ in }
            }

            executeRawQuery("CREATE TABLE " +  t.tableName + " (tinyintCol tinyint, smallintCol smallint, mediumintCol mediumint, intCol int, bigintCol bigint, floatCol float, doubleCol double, dateCol date, timeCol time, datetimeCol datetime, blobCol blob, enumCol enum('enum1', 'enum2', 'enum3'), setCol set('smallSet', 'mediumSet', 'largeSet'), jsonCol json)", connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
            }

            let rawInsert = "INSERT INTO " + t.tableName + " (tinyintCol, smallintCol, mediumintCol, intCol, bigintCol, floatCol, doubleCol, dateCol, timeCol, datetimeCol, blobCol, enumCol, setCol, jsonCol) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

            let parameters: [Any] = [Int8.max, Int16.max, Int16.min, Int32.max, Int64.max, Float.greatestFiniteMagnitude, Double.greatestFiniteMagnitude, "2017-02-27", "13:51:52", "2017-02-27 13:51:52", Data(repeating: 0x84, count: 96), "enum2", "mediumSet", "{\"x\": 1}"]

            executeRawQueryWithParameters(rawInsert, connection: connection, parameters: parameters) { result, rows in
                XCTAssertEqual(result.success, true, "INSERT failed")
                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                let rawSelect = "SELECT * from " + t.tableName
                executeRawQuery(rawSelect, connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "SELECT failed")
                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                    XCTAssertNotNil(rows, "SELECT returned no rows")
                    if let rows = rows {
                        for (index, selected) in rows[0].enumerated() {
                            let inserted = parameters[index]
                            if let selected = selected {
                                if inserted is Data {
                                    let insertedData = inserted as! Data
                                    let selectedData = selected as! Data
                                    if insertedData != selectedData {
                                        XCTFail("Column \(index+1) inserted Data (\(insertedData.map{ String(format: "%02x", $0) }.joined())) != selected Data (\(selectedData.map{ String(format: "%02x", $0) }.joined()))")
                                    }
                                } else {
                                    XCTAssertEqual(String(describing: inserted), String(describing: selected), "Column \(index+1) inserted value (type: \(type(of: inserted))) != selected value (type: \(type(of: selected)))")
                                }
                            } else {
                                XCTFail("nil value selected instead of inserted value: \(inserted) for column \(index)")
                            }
                        }
                    }
                }
            }
        })
    }

    func testBlobs() {
        testBlobs(copyBlobData: true)
    }

    func testBlobsNoCopyBytes() {
        testBlobs(copyBlobData: false)
    }

    func testBlobs(copyBlobData: Bool) {
        performTest(copyBlobData: copyBlobData, asyncTasks: { connection in
            let t = MyTable()
            defer {
                cleanUp(table: t.tableName, connection: connection) { _ in }
            }

            executeRawQuery("CREATE TABLE " +  t.tableName + " (idCol int, blobCol blob)", connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
            }

            let rawInsert = "INSERT INTO " + t.tableName + " (idCol, blobCol) VALUES (?, ?)"

            let insertedBlobs = [Data(repeating: 0x84, count: 96), Data(repeating: 0x70, count: 50)]

            executeRawQueryWithParameters(rawInsert, connection: connection, parameters: [1, insertedBlobs[0]]) { result, rows in
                XCTAssertEqual(result.success, true, "INSERT failed")
                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
            }

            executeRawQueryWithParameters(rawInsert, connection: connection, parameters: [2, insertedBlobs[1]]) { result, rows in
                XCTAssertEqual(result.success, true, "INSERT failed")
                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
            }

            let rawSelect = "SELECT * from " + t.tableName + " order by idCol"
            connection.execute(rawSelect) { result in
                XCTAssertEqual(result.success, true, "SELECT failed")
                XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                XCTAssertNotNil(result.asResultSet, "SELECT returned no result set")

                if let resultSet = result.asResultSet {
                    var index = 0
                    var firstSelectedBlob: Data? = nil
                    for row in resultSet.rows {
                        let selectedBlob1 = row[1] as! Data
                        if index == 0 {
                            firstSelectedBlob = selectedBlob1
                        }

                        XCTAssertEqual(selectedBlob1, insertedBlobs[index], "Inserted Data (\(insertedBlobs[index].map{ String(format: "%02x", $0) }.joined())) != selected Data (\(selectedBlob1.map{ String(format: "%02x", $0) }.joined()))")

                        index += 1
                    }

                    XCTAssertEqual(index, 2, "Returned row count (\(index)) != Expected row count (2)")

                    if let firstSelectedBlob = firstSelectedBlob {
                        if copyBlobData {
                            XCTAssertEqual(firstSelectedBlob, insertedBlobs[0], "Inserted Data (\(insertedBlobs[0].map{ String(format: "%02x", $0) }.joined())) != first selected Data (\(firstSelectedBlob.map{ String(format: "%02x", $0) }.joined()))")
                        } else {
                            XCTAssertNotEqual(firstSelectedBlob, insertedBlobs[0], "Inserted Data (\(insertedBlobs[0].map{ String(format: "%02x", $0) }.joined())) == first selected Data (\(firstSelectedBlob.map{ String(format: "%02x", $0) }.joined()))")
                        }
                    }
                }
            }
        })
    }
}
