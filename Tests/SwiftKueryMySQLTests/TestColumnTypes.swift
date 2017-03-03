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
    import CmySQLlinux
#else
    import CmySQLosx
#endif

class TestColumnTypes: MySQLTest {

    static var allTests: [(String, (TestColumnTypes) -> () throws -> Void)] {
        return [
            ("testColumnTypes", testColumnTypes),
            ("testUnhandledParameterType", testUnhandledParameterType),
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
            cleanUp(table: t.tableName, connection: connection) { _ in }
            defer {
                cleanUp(table: t.tableName, connection: connection) { _ in }
            }

            executeRawQuery("CREATE TABLE " +  t.tableName + " (tinyintCol tinyint, smallintCol smallint, unsignedmediumintCol mediumint, intCol int, bigintCol bigint, floatCol float, doubleCol double, dateCol date, timeCol time, datetimeCol datetime, mySqlTimeCol timestamp, blobCol blob, enumCol enum('enum1', 'enum2', 'enum3'), setCol set('smallSet', 'mediumSet', 'largeSet'), jsonCol json, nulCol int, emptyCol varchar(10))", connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
            }

            let rawInsert = "INSERT INTO " + t.tableName + " (tinyintCol, smallintCol, unsignedmediumintCol, intCol, bigintCol, floatCol, doubleCol, dateCol, timeCol, datetimeCol, mySqlTimeCol, blobCol, enumCol, setCol, jsonCol, nulCol, emptyCol) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

            var ts = MYSQL_TIME()
            ts.year = 2017;
            ts.month = 03;
            ts.day = 03;
            ts.hour = 11;
            ts.minute = 22;
            ts.second = 59;

            let parameters: [Any?] = [Int8.max, Int16.max, UInt16.max, Int32.max, Int64.max, Float.greatestFiniteMagnitude, Double.greatestFiniteMagnitude, "2017-02-27", "13:51:52", "2017-02-27 13:51:52", ts, Data(repeating: 0x84, count: 96), "enum2", "mediumSet", "{\"x\": 1}", nil, ""]

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
                            if let selected = selected, let inserted = inserted {
                                if inserted is Data {
                                    let insertedData = inserted as! Data
                                    let selectedData = selected as! Data
                                    XCTAssertEqual(insertedData, selectedData, "Column \(index+1) inserted Data (\(insertedData.hexString())) is not equal to selected Data (\(selectedData.hexString()))")
                                } else if inserted is MYSQL_TIME {
                                    let time = inserted as! MYSQL_TIME
                                    let selectedTime = selected as! String
                                    let formattedTime = "\(time.year)-\(time.month.pad())-\(time.day.pad()) \(time.hour.pad()):\(time.minute.pad()):\(time.second.pad())"
                                    XCTAssertEqual(formattedTime, selectedTime, "Column \(index+1) inserted Data (\(formattedTime)) is not equal to selected Data (\(selectedTime))")
                                } else {
                                    XCTAssertEqual(String(describing: inserted), String(describing: selected), "Column \(index+1) inserted value (\(inserted)) (type: \(type(of: inserted))) != selected value (\(selected)) (type: \(type(of: selected)))")
                                }
                            } else if inserted == nil {
                                XCTAssertNil(selected, "value: \(selected) selected instead of inserted value: nil for column \(index)")
                            } else {
                                XCTFail("nil value selected instead of inserted value: \(inserted) for column \(index)")
                            }
                        }
                    }
                }
            }
        })
    }

    func testUnhandledParameterType() {
        performTest(asyncTasks: { connection in
            let t = MyTable()
            cleanUp(table: t.tableName, connection: connection) { _ in }
            defer {
                cleanUp(table: t.tableName, connection: connection) { _ in }
            }

            executeRawQuery("CREATE TABLE " +  t.tableName + " (idCol int, randomCol varchar(100))", connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
            }

            let rawInsert = "INSERT INTO " + t.tableName + " (idCol, randomCol) VALUES (?, ?)"

            let unhandledParameter = URL(string: "http://www.kitura.io")!

            executeRawQueryWithParameters(rawInsert, connection: connection, parameters: [1, unhandledParameter]) { result, rows in
                XCTAssertEqual(result.success, true, "INSERT failed")
                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                let rawSelect = "SELECT * from " + t.tableName
                executeRawQuery(rawSelect, connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "SELECT failed")
                    XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                    XCTAssertNotNil(rows, "SELECT returned no rows")
                    if let rows = rows {
                        let inserted = unhandledParameter
                        let selected = rows[0][1]!
                        XCTAssertEqual(String(describing: inserted), String(describing: selected), "SELECT failed")
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
            cleanUp(table: t.tableName, connection: connection) { _ in }
            defer {
                cleanUp(table: t.tableName, connection: connection) { _ in }
            }

            executeRawQuery("CREATE TABLE " +  t.tableName + " (idCol int, blobCol blob)", connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
            }

            let rawInsert = "INSERT INTO " + t.tableName + " (idCol, blobCol) VALUES (?, ?)"

            let insertedBlobs = [Data(repeating: 0x84, count: 10), Data(repeating: 0x70, count: 10000), Data(repeating: 0x52, count: 1), Data(repeating: 0x40, count: 10000)]

            let parametersArray = [[0, insertedBlobs[0]], [1, [UInt8](insertedBlobs[1])], [2, insertedBlobs[2]], [3, insertedBlobs[3]]]

            executeRawQueryWithParameters(rawInsert, connection: connection, parametersArray: parametersArray) { result, rows in
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

                        XCTAssertEqual(selectedBlob1, insertedBlobs[index], "Inserted Data (\(insertedBlobs[index].hexString())) is not equal to selected Data (\(selectedBlob1.hexString()))")

                        index += 1
                    }

                    XCTAssertEqual(index, parametersArray.count, "Returned row count (\(index)) != Expected row count (\(parametersArray.count))")

                    if let firstSelectedBlob = firstSelectedBlob {
                        if copyBlobData {
                            XCTAssertEqual(firstSelectedBlob, insertedBlobs[0], "Inserted Data (\(insertedBlobs[0].hexString())) is not equal to first selected Data (\(firstSelectedBlob.hexString()))")
                        } else {
                            XCTAssertNotEqual(firstSelectedBlob, insertedBlobs[0], "Inserted Data (\(insertedBlobs[0].hexString())) is equal to first selected Data (\(firstSelectedBlob.hexString()))")
                        }
                    }
                }
            }
        })
    }
}

extension Data {
    public func hexString() -> String {
        return self.map { String(format: "%02x", $0) }.joined()
    }
}

extension UInt32 {
    public func pad() -> String {
        return String(format: "%02u", self)
    }
}
