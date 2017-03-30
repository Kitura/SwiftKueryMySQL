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
import SwiftKueryMySQL

#if os(Linux)
    import CmySQLlinux
#else
    import CmySQLosx
#endif

class TestColumnTypes: MySQLTest {

    static var allTests: [(String, (TestColumnTypes) -> () throws -> Void)] {
        return [
            ("testColumnTypesNoBatch", testColumnTypesNoBatch),
            ("testColumnTypesBatch", testColumnTypesBatch),
            ("testUnhandledParameterType", testUnhandledParameterType),
            ("testBlobs", testBlobs)
        ]
    }

    class MyTable : Table {
        let tableName = "TestColumnTypes"
    }

    func testColumnTypesNoBatch() {
        testColumnTypes(batchParameters: false)
    }

    func testColumnTypesBatch() {
        testColumnTypes(batchParameters: true)
    }

    func testColumnTypes(batchParameters: Bool) {
        performTest(usePool: false, timeout: 60, asyncTasks: { connection in
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

            var ts1 = MYSQL_TIME()
            ts1.year = 2017
            ts1.month = 03
            ts1.day = 03
            ts1.hour = 11
            ts1.minute = 22
            ts1.second = 59

            var ts2 = ts1
            ts2.year = 2018

            let parameters1: [Any?] = [Int8.max, Int16.max, UInt16.max, Int32.max, Int64.max, Float.greatestFiniteMagnitude, Double.greatestFiniteMagnitude, "2017-02-27", "13:51:52", "2017-02-27 13:51:52", ts1, Data(repeating: 0x84, count: 96), "enum2", "mediumSet", "{\"x\": 1}", nil, ""]

            let parameters2: [Any?] = [Int8.min, Int16.min, UInt16.min, Int32.min, Int64.min, Float.leastNonzeroMagnitude, Double.leastNonzeroMagnitude, "2017-03-06", "13:41:05", "2017-03-06 13:41:05", ts2, Data(repeating: 0x72, count: 75), "enum1", "largeSet", "{\"y\": 2}", nil, "abc"]

            let parametersCount = 500

            var error: Error? = nil
            let start = Date.timeIntervalSinceReferenceDate
            if batchParameters {
                var parametersArray = Array(repeating: parameters2, count: parametersCount*2)
                for index in 0 ..< parametersCount {
                    parametersArray[index * 2] = parameters1
                }

                (connection as! MySQLConnection).execute(rawInsert, parametersArray: parametersArray) { result in
                    error = result.asError
                }
            } else {
                for index in 0 ..< parametersCount*2 {
                    let parameters = index % 2 == 0 ? parameters1 : parameters2
                    connection.execute(rawInsert, parameters: parameters) { result in
                        error = result.asError
                    }
                    if error != nil {
                        break
                    }
                }
            }
            let end = Date.timeIntervalSinceReferenceDate

            if let error = error {
                XCTFail("Error in INSERT: \(error)")
            } else {
                print("INSERT with batchParameters: \(batchParameters) took \(end-start) seconds for \(parametersCount) rows")
            }

            let selectCount = "SELECT count(*) from " + t.tableName
            executeRawQuery(selectCount, connection: connection) { result, rows in
                XCTAssertEqual(result.success, true, "SELECT failed")
                XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                XCTAssertNotNil(rows, "SELECT returned no rows")
                if let rows = rows {
                    let rowCount = rows[0][0]
                    XCTAssertEqual(rowCount as? Int64, Int64(parametersCount * 2), "Incorrect number of rows inserted: \(String(describing: rowCount)) (type: \(type(of: rowCount)))")
                }
            }

            let rawSelect = "SELECT * from " + t.tableName
            connection.execute(rawSelect) { result in
                XCTAssertEqual(result.success, true, "SELECT failed")
                XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                let resultSet = result.asResultSet
                XCTAssertNotNil(resultSet, "SELECT returned no resultSet")
                if let resultSet = resultSet {
                    for (rowIndex, row) in resultSet.rows.enumerated() {
                        let parameters = rowIndex % 2 == 0 ? parameters1 : parameters2
                        for (columnIndex, selected) in row.enumerated() {
                            let inserted = parameters[columnIndex]
                            if let selected = selected, let inserted = inserted {
                                if inserted is Data {
                                    let insertedData = inserted as! Data
                                    let selectedData = selected as! Data
                                    XCTAssertEqual(insertedData, selectedData, "Column \(columnIndex+1) inserted Data (\(insertedData.hexString())) is not equal to selected Data (\(selectedData.hexString()))")
                                } else if inserted is MYSQL_TIME {
                                    let time = inserted as! MYSQL_TIME
                                    let selectedTime = selected as! String
                                    let formattedTime = "\(time.year)-\(time.month.pad())-\(time.day.pad()) \(time.hour.pad()):\(time.minute.pad()):\(time.second.pad())"
                                    XCTAssertEqual(formattedTime, selectedTime, "Column \(columnIndex+1) inserted Data (\(formattedTime)) is not equal to selected Data (\(selectedTime))")
                                } else {
                                    XCTAssertEqual(String(describing: inserted), String(describing: selected), "Column \(columnIndex+1) inserted value (\(inserted)) (type: \(type(of: inserted))) != selected value (\(selected)) (type: \(type(of: selected)))")
                                }
                            } else if inserted == nil {
                                XCTAssertNil(selected, "value: \(String(describing: selected)) selected instead of inserted value: nil for column \(index)")
                            } else {
                                XCTFail("nil value selected instead of inserted value: \(String(describing: inserted)) for column \(index)")
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

            executeRawQuery("CREATE TABLE " +  t.tableName + " (idCol int, randomCol varchar(500))", connection: connection) { result, rows in
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
        performTest(usePool: false, asyncTasks: { connection in
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

            executeRawQueryWithParameters(rawInsert, connection: connection as! MySQLConnection, parametersArray: parametersArray) { result, rows in
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
                    for row in resultSet.rows {
                        let selectedBlob1 = row[1] as! Data
                        XCTAssertEqual(selectedBlob1, insertedBlobs[index], "Inserted Data (\(insertedBlobs[index].hexString())) is not equal to selected Data (\(selectedBlob1.hexString()))")

                        index += 1
                    }

                    XCTAssertEqual(index, parametersArray.count, "Returned row count (\(index)) != Expected row count (\(parametersArray.count))")
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
