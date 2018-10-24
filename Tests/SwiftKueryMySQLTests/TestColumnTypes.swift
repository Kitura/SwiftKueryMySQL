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
import Dispatch
import SwiftKuery

import CMySQL

@testable import SwiftKueryMySQL

class TestColumnTypes: XCTestCase {

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

    func executeRecursivelyColumnTypes(statement: PreparedStatement, count index: Int, params: [[Any?]], connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        let parameters = index % 2 == 0 ? params[0] : params[1]
        connection.execute(preparedStatement: statement, parameters: parameters) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            let nextIndex = index - 1
            if nextIndex > 0 {
                self.executeRecursivelyColumnTypes(statement: statement, count: nextIndex, params: params, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    func executeRecursivelyColumnTypes(raw: String, count index: Int, params: [[Any?]], connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        let parameters = index % 2 == 0 ? params[0] : params[1]
        connection.execute(raw, parameters: parameters) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            let nextIndex = index - 1
            if nextIndex > 0 {
                self.executeRecursivelyColumnTypes(raw: raw, count: nextIndex, params: params, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    func executeRecursively(statement: PreparedStatement, count index: Int, params: [[Any?]], connection: Connection, onCompletion: @escaping ((QueryResult) -> ())) {
        let iteration = params.count - index
        let parameters = params[iteration]
        connection.execute(preparedStatement: statement, parameters: parameters) { result in
            if result.asError != nil {
                onCompletion(result)
                return
            }
            let nextIndex = index - 1
            if nextIndex > 0 {
                self.executeRecursively(statement: statement, count: nextIndex, params: params, connection: connection, onCompletion: onCompletion)
            } else {
                onCompletion(result)
            }
        }
    }

    func testColumnTypes(batchParameters: Bool) {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t.tableName, connection: connection) { _ in
                //sleep(1)
                executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (tinyintCol tinyint, smallintCol smallint, unsignedmediumintCol mediumint, intCol int, bigintCol bigint, floatCol float, doubleCol double, dateCol date, timeCol time, datetimeCol datetime, mySqlTimeCol timestamp, blobCol blob, enumCol enum('enum1', 'enum2', 'enum3'), setCol set('smallSet', 'mediumSet', 'largeSet'), nulCol int, emptyCol varchar(10), text text)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                    let rawInsert = "INSERT INTO " + t.tableName + " (tinyintCol, smallintCol, unsignedmediumintCol, intCol, bigintCol, floatCol, doubleCol, dateCol, timeCol, datetimeCol, mySqlTimeCol, blobCol, enumCol, setCol, nulCol, emptyCol, text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)"

                    var ts1 = MYSQL_TIME()
                    ts1.year = 2017
                    ts1.month = 03
                    ts1.day = 03
                    ts1.hour = 11
                    ts1.minute = 22
                    ts1.second = 59

                    var ts2 = ts1
                    ts2.year = 2018

                    let parameters1: [Any?] = [Int8.max, Int16.max, UInt16.max, Int32.max, Int64.max, Float.greatestFiniteMagnitude, Double.greatestFiniteMagnitude, "2017-01-01", "13:51:52", "2017-02-27 15:51:52", ts1, Data(repeating: 0x84, count: 96), "enum2", "mediumSet", nil, "", ""]

                    let parameters2: [Any?] = [Int8.min, Int16.min, UInt16.min, Int32.min, Int64.min, Float.leastNonzeroMagnitude, Double.leastNonzeroMagnitude, "2018-01-01", "13:41:05", "2018-02-27 15:51:05", ts2, Data(repeating: 0x72, count: 75), "enum1", "largeSet", nil, "abc", "test"]

                    let parameters: [[Any?]] = [parameters1, parameters2]

                    let parametersCount = 100

                    let start = Date.timeIntervalSinceReferenceDate
                    if batchParameters {
                        connection.prepareStatement(rawInsert) { stmt, error in
                            guard let preparedStatement = stmt else {
                                guard let error = error else {
                                    XCTFail("Error in INSERT")
                                    return
                                }
                                XCTFail("Error in INSERT: \(error.localizedDescription)")
                                return
                            }
                            self.executeRecursivelyColumnTypes(statement: preparedStatement, count: parametersCount, params: parameters, connection: connection) { result in
                                if let error = result.asError {
                                    connection.release(preparedStatement: preparedStatement) { _ in }
                                    XCTFail("Error in INSERT: \(error.localizedDescription)")
                                }
                                let end = Date.timeIntervalSinceReferenceDate
                                print("INSERT with batchParameters: \(batchParameters) took \(end-start) seconds for \(parametersCount) rows")
                                connection.release(preparedStatement: preparedStatement) { _ in
                                    let selectCount = "SELECT count(*) from " + packName(t.tableName)
                                    executeRawQuery(selectCount, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        if let rows = rows {
                                            let rowCount = rows[0][0]
                                            XCTAssertEqual(rowCount as? Int64, Int64(parametersCount), "Incorrect number of rows inserted: \(String(describing: rowCount)) (type: \(type(of: rowCount)))")
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
                                                                let selectedTime = MySQLConnection.dateTimeFormatter.string(from: selected as! Date)
                                                                let formattedTime = "\(time.year)-\(time.month.pad())-\(time.day.pad()) \(time.hour.pad()):\(time.minute.pad()):\(time.second.pad())"
                                                                XCTAssertEqual(formattedTime, selectedTime, "Column \(columnIndex+1) inserted Data (\(formattedTime)) is not equal to selected Data (\(selectedTime))")
                                                            } else if selected is Date {
                                                                let selectedTime = MySQLConnection.dateTimeFormatter.string(from: selected as! Date)
                                                                XCTAssertEqual(inserted as! String, selectedTime, "Column \(columnIndex+1) inserted Data (\(inserted)) is not equal to selected Data (\(selectedTime))")
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
                                                cleanUp(table: t.tableName, connection: connection) { _ in
                                                    semaphore.signal()
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        self.executeRecursivelyColumnTypes(raw: rawInsert, count: parametersCount, params: parameters, connection: connection) { result in
                            if let error = result.asError {
                                XCTFail("Error in INSERT: \(error.localizedDescription)")
                            }
                            let end = Date.timeIntervalSinceReferenceDate
                            print("INSERT with batchParameters: \(batchParameters) took \(end-start) seconds for \(parametersCount) rows")

                            let selectCount = "SELECT count(*) from " + packName(t.tableName)
                            executeRawQuery(selectCount, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                if let rows = rows {
                                    let rowCount = rows[0][0]
                                    XCTAssertEqual(rowCount as? Int64, Int64(parametersCount), "Incorrect number of rows inserted: \(String(describing: rowCount)) (type: \(type(of: rowCount)))")
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
                                                        let selectedTime = MySQLConnection.dateTimeFormatter.string(from: selected as! Date)
                                                        let formattedTime = "\(time.year)-\(time.month.pad())-\(time.day.pad()) \(time.hour.pad()):\(time.minute.pad()):\(time.second.pad())"
                                                        XCTAssertEqual(formattedTime, selectedTime, "Column \(columnIndex+1) inserted Data (\(formattedTime)) is not equal to selected Data (\(selectedTime))")
                                                    } else if selected is Date {
                                                        let selectedTime = MySQLConnection.dateTimeFormatter.string(from: selected as! Date)
                                                        XCTAssertEqual(inserted as! String, selectedTime, "Column \(columnIndex+1) inserted Data (\(inserted)) is not equal to selected Data (\(selectedTime))")
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
                                        cleanUp(table: t.tableName, connection: connection) { _ in
                                            semaphore.signal()
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            //sleep(5)
            expectation.fulfill()
        })
    }

    func testUnhandledParameterType() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t.tableName, connection: connection) { _ in
                //sleep(1)
                executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (idCol int, randomCol varchar(500))", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                    let rawInsert = "INSERT INTO " + packName(t.tableName) + " (idCol, randomCol) VALUES (?, ?)"

                    let unhandledParameter = URL(string: "http://www.kitura.io")!

                    executeRawQueryWithParameters(rawInsert, connection: connection, parameters: [1, unhandledParameter]) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        let rawSelect = "SELECT * from " + packName(t.tableName)
                        executeRawQuery(rawSelect, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNil(result.asError, "Error in SELECT: \(result.asError!)")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            if let rows = rows {
                                let inserted = unhandledParameter
                                let selected = rows[0][1]!
                                XCTAssertEqual(String(describing: inserted), String(describing: selected), "SELECT failed")
                            }

                            cleanUp(table: t.tableName, connection: connection) { _ in
                                semaphore.signal()
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            //sleep(5)
            expectation.fulfill()
        })
    }

    func testBlobs() {
        let t = MyTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            let semaphore = DispatchSemaphore(value: 0)

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t.tableName, connection: connection) { _ in
                //sleep(1)
                executeRawQuery("CREATE TABLE " +  packName(t.tableName) + " (idCol int, blobCol blob)", connection: connection) { result, rows in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                    let rawInsert = "INSERT INTO " + t.tableName + " (idCol, blobCol) VALUES (?, ?)"

                    let insertedBlobs = [Data(repeating: 0x84, count: 10), Data(repeating: 0x70, count: 10000), Data(repeating: 0x52, count: 1), Data(repeating: 0x40, count: 10000)]

                    let parametersArray = [[0, insertedBlobs[0]], [1, [UInt8](insertedBlobs[1])], [2, insertedBlobs[2]], [3, insertedBlobs[3]]]

                    connection.prepareStatement(rawInsert) { stmt, error in
                        guard let preparedStatement = stmt else {
                            guard let error = error else {
                                XCTFail("Error in INSERT")
                                return
                            }
                            XCTFail("Error in INSERT: \(error.localizedDescription)")
                            return
                        }

                        self.executeRecursively(statement: preparedStatement, count: parametersArray.count, params: parametersArray, connection: connection) { result in
                            if let error = result.asError {
                                connection.release(preparedStatement: preparedStatement) { _ in }
                                XCTFail("Error in INSERT: \(error.localizedDescription)")
                            }
                            connection.release(preparedStatement: preparedStatement) { _ in
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

                                    cleanUp(table: t.tableName, connection: connection) { _ in
                                        semaphore.signal()
                                    }
                                }
                            }
                        }
                    }
                }
            }
            semaphore.wait()
            //sleep(5)
            expectation.fulfill()
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
