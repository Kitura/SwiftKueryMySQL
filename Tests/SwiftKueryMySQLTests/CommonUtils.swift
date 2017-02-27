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
import SwiftKuery

func executeQuery(query: Query, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: [Any], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func executeRawQueryWithParameters(_ raw: String, connection: Connection, parameters: [Any], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw, parameters: parameters) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: [String:Any], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func executeRawQueryWithParameters(_ raw: String, connection: Connection, parameters: [String:Any], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw, parameters: parameters) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func executeRawQuery(_ raw: String, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw) { result in
        let rows = printResultAndGetRowsAsArray(result)
        callback(result, rows)
    }
}

func cleanUp(table: String, connection: Connection, callback: @escaping (QueryResult)->()) {
    connection.execute("DROP TABLE " + table) { result in
        callback(result)
    }
}

private func printResultAndGetRowsAsArray(_ result: QueryResult) -> [[Any?]]? {
    var rows: [[Any?]]? = nil
    if let resultSet = result.asResultSet {
        let titles = resultSet.titles
        for title in titles {
            print(title.padding(toLength: 11, withPad: " ", startingAt: 0), terminator: "")
        }
        print()
        rows = rowsAsArray(resultSet)
        if let rows = rows {
            for row in rows {
                for value in row {
                    if let value = value {
                        print(value, terminator: " ")
                    } else {
                        print("nil", terminator: " ")
                    }
                }
                print()
            }
        }
    }
    else if let value = result.asValue  {
        print("Result: ", value)
    }
    else if result.success  {
        print("Success")
    }
    else if let queryError = result.asError {
        print("Error in query: ", queryError)
    }
    return rows
}

func getNumberOfRows(_ result: ResultSet) -> Int {
    return result.rows.map{ $0 as [Any?] }.count
}

func rowsAsArray(_ result: ResultSet) -> [[Any?]] {
    return result.rows.map{ $0 as [Any?] }
}

// Dummy class for test framework
class CommonUtils { }
