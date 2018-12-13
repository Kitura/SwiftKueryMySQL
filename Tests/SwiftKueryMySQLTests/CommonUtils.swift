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
import Foundation
import SwiftKuery
import SwiftKueryMySQL

func read(fileName: String) -> String {
    // Read in a configuration file into an NSData
    do {
        var pathToTests = #file
        if pathToTests.hasSuffix("CommonUtils.swift") {
            pathToTests = pathToTests.replacingOccurrences(of: "CommonUtils.swift", with: "")
        }
        let fileData = try Data(contentsOf: URL(fileURLWithPath: "\(pathToTests)\(fileName)"))
        XCTAssertNotNil(fileData, "Failed to read in the \(fileName) file")

        let resultString = String(data: fileData, encoding: String.Encoding.utf8)

        guard
            let resultLiteral = resultString
            else {
                XCTFail("Error in \(fileName).")
                exit(1)
        }
        return resultLiteral.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
    } catch {
        XCTFail("Error in \(fileName).")
        exit(1)
    }
}

func executeQuery(query: Query, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: [Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeQueryWithNamedParameters(query: Query, connection: Connection, parameters: [String:Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeRawQueryWithParameters(_ raw: String, connection: Connection, parameters: [Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeQueryWithParameters(query: Query, connection: Connection, parameters: [String:Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    do {
        try print("=======\(connection.descriptionOf(query: query))=======")
    }
    catch {}
    connection.execute(query: query, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeRawQueryWithParameters(_ raw: String, connection: Connection, parameters: [String:Any?], callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw, parameters: parameters) { result in
        printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func executeRawQuery(_ raw: String, connection: Connection, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    print("=======\(raw)=======")
    connection.execute(raw) { result in
       printResultAndGetRowsAsArray(result, callback: callback)
    }
}

func cleanUp(table: String, connection: Connection, callback: @escaping (QueryResult)->()) {
    connection.execute("DROP TABLE " + packName(table)) { result in
        callback(result)
    }
}

private func printResultAndGetRowsAsArray(_ result: QueryResult, callback: @escaping (QueryResult, [[Any?]]?)->()) {
    var rows: [[Any?]] = [[Any?]]()
    if let resultSet = result.asResultSet {
        resultSet.getColumnTitles() { titles, error in
            guard let titles = titles else {
                return callback(result, nil)
            }
            for title in titles {
                print(title.padding(toLength: 11, withPad: " ", startingAt: 0), terminator: "")
            }
            print()
            resultSet.forEach() { row, error in
                guard let row = row else {
                    // No more rows
                    return callback(result, rows)
                }
                for value in row {
                    if let value = value {
                        print(value, terminator: " ")
                    } else {
                        print("nil", terminator: " ")
                    }
                }
                print()
                rows.append(row)
            }
        }
    } else if let value = result.asValue  {
        print("Result: ", value)
        callback(result, nil)
    } else if result.success  {
        print("Success")
        callback(result, nil)
    } else if let queryError = result.asError {
        print("Error in query: ", queryError)
        callback(result, nil)
    }
}

func packName(_ name: String) -> String {
    var result = name
    let identifierQuoteCharacter = "`"
    if !result.hasPrefix(identifierQuoteCharacter) {
        result = identifierQuoteCharacter + result + identifierQuoteCharacter
    }
    return result
}

class CommonUtils {
    private var pool: ConnectionPool?
    static let sharedInstance = CommonUtils()
    private init() {}

    func getConnectionPool(characterSet: String? = nil) -> ConnectionPool {
        if let pool = pool {
            return pool
        }
        do {
            let connectionFile = #file.replacingOccurrences(of: "CommonUtils.swift", with: "connection.json")
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

                let poolOptions = ConnectionPoolOptions(initialCapacity: 1, maxCapacity: 1, timeout: 10000)

                if characterSet != nil || randomBinary == 0 {
                    pool = MySQLConnection.createPool(host: host, user: username, password: password, database: database, port: port, characterSet: characterSet, poolOptions: poolOptions)
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
                        pool = MySQLConnection.createPool(url: url, poolOptions: poolOptions)
                    } else {
                        pool = nil
                        XCTFail("Invalid URL format: \(urlString)")
                    }
                }
            } else {
                pool = nil
                XCTFail("Invalid format for connection.json contents: \(json)")
            }
        } catch {
            print("caught throw")
            pool = nil
            XCTFail(error.localizedDescription)
        }
        return pool!
    }
}
