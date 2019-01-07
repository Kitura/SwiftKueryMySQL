<p align="center">
    <a href="http://kitura.io/">
        <img src="https://raw.githubusercontent.com/IBM-Swift/Kitura/master/Sources/Kitura/resources/kitura-bird.svg?sanitize=true" height="100" alt="Kitura">
    </a>
</p>


<p align="center">
    <a href="https://ibm-swift.github.io/SwiftKueryMySQL/index.html">
    <img src="https://img.shields.io/badge/apidoc-SwiftKueryMySQL-1FBCE4.svg?style=flat" alt="APIDoc">
    </a>
    <a href="https://travis-ci.org/IBM-Swift/SwiftKueryMySQL">
    <img src="https://travis-ci.org/IBM-Swift/SwiftKueryMySQL.svg?branch=master" alt="Build Status - Master">
    </a>
    <img src="https://img.shields.io/badge/os-macOS-green.svg?style=flat" alt="macOS">
    <img src="https://img.shields.io/badge/os-linux-green.svg?style=flat" alt="Linux">
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
    <a href="http://swift-at-ibm-slack.mybluemix.net/">
    <img src="http://swift-at-ibm-slack.mybluemix.net/badge.svg" alt="Slack Status">
    </a>
</p>

# SwiftKueryMySQL

[MySQL](https://dev.mysql.com/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in a MySQL database.

## Swift version
The latest version of SwiftKueryMySQL requires **Swift 4.0 or newer**. You can download this version of the Swift binaries by following this [link](https://swift.org/download/). Compatibility with other Swift versions is not guaranteed.

## Install MySQL

#### macOS
```
brew install mysql
mysql.server start
```

#### Linux
```
sudo apt-get update
sudo apt-get install mysql-server libmysqlclient-dev pkg-config
sudo service mysql start
```

## Usage

On macOS, regular swift commands can be used for build and test. Use the example command below for generating an Xcode project.

For example,
```
swift build
swift test
swift package generate-xcodeproj --xcconfig-overrides Config.xcconfig
```
On linux standard swift commands will also work provided your mysql installation is version 5.7 or greater. If using an earlier version of mysql add ` -Xcc -I/usr/include/mysql` to swift commands to point the compiler at the mysql header files:

For example,
```
swift build -Xcc -I/usr/include/mysql/
swift test -Xcc -I/usr/include/mysql/
```

#### Add dependencies

Add the `SwiftKueryMySQL` package to the dependencies within your application’s `Package.swift` file. Substitute `"x.x.x"` with the latest `SwiftKueryMySQL` [release](https://github.com/IBM-Swift/SwiftKueryMySQL/releases).

```swift
.package(url: "https://github.com/IBM-Swift/SwiftKueryMySQL.git", from: "x.x.x")
```

Add `SwiftKueryMySQL` to your target's dependencies:

```swift
.target(name: "example", dependencies: ["SwiftKueryMySQL"]),
```

#### Import package

  ```swift
  import SwiftKueryMySQL
  ```

## Using SwiftKueryMySQL

Create an instance of `MySQLConnection` by calling:

```swift
let connection = MySQLConnection(host: host, user: user, password: password, database: database,
                                 port: port, characterSet: characterSet)
```
**Where:**
- *host* - hostname or IP of the MySQL server, defaults to localhost
- *user* - the user name, defaults to current user
- *password* - the user password, defaults to no password
- *database* - default database to use, if specified
- *port* - port number for the TCP/IP connection if connecting to server on a non-standard port (i.e. not 3306)
- *characterSet* - MySQL character set to use for the connection

All the connection parameters are optional, so if you were using a standard local MySQL server as the current user, you could simply use:
```swift
let connection = MySQLConnection(password: password)
```
*password* is also optional, but recommended.

Alternatively, call:
```swift
let connection = MySQLConnection(url: URL(string: "mysql://\(user):\(password)@\(host):\(port)/\(database)")!))
```
You now have a connection that can be used to execute SQL queries created using Swift-Kuery.

To connect to the server and execute a query:
```swift
connection.connect() { result in
    guard result.success else {
        // Connection unsuccessful
        return
    }
    // Connection succesful
    // Use connection
    connection.execute(query: query) { queryResult in
      guard queryResult.success else {
          // Check for Error and handle
          return
      }
      // Process queryResult
   }
}
```

MySQLConnections should not be used to execute concurrent operations and therefore should not be shared across threads without proper synchronisation in place. It is recommended to use a connection pool if you wish to share connections between multiple threads as the connection pool will ensure your connection is not used concurrently.  

The example below creates a `ConnectionPool` containing a single connection and uses it to perform an insert on multiple threads:

```swift
var connectionPoolOptions = ConnectionPoolOptions.init(initialCapacity: 1, maxCapacity: 1)
let connectionPool = MySQLConnection.createPool(host: host, user: user, password: password, database: database, port: port, characterSet: nil, connectionTimeout: 10000, poolOptions: connectionPoolOptions)
.......
let insertQuery = Insert(into: infos, values: "firstname", "surname", Parameter())
let insertGroup = DispatchGroup()
for age in 0 ... 5 {
    insertGroup.enter()
    connectionPool.getConnection() { connection, error in
        guard let connection = connection else {
            // Error Handling and return
        }
        connection.execute(query: insertQuery, parameters: [age]) { result in
            guard result.success else {
                // Error handling and return
            }
            print("Successfully inserted age: \(age)")
            return insertGroup.leave()
        }
    }
}
insertGroup.wait()
```
When executing this example code you see output similar to:
```
Successfully inserted age: 0
Successfully inserted age: 1
Successfully inserted age: 2
Successfully inserted age: 3
Successfully inserted age: 4
Successfully inserted age: 5
```
This is because the single connection pool only allows a single thread at a time to obtain the connection. If you edit the maximum capacity of the thread pool the ordering of inserts is more random as they occur concurrently on different connections:
```
Successfully inserted age: 0
Successfully inserted age: 1
Successfully inserted age: 3
Successfully inserted age: 2
Successfully inserted age: 5
Successfully inserted age: 4
```

View the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) documentation for detailed information on using the Swift-Kuery framework.


## For testing purposes - MySQL test setup

To run `swift test` to validate your MySQL installation, you must first run the following commands to set up your MySQL:
```
mysql_upgrade -uroot || echo "No need to upgrade"
mysql -uroot -e "CREATE USER 'swift'@'localhost' IDENTIFIED BY 'kuery';"
mysql -uroot -e "CREATE DATABASE IF NOT EXISTS test;"
mysql -uroot -e "GRANT ALL ON test.* TO 'swift'@'localhost';"
```

## API Documentation
For more information visit our [API reference](https://ibm-swift.github.io/SwiftKueryMySQL/index.html).

## Community

We love to talk server-side Swift, and Kitura. Join our [Slack](http://swift-at-ibm-slack.mybluemix.net/) to meet the team!

## Deployment via Cloud Foundry

If you include SwiftKueryMySQL as a dependancy in an application you are deploying using the Cloud Foundry Swift buildpack then you will need to specify some additional flags when compiling your application. This is best achieved by adding a file to the root of your application named .swift-build-linux-options with the content:

```
$ cat .swift-build-options-linux 
-Xcc -I/usr/include/mysql/
```
These flags tell the compiler where to find the MySQL header files required to build the CMySQL library.

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/IBM-Swift/SwiftKueryMySQL/blob/master/LICENSE.txt).
