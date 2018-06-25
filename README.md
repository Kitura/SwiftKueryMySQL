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
The latest version of SwiftKueryMySQL requires **Swift 4.0**. You can download this version of the Swift binaries by following this [link](https://swift.org/download/). Compatibility with other Swift versions is not guaranteed.

## Install MySQL

#### macOS
```
brew install mysql
mysql.server start
```

On macOS, add `-Xlinker -L/usr/local/lib` to Swift commands to point the linker to the MySQL library location.
For example,
```
swift build -Xlinker -L/usr/local/lib
swift test -Xlinker -L/usr/local/lib
swift package -Xlinker -L/usr/local/lib generate-xcodeproj
```

#### Linux
Download the release package for your Linux distribution from http://dev.mysql.com/downloads/repo/apt/
For example: `wget https://repo.mysql.com//mysql-apt-config_0.8.10-1_all.deb`
```
sudo dpkg -i mysql-apt-config_0.8.10-1_all.deb
sudo apt-get update
sudo apt-get install mysql-server libmysqlclient-dev
sudo service mysql start
```

## Usage

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


If you want to share a connection instance between multiple threads use `MySQLThreadSafeConnection` instead of `MySQLConnection`:
```swift
let connection = MySQLThreadSafeConnection(host: host, user: user, password: password, database: database,
                                 port: port, characterSet: characterSet)
```
A MySQLThreadSafeConnection instance can be used to safely execute queries on multiple threads sharing the same connection.


To connect to the server and execute a query:
```swift
connection.connect() { error in
   // if error is nil, connect() was successful
   let query = Select(from: table)
   connection.execute(query: query) { queryResult in
      if let resultSet = queryResult.asResultSet {
         for row in resultSet.rows {
            // process each row
         }
      }
   }
}
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

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/IBM-Swift/SwiftKueryMySQL/blob/master/LICENSE.txt).
