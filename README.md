# SwiftKueryMySQL
MySQL plugin for Swift-Kuery framework

[![Build Status - Master](https://travis-ci.org/IBM-Swift/SwiftKueryMySQL.svg?branch=master)](https://travis-ci.org/IBM-Swift/SwiftKueryMySQL)
![macOS](https://img.shields.io/badge/os-Mac%20OS%20X-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[MySQL](https://dev.mysql.com/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in a MySQL database.

### Install MySQL

#### macOS
```
brew install mysql
mysql.server start
```
Other install options: https://dev.mysql.com/doc/refman/5.7/en/osx-installation.html

On macOS, add `-Xlinker -L/usr/local/lib` to swift commands to point the linker to the MySQL library location.
For example,
```
swift build -Xlinker -L/usr/local/lib
swift test -Xlinker -L/usr/local/lib
swift package -Xlinker -L/usr/local/lib generate-xcodeproj
```

#### Linux
Download the release package for your Linux distribution from http://dev.mysql.com/downloads/repo/apt/
For example: `wget https://repo.mysql.com//mysql-apt-config_0.8.4-1_all.deb`
```
sudo dpkg -i mysql-apt-config_0.8.4-1_all.deb
sudo apt-get update
sudo apt-get install mysql-server libmysqlclient-dev
sudo service mysql start
```
More details: https://dev.mysql.com/doc/refman/5.7/en/linux-installation.html

On linux, regular swift commands should work fine:
```
swift build
swift test
swift package generate-xcodeproj
```

## Using Swift-Kuery-MySQL

First create an instance of `MySQLConnection` by calling:

```swift
let connection = MySQLConnection(host: host, user: user, password: password, database: database, 
                                 port: port, characterSet: characterSet)
```
**Where:**
- *host* - hostname or IP of the MySQL server, defaults to localhost 
- *user* - the user name, defaults to current user
- *password* - the user password, defaults to no password
- *database* - default database to use if specified
- *port* - port number for the TCP/IP connection if connecting to server on a non-standard port (not 3306)
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

View the [Kuery](https://github.com/IBM-Swift/Swift-Kuery) documentation for detailed information on using the Kuery framework.

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
