# SwiftKueryMySQL
MySQL plugin for Swift-Kuery framework

<!-- 
[![Build Status - Master](https://travis-ci.org/IBM-Swift/SwiftKueryMySQL.svg?branch=master)](https://travis-ci.org/IBM-Swift/SwiftKueryMySQL)
-->
![macOS](https://img.shields.io/badge/os-Mac%20OS%20X-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[MySQL](https://dev.mysql.com/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in a MySQL database.

### Install MySQL

#### macOS
```
brew install mysql
```
Other install options: https://dev.mysql.com/doc/refman/5.7/en/osx-installation.html

On macOS, add `-Xlinker -L/usr/local/lib` to swift commands to point the linker to the MySQL library location.
For example,
```
swift package generate-xcodeproj -Xlinker -L/usr/local/lib
swift build -Xlinker -L/usr/local/lib
swift test -Xlinker -L/usr/local/lib
```

#### Linux
Download the release package for your Linux distribution from http://dev.mysql.com/downloads/repo/apt/
```
sudo dpkg -i mysql-apt-config_w.x.y-z_all.deb
sudo apt-get update
sudo apt-get install mysql-server
```
More details: https://dev.mysql.com/doc/refman/5.7/en/linux-installation.html

## Using Swift-Kuery-MySQL

First create an instance of `MySQLConnection` by calling:

```swift
let connection = MySQLConnection(host: host, user: user, password: password, database: database, port: port)
```
**Where:**
- *host* and *port* are the host and the port of the MySQL server
- *user* - the user name
- *password* - the user password
- *database* - the database name

<br>
Alternatively, call:
```swift
let connection = MySQLConnection(url: URL(string: "mysql://\(user):\(password)@\(host):\(port)/\(database)")!))
```

To establish a connection call:

```swift
MySQLConnection.connect(onCompletion: (QueryError?) -> ())
```

If you want to share a connection instance between multiple threads use `MySQLThreadSafeConnection` instead of `MySQLConnection`

You now have a connection that can be used to execute SQL queries created using Swift-Kuery. View the [Kuery](https://github.com/IBM-Swift/Swift-Kuery) documentation for more information.

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
