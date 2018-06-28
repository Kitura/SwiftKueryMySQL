#!/bin/bash

set -o verbose


if [[ $TRAVIS_OS_NAME == "osx" ]]; then
    mysql --version || { brew update && brew install mysql && mysql.server start && mysql --version; }
else
    export DEBIAN_FRONTEND="noninteractive"
    { sudo service mysql stop &&  sudo apt-get update && sudo apt-get install -y mysql-server libmysqlclient-dev && sudo service mysql start && mysql --version; }
fi

mysql_upgrade -uroot || echo "No need to upgrade"
mysql -uroot -e "CREATE USER 'swift'@'localhost' IDENTIFIED BY 'kuery';"
mysql -uroot -e "CREATE DATABASE IF NOT EXISTS test;"
mysql -uroot -e "GRANT ALL ON test.* TO 'swift'@'localhost';"

git clone https://github.com/IBM-Swift/Package-Builder.git
./Package-Builder/build-package.sh -projectDir $(pwd)

