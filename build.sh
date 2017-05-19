#!/bin/bash

set -o verbose

if [ -n "${DOCKER_IMAGE}" ]; then
    docker pull ${DOCKER_IMAGE}
    docker run --env SWIFT_VERSION -v ${TRAVIS_BUILD_DIR}:${TRAVIS_BUILD_DIR} ${DOCKER_IMAGE} /bin/bash -c "apt-get update && apt-get install -y git sudo lsb-release wget libxml2 && cd $TRAVIS_BUILD_DIR && ./build.sh"
else
    if [[ $TRAVIS_OS_NAME == "osx" ]]; then
        mysql --version || { brew update && brew install mysql && mysql.server start && mysql --version; }
    else
        mysql --version || { apt-get update && apt-get install mysql-server && service mysql start && mysql --version; }
    fi

    mysql_upgrade -uroot || echo "No need to upgrade"
    mysql -uroot -e "CREATE USER 'swift'@'localhost' IDENTIFIED BY 'kuery';"
    mysql -uroot -e "CREATE DATABASE IF NOT EXISTS test;"
    mysql -uroot -e "GRANT ALL ON test.* TO 'swift'@'localhost';"

    test -n "${SWIFT_VERSION}" && echo "${SWIFT_VERSION}" > .swift-version || echo "SWIFT_VERSION not set, using $(cat .swift-version)"
    git clone https://github.com/IBM-Swift/Package-Builder.git
    ./Package-Builder/build-package.sh -projectDir $(pwd)
fi
