#ifndef libMySQLWrapper_h
#define libMySQLWrapper_h

#ifdef __linux__
    #include <stdbool.h>
    #include <stdio.h>
#else
    #include <Mactypes.h>
#endif

#include <mysql/mysql.h>
#include <mysql/errmsg.h>

#if LIBMYSQL_VERSION_ID < 80000
  typedef my_bool mysql_bool;
#else
  typedef bool mysql_bool;
#endif

mysql_bool mysql_true();
mysql_bool mysql_false();

#endif /* libMySQLWrapper_h */
