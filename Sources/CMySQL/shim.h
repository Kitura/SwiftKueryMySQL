#ifndef __CMYSQL_SHIM_H__
#define __CMYSQL_SHIM_H__
#include <mysql.h>
#include <errmsg.h>

#ifdef __linux__
    #include <stdbool.h>
    #include <stdio.h>
#else
    #include <Mactypes.h>
#endif

#include <mysql.h>
#include <errmsg.h>

#if LIBMYSQL_VERSION_ID < 80000

  typedef my_bool mysql_bool;

  static inline mysql_bool mysql_true(){
    return 1;
  }

  static inline mysql_bool mysql_false(){
    return 0;
  }

#else

  typedef bool mysql_bool;

  static inline mysql_bool mysql_true(){
    return true;
  }

  static inline mysql_bool mysql_false(){
    return false;
  }

#endif

#endif
