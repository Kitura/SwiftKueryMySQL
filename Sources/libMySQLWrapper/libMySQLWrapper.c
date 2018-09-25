#include "include/libMySQLWrapper.h"

#if LIBMYSQL_VERSION_ID < 80000
  mysql_bool mysql_true(){
    return 1;
  }

  mysql_bool mysql_false(){
    return 0;
  }
#else
  mysql_bool mysql_true(){
    return true;
  }

  mysql_bool mysql_false(){
    return false;
  }
#endif
