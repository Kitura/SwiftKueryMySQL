//
//  libMySQLWrapper.h
//  SwiftKueryMySQL
//
//  Created by Matthew Kilner on 16/08/2018.
//

/**
 Copyright IBM Corporation 2018

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

#ifndef libMySQLWrapper_h
#define libMySQLWrapper_h

#ifdef __linux__
    #include <stdbool.h>
#else
    #include <Mactypes.h>
#endif

#include <mysql/mysql.h>
#include <mysql/errmsg.h>

typedef struct WRAPPER_MYSQL_BIND {
    unsigned long *length; /* output length pointer */
    bool *is_null;         /* Pointer to null indicator */
    void *buffer;          /* buffer to get/put data */
    /* set this if you want to track data truncations happened during fetch */
    bool *error;
    unsigned char *row_ptr; /* for the current data position */
    void (*store_param_func)(NET *net, struct WRAPPER_MYSQL_BIND *param);
    void (*fetch_result)(struct WRAPPER_MYSQL_BIND *, MYSQL_FIELD *, unsigned char **row);
    void (*skip_result)(struct WRAPPER_MYSQL_BIND *, MYSQL_FIELD *, unsigned char **row);
    /* output buffer length, must be set when fetching str/binary */
    unsigned long buffer_length;
    unsigned long offset;              /* offset position for char/binary fetch */
    unsigned long length_value;        /* Used if length is 0 */
    unsigned int param_number;         /* For null count and error messages */
    unsigned int pack_length;          /* Internal length for packed data */
    enum enum_field_types buffer_type; /* buffer type */
    bool error_value;                  /* used if error is 0 */
    bool is_unsigned;                  /* set if integer type is unsigned */
    bool long_data_used;               /* If used with mysql_send_long_data */
    bool is_null_value;                /* Used if is_null is 0 */
    void *extension;
} WRAPPER_MYSQL_BIND;

int wrapper_mysql_library_init(int argc, char **argv, char **groups);

int wrapper_mysql_ping(MYSQL *mysql);

MYSQL *wrapper_mysql_init(MYSQL *mysql);

int wrapper_mysql_options(MYSQL *mysql, enum mysql_option option, const void *arg);

MYSQL *wrapper_mysql_real_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag);

unsigned int wrapper_mysql_errno(MYSQL *mysql);

int wrapper_mysql_set_character_set(MYSQL *mysql, const char *csname);

const char *wrapper_mysql_character_set_name(MYSQL *mysql);

void wrapper_mysql_thread_end(void);

void wrapper_mysql_close(MYSQL *mysql);

int wrapper_mysql_query(MYSQL *mysql, const char *stmt_str);

const char *wrapper_mysql_error(MYSQL *mysql);

unsigned int wrapper_mysql_stmt_errno(MYSQL_STMT *stmt);

const char *wrapper_mysql_stmt_error(MYSQL_STMT *stmt);

MYSQL_STMT *wrapper_mysql_stmt_init(MYSQL *mysql);

int wrapper_mysql_stmt_prepare(MYSQL_STMT *stmt, const char *stmt_str, unsigned long length);

bool wrapper_mysql_stmt_close(MYSQL_STMT *stmt);

MYSQL_RES *wrapper_mysql_stmt_result_metadata(MYSQL_STMT *stmt);

int wrapper_mysql_stmt_execute(MYSQL_STMT *stmt);

my_ulonglong wrapper_mysql_stmt_affected_rows(MYSQL_STMT *stmt);

void wrapper_mysql_free_result(MYSQL_RES *result);

char **wrapper_mysql_stmt_bind_param(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *bind, int bindCount);

MYSQL_FIELD *wrapper_mysql_fetch_fields(MYSQL_RES *result);

unsigned int wrapper_mysql_num_fields(MYSQL_RES *result);

bool wrapper_mysql_stmt_bind_result(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *bind, int bindCount);

int wrapper_mysql_stmt_fetch(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *binds, int bindCount);

void wrapper_release_statement_binds(MYSQL_STMT *stmt, int bindCount);

void wrapper_release_params(char **allocatedParams, int bindCount);

// Conversion functions

void *convertFromBool(MYSQL *mysql, bool *value);

bool convertToBool(MYSQL *mysql, void *value);

void convert_to_MYSQL_BIND(MYSQL *mysql, WRAPPER_MYSQL_BIND *wrapper, MYSQL_BIND *newBind, char **allocPointers, int paramIndex);

void convert_from_MYSQL_BIND(MYSQL *mysql,MYSQL_BIND *bind, WRAPPER_MYSQL_BIND *wrapper);

#endif /* libMySQLWrapper_h */
