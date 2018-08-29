//
//  libMySQLWrapper.c
//  SwiftKuery
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

#include "include/libMySQLWrapper.h"

int wrapper_mysql_library_init(int argc, char **argv, char **groups)
{
    return mysql_library_init(0, NULL, NULL);
}

int wrapper_mysql_ping(MYSQL *mysql)
{
    return mysql_ping(mysql);
}

MYSQL *wrapper_mysql_init(MYSQL *mysql)
{
    return mysql_init(mysql);
}

int wrapper_mysql_options(MYSQL *mysql, enum mysql_option option, const void *arg)
{
    return mysql_options(mysql, option, arg);
}

MYSQL *wrapper_mysql_real_connect(MYSQL *mysql, const char *host, const char *user, const char *passwd, const char *db, unsigned int port, const char *unix_socket, unsigned long client_flag)
{
    return mysql_real_connect(mysql, host, user, passwd, db, port, unix_socket, client_flag);
}

unsigned int wrapper_mysql_errno(MYSQL *mysql)
{
    return mysql_errno(mysql);
}

int wrapper_mysql_set_character_set(MYSQL *mysql, const char *csname)
{
    return mysql_set_character_set(mysql, csname);
}

const char *wrapper_mysql_character_set_name(MYSQL *mysql)
{
    return mysql_character_set_name(mysql);
}

void wrapper_mysql_thread_end(void)
{
    return mysql_thread_end();
}

void wrapper_mysql_close(MYSQL *mysql)
{
    return mysql_close(mysql);
}

int wrapper_mysql_query(MYSQL *mysql, const char *stmt_str)
{
    return mysql_query(mysql, stmt_str);
}

const char *wrapper_mysql_error(MYSQL *mysql)
{
    return mysql_error(mysql);
}

unsigned int wrapper_mysql_stmt_errno(MYSQL_STMT *stmt)
{
    return mysql_stmt_errno(stmt);
}

const char *wrapper_mysql_stmt_error(MYSQL_STMT *stmt){
    return mysql_stmt_error(stmt);
}

MYSQL_STMT *wrapper_mysql_stmt_init(MYSQL *mysql)
{
    return mysql_stmt_init(mysql);
}

int wrapper_mysql_stmt_prepare(MYSQL_STMT *stmt, const char *stmt_str, unsigned long length)
{
    return mysql_stmt_prepare(stmt, stmt_str, length);
}

bool wrapper_mysql_stmt_close(MYSQL_STMT *stmt)
{
    return mysql_stmt_close(stmt);
}

MYSQL_RES *wrapper_mysql_stmt_result_metadata(MYSQL_STMT *stmt)
{
    return mysql_stmt_result_metadata(stmt);
}

int wrapper_mysql_stmt_execute(MYSQL_STMT *stmt)
{
    return mysql_stmt_execute(stmt);
}

my_ulonglong wrapper_mysql_stmt_affected_rows(MYSQL_STMT *stmt)
{
    return mysql_stmt_affected_rows(stmt);
}

void wrapper_mysql_free_result(MYSQL_RES *result)
{
    return mysql_free_result(result);
}

char **wrapper_mysql_stmt_bind_param(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *bind, int bindCount)
{
    //Keep a record of allocations so we can free them.
    int allocCount = bindCount * 2;
    char **allocPointers = calloc(allocCount, sizeof(char *));
    // Again this is a pointer to an array so array conversion required.
    MYSQL *mysql = stmt->mysql;
    MYSQL_BIND *newBind = calloc(bindCount, sizeof(MYSQL_BIND));
    MYSQL_BIND *newOffset = newBind;
    WRAPPER_MYSQL_BIND *wrapperOffset = bind;
    for (int index = 0; index < bindCount; index++) {
        //Convert bind
        convert_to_MYSQL_BIND(mysql, wrapperOffset, newOffset, allocPointers, index);
        //Increment offset pointers
        newOffset++;
        wrapperOffset++;
    }
    bool result = mysql_stmt_bind_param(stmt, newBind);
    newOffset = newBind;
    wrapperOffset = bind;
    for (int index = 0; index < bindCount; index++) {
        //Convert bind
        convert_from_MYSQL_BIND(mysql, newOffset, wrapperOffset);
        //Increment offset pointers
        newOffset++;
        wrapperOffset++;
    }
    free(newBind);
    return allocPointers;
}

MYSQL_FIELD *wrapper_mysql_fetch_fields(MYSQL_RES *result)
{
    return mysql_fetch_fields(result);
}

unsigned int wrapper_mysql_num_fields(MYSQL_RES *result)
{
    return mysql_num_fields(result);
}

bool wrapper_mysql_stmt_bind_result(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *bind, int bindCount)
{
    // *bind is a pointer to an array of binds so we need to do an array conversion
    MYSQL *mysql = stmt->mysql;
    MYSQL_BIND *newBind = calloc(bindCount, sizeof(MYSQL_BIND));
    MYSQL_BIND *newOffset = newBind;
    WRAPPER_MYSQL_BIND *wrapperOffset = bind;
    for (int index = 0; index < bindCount; index++) {
        //Convert bind
        convert_to_MYSQL_BIND(mysql, wrapperOffset, newOffset, NULL, 0);
        //Increment offset pointers
        newOffset++;
        wrapperOffset++;
    }
    bool result = mysql_stmt_bind_result(stmt, newBind);
    newOffset = newBind;
    wrapperOffset = bind;
    for (int index = 0; index < bindCount; index++) {
        //Convert bind
        convert_from_MYSQL_BIND(mysql, newOffset, wrapperOffset);
        //Increment offset pointers
        newOffset++;
        wrapperOffset++;
    }
    free(newBind);
    return result;
}

int wrapper_mysql_stmt_fetch(MYSQL_STMT *stmt, WRAPPER_MYSQL_BIND *binds, int bindCount)
{
    // For pre 8.0 mysql releases we need to fixup the passed binds with the values from the statement binds before returning
    int result = mysql_stmt_fetch(stmt);
    MYSQL_BIND *dbBinds = stmt->bind;
    MYSQL_BIND *dbBindsOffset = dbBinds;
    WRAPPER_MYSQL_BIND *wrapperOffset = binds;
    for (int index = 0; index < bindCount; index++) {
        //Convert bind
        convert_from_MYSQL_BIND(stmt->mysql, dbBindsOffset, wrapperOffset);
        //Increment offset pointers
        dbBindsOffset++;
        wrapperOffset++;
    }
    return result;
}

// Conversion functions

// This function converts from bool to my_bool
void *convertFromBool(MYSQL *mysql, bool *value)
{
    char *val = NULL;
    if (value == NULL) {
        return (void *)val;
    }
    val = malloc(sizeof(char));
    int version = mysql_get_server_version(mysql);
    if (version < 80000) {
        if (*value == false) {
            *val = 0;
        } else {
            *val = 1;
        }
        return (void *)val;
    }
    return (void *)value;
}

bool convertToBool(MYSQL *mysql, void *value)
{
    bool result;
    int version = mysql_get_server_version(mysql);
    if (version < 80000) {
        if (*(char *)value == 1) {
            return true;
        } else {
            return false;
        }
    } else {
        if (*(bool *)value == true) {
            return true;
        } else {
            return false;
        }
    }
}

void convert_to_MYSQL_BIND(MYSQL *mysql, WRAPPER_MYSQL_BIND *wrapper, MYSQL_BIND *newBind, char **allocPointers, int paramIndex)
{
    int version = mysql_get_server_version(mysql);
    if (version < 80000 && allocPointers != NULL) {
        newBind->is_null = convertFromBool(mysql, wrapper->is_null);
        allocPointers[paramIndex*2] = newBind->is_null;
        newBind->error = convertFromBool(mysql, wrapper->error);
        allocPointers[(paramIndex*2)+1] = newBind->error;
    } else {
        newBind->is_null = convertFromBool(mysql, wrapper->is_null);
        newBind->error = convertFromBool(mysql, wrapper->error);
    }
    newBind->length = wrapper->length;
    newBind->buffer = wrapper->buffer;
    newBind->row_ptr = wrapper->row_ptr;
    newBind->store_param_func = NULL;
    newBind->fetch_result = NULL;
    newBind->skip_result = NULL;
    newBind->buffer_length = wrapper->buffer_length;
    newBind->offset = wrapper->offset;
    newBind->length_value = wrapper->length_value;
    newBind->param_number = wrapper->param_number;
    newBind->pack_length = wrapper->pack_length;
    newBind->buffer_type = wrapper->buffer_type;
    newBind->error_value = wrapper->error_value;
    newBind->is_unsigned = wrapper->is_unsigned;
    newBind->long_data_used = wrapper->long_data_used;
    newBind->is_null_value = wrapper->is_null_value;
    newBind->extension = wrapper->extension;
}

void convert_from_MYSQL_BIND(MYSQL *mysql, MYSQL_BIND *bind, WRAPPER_MYSQL_BIND *wrapper)
{
    wrapper->length = bind->length;
    if (wrapper->is_null != NULL) {
        *(wrapper->is_null) = convertToBool(mysql, bind->is_null);
    }
    wrapper->buffer = bind->buffer;
    if (wrapper->error != NULL) {
        *(wrapper->error) = convertToBool(mysql, bind->error);
    }
    wrapper->row_ptr = bind->row_ptr;
    wrapper->buffer_length = bind->buffer_length;
    wrapper->offset = bind->offset;
    wrapper->length_value = bind->length_value;
    wrapper->param_number = bind->param_number;
    wrapper->pack_length = bind->pack_length;
    wrapper->buffer_type = bind->buffer_type;
    wrapper->error_value = bind->error_value;
    wrapper->is_unsigned = bind->is_unsigned;
    wrapper->long_data_used = bind->long_data_used;
    wrapper->is_null_value = bind->is_null_value;
    wrapper->extension = bind->extension;
}

void wrapper_release_statement_binds(MYSQL_STMT *stmt, int bindCount)
{
    int version = mysql_get_server_version(stmt->mysql);
    if (version < 80000) {
        MYSQL_BIND *bindPtr = stmt->bind;
        MYSQL_BIND *bindOffset = bindPtr;
        for (int index = 0; index < bindCount; index++) {
            if (bindOffset->is_null != NULL) {
                free(bindOffset->is_null);
            }
            if (bindOffset->error != NULL) {
                free(bindOffset->error);
            }
            bindOffset++;
        }
    }
}

void wrapper_release_params(char **allocatedParams, int bindCount)
{
    int paramCount = bindCount * 2;
    for (int index = 0; index < paramCount; index++){
        free(allocatedParams[index]);
    }
}


