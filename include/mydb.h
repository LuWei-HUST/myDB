#ifndef MYDB_H
#define MYDB_H

#include <iostream>
#include <sstream>
#include <cstdint>
#include <cstring>

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { 
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR, 
    PREPARE_UNRECOGNIZED_STATEMENT 
} PrepareResult;

typedef enum { 
    STATEMENT_INSERT, 
    STATEMENT_SELECT 
} StatementType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

// (Struct*)0：将 0 转换为指向 Struct 类型的指针
// ((Struct*)0)->Attribute：访问这个"假指针"指向的成员
// sizeof(...)：计算该成员的大小
// 这个操作在编译时完成，不产生运行时开销
// 因为只是计算类型大小，不需要实际的内存访问
// 编译器知道成员的类型，所以能直接计算大小
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

#define TABLE_MAX_PAGES 100
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct {
    uint32_t num_rows;
    void* pages[TABLE_MAX_PAGES];
} Table;

typedef enum { 
    EXECUTE_SUCCESS, 
    EXECUTE_TABLE_FULL 
} ExecuteResult;


void print_prompt();
PrepareResult prepare_statement(std::string input_buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, Table* table);
MetaCommandResult do_meta_command(std::string input_buffer);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* row_slot(Table* table, uint32_t row_num);
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
void print_row(Row* row);
Table* new_table();
void free_table(Table* table);

#endif