#ifndef MYDB_H
#define MYDB_H

#include <iostream>
#include <sstream>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { 
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG, 
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
    char username[COLUMN_USERNAME_SIZE+1];
    char email[COLUMN_EMAIL_SIZE+1];
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
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;    // page的数量
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager* pager;
    uint32_t root_page_num;
} Table;

typedef enum { 
    EXECUTE_SUCCESS, 
    EXECUTE_TABLE_FULL,
    EXECUTE_UNKNOWN_COMMAND 
} ExecuteResult;

typedef struct {
  Table* table;
  uint32_t page_num;
  uint32_t cell_num;
  bool end_of_table;  // Indicates a position one past the last element
} Cursor;

typedef enum { 
    NODE_INTERNAL, 
    NODE_LEAF 
} NodeType;

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;


/*
 * Leaf Node Header Layout
 */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


/*
 * Leaf Node Body Layout
 */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;

void print_prompt();
PrepareResult prepare_statement(std::string input_buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, Table* table);
MetaCommandResult do_meta_command(std::string input_buffer, Table* table);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* cursor_value(Cursor* cursor);
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
void print_row(Row* row);
Pager* pager_open(const char* filename);
Table* db_open(const char* filename);
void* get_page(Pager* pager, uint32_t page_num);
void db_close(Table* table);
void pager_flush(Pager* pager, uint32_t page_num);
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void cursor_advance(Cursor* cursor);
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);
void print_constants();
void print_leaf_node(void* node);
uint32_t* leaf_node_num_cells(void* node);
void* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
void initialize_leaf_node(void* node);
void print_page(Pager* pager, uint32_t page_num);

#endif