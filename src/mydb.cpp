#include "mydb.h"

using namespace std;

void print_prompt() { 
    cout << "db > "; 
}

PrepareResult prepare_statement(string input_buffer, Statement* statement) {
    if (input_buffer.substr(0, 6) == "insert") {
        statement->type = STATEMENT_INSERT;
        istringstream iss(input_buffer);
        string command;
        string username;
        string email;

        if (!(iss >> command >> statement->row_to_insert.id >> username >> email)) {
            return PREPARE_SYNTAX_ERROR;
        }

        if (command != "insert") {
            return PREPARE_SYNTAX_ERROR;
        }

        if (username.length() > COLUMN_USERNAME_SIZE || email.length() > COLUMN_EMAIL_SIZE) {
            return PREPARE_STRING_TOO_LONG;
        }

        strncpy(statement->row_to_insert.username, username.c_str(), COLUMN_USERNAME_SIZE);
        statement->row_to_insert.username[username.length()] = '\0';
        strncpy(statement->row_to_insert.email, email.c_str(), COLUMN_EMAIL_SIZE);
        statement->row_to_insert.email[email.length()] = '\0';

        return PREPARE_SUCCESS;
    }
    if (input_buffer == "select") {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
        default:
            // 不应该到达这里
            return EXECUTE_UNKNOWN_COMMAND;
    }
}

MetaCommandResult do_meta_command(string input_buffer, Table* table) {
    if (input_buffer == ".exit") {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (input_buffer == ".btree") {
        cout << "Tree:\n";
        print_leaf_node(get_page(table->pager, 0));
        // print_page(table->pager, 0);
        return META_COMMAND_SUCCESS;
    } else if (input_buffer == ".constants") {
        cout << "Constants:\n";
        print_constants();
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}


void serialize_row(Row* source, void* destination) {
    char* dest = static_cast<char*>(destination);

    memcpy(dest + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(dest + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(dest + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    char* src = static_cast<char*>(source);

    memcpy(&(destination->id), src + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), src + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), src + EMAIL_OFFSET, EMAIL_SIZE);
}


void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        cout << "Tried to fetch page number out of bounds. " << page_num
             << " > " << TABLE_MAX_PAGES << endl;
        exit(EXIT_FAILURE);
    }
    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        // We might save a partial page at the end of the file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                cout << "Error reading file: " << errno << endl;
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        cursor->end_of_table = true;
    }
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    void* node = get_page(table->pager, table->root_page_num);
    if ((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)) {
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);
    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);
    Row row;
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

void print_row(Row* row) {
    cout << "(" << row->id << ", " << row->username << ", " << row->email << ")" << endl;
}

void print_page(Pager* pager, uint32_t page_num) {
    cout << "Page:\n";
    void* p = get_page(pager, page_num);
    uint32_t* num_cells_ptr = leaf_node_num_cells(p);
    uint32_t num_cells = *num_cells_ptr;
    cout << "num_cells: " << num_cells << endl;
    for (int i=0;i<num_cells;i++) {
        uint32_t* k_ptr = leaf_node_key(p, i);
        uint32_t k = *k_ptr;
        cout << "cell: " << i << ", key: " << k << endl;
    }
    Row row;
    for (int i=0;i<num_cells;i++) {
        deserialize_row(leaf_node_value(p, i), &row);
        print_row(&row);
    }
}

Pager* pager_open(const char* filename) {
    int fd = open(filename,
                    O_RDWR |      // Read/Write mode
                        O_CREAT,  // Create file if it does not exist
                    S_IWUSR |     // User write permission
                        S_IRUSR   // User read permission
                    );
    if (fd == -1) {
        cout << "Unable to open file." << endl;
        exit(EXIT_FAILURE);
    }
    off_t file_length = lseek(fd, 0, SEEK_END);
    // cout << "file_length: " << file_length << endl;
    Pager* pager = static_cast<Pager*>(malloc(sizeof(Pager)));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);
    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node.
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }
    return table;
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        // print_page(pager, 0);
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        cout << "Error closing db file.\n";
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        cout << "Tried to flush null page" << endl;
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        cout << "Error seeking: " << errno << endl;
        exit(EXIT_FAILURE);
    }
    // cout << "page_num: " << page_num << endl;
    // print_page(pager, page_num);
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        cout << "Error writing: " << errno << endl;
        exit(EXIT_FAILURE);
    }
}

Cursor* table_start(Table* table) {
    Cursor* cursor = static_cast<Cursor*>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = static_cast<Cursor*>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;
    return cursor;
}

uint32_t* leaf_node_num_cells(void* node) {
    return static_cast<uint32_t*>(node + LEAF_NODE_NUM_CELLS_OFFSET);
}
void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return static_cast<uint32_t*>(leaf_node_cell(node, cell_num));
}
void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}
void initialize_leaf_node(void* node) { *leaf_node_num_cells(node) = 0; }


void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        cout << "Need to implement splitting a leaf node.\n";
        exit(EXIT_FAILURE);
    }
    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1),
                LEAF_NODE_CELL_SIZE);
        }
    }
    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

void print_constants() {
    cout << "ROW_SIZE: " << ROW_SIZE << endl;
    cout << "COMMON_NODE_HEADER_SIZE: " << COMMON_NODE_HEADER_SIZE << endl;
    cout << "LEAF_NODE_HEADER_SIZE: " << LEAF_NODE_HEADER_SIZE << endl;
    cout << "LEAF_NODE_CELL_SIZE: " << LEAF_NODE_CELL_SIZE << endl;
    cout << "LEAF_NODE_SPACE_FOR_CELLS: " << LEAF_NODE_SPACE_FOR_CELLS << endl;
    cout << "LEAF_NODE_MAX_CELLS: " << LEAF_NODE_MAX_CELLS << endl;
}


void print_leaf_node(void* node) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    cout << "leaf (size " << num_cells << ")" << endl;
    for (uint32_t i = 0; i < num_cells; i++) {
        uint32_t key = *leaf_node_key(node, i);
        cout << "  - " << i << " : " << key << endl;
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cout << "Must supply a database filename." << endl;
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];

    Table* table = db_open(filename);
    string input_buffer;

    while (true) {
        print_prompt();
        getline(cin, input_buffer);
        if (!input_buffer.empty() && input_buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    cout << "Unrecognized command '" << input_buffer << "'." << endl;
                    continue;
            }
        }
        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                cout << "Syntax error. Could not parse statement." << endl;
                continue;
            case (PREPARE_STRING_TOO_LONG):
                cout << "String is too long." << endl;
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                cout << "Unrecognized keyword at start of '" << input_buffer << "'." << endl;
                continue;
        }
        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                cout << "Executed." << endl; 
                break;
            case (EXECUTE_TABLE_FULL):
                cout << "Error: Table full." << endl;
                break;
        }
    }
}