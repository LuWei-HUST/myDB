#include "mydb.h"

using namespace std;

void print_prompt() { 
    cout << "db > "; 
}

// 去除左侧空格
string ltrim(const string& s) {
    size_t start = s.find_first_not_of(" \t\n\r\f\v");
    return (start == string::npos) ? "" : s.substr(start);
}

// 去除右侧空格
string rtrim(const string& s) {
    size_t end = s.find_last_not_of(" \t\n\r\f\v");
    return (end == string::npos) ? "" : s.substr(0, end + 1);
}

// 去除两侧空格
string trim(const string& s) {
    return rtrim(ltrim(s));
}

vector<string> split(const string& str, char delimiter) {
    vector<string> result;
    size_t start = 0;
    size_t end = str.find(delimiter);
    
    while (end != string::npos) {
        result.push_back(str.substr(start, end - start));
        start = end + 1;
        end = str.find(delimiter, start);
    }
    
    // 添加最后一个部分
    result.push_back(str.substr(start));
    
    return result;
}

vector<string> splitAndRemoveEmptyString(const string& str, char delimiter) {
    vector<string> result_;
    vector<string> result;
    size_t start = 0;
    size_t end = str.find(delimiter);
    
    while (end != string::npos) {
        result_.push_back(str.substr(start, end - start));
        start = end + 1;
        end = str.find(delimiter, start);
    }
    
    // 添加最后一个部分
    result_.push_back(str.substr(start));

    for (int i=0;i<result_.size();i++) {
        if (trim(result_[i]).length() > 0) {
            result.push_back(trim(result_[i]));
        }
    }
    
    return result;
}

PrepareResult prepare_statement(string input_buffer, Statement* statement) {
    input_buffer = trim(input_buffer);

    if (input_buffer.substr(0, 6) == "insert") {
        // statement->type = STATEMENT_INSERT;
        // istringstream iss(input_buffer);
        // string command;
        // string username;
        // string email;

        // if (!(iss >> command >> statement->row_to_insert.id >> username >> email)) {
        //     cout << "Syntax error. Could not parse statement." << endl;
        //     return PREPARE_SYNTAX_ERROR;
        // }

        // if (command != "insert") {
        //     cout << "Syntax error. Could not parse statement." << endl;
        //     return PREPARE_SYNTAX_ERROR;
        // }

        // if (username.length() > COLUMN_USERNAME_SIZE || email.length() > COLUMN_EMAIL_SIZE) {
        //     return PREPARE_STRING_TOO_LONG;
        // }

        // strncpy(statement->row_to_insert.username, username.c_str(), COLUMN_USERNAME_SIZE);
        // statement->row_to_insert.username[username.length()] = '\0';
        // strncpy(statement->row_to_insert.email, email.c_str(), COLUMN_EMAIL_SIZE);
        // statement->row_to_insert.email[email.length()] = '\0';

        return PREPARE_SUCCESS;
    }
    if (input_buffer.substr(0, 6) == "create") {
        statement->type = STATEMENT_CREATE;

        string tableName;
        vector<string> colNames;
        vector<Type> colTypes;

        regex pattern(R"(create\s+table\s+(\w+)\s*\((.*)\))");
        
        smatch matches;
        if (regex_search(input_buffer, matches, pattern)) {
            tableName = matches[1];
            string columnsStr = matches[2];
            vector<string> cols_ = split(columnsStr, ',');
            vector<string> cols;
            for (int i=0;i<cols_.size();i++) {
                // ,分割后为空
                if (trim(cols_[i]).length() == 0) {
                    cout << "syntax error, create table tableName(col1 type1, col2 type2...);\n";
                    return PREPARE_SYNTAX_ERROR;
                } else {
                    cols.push_back(trim(cols_[i]));
                }
            }
            vector<string> tmp_;
            for (int i=0;i<cols.size();i++) {
                tmp_ = splitAndRemoveEmptyString(cols[i], ' ');
                if (tmp_.size() != 2) {
                    cout << "syntax error, create table tableName(col1 type1, col2 type2...);\n";
                    return PREPARE_SYNTAX_ERROR;
                } else {
                    if (tmp_[1] != "INT" && tmp_[1] != "STRING") {
                        cout << "syntax error, unsupported type '" << tmp_[1] << "'\n";
                        return PREPARE_SYNTAX_ERROR;
                    } else {
                        colNames.push_back(tmp_[0]);
                        if (tmp_[1] == "INT") {
                            colTypes.push_back(INT);
                        } else if (tmp_[1] == "STRING") {
                            colTypes.push_back(STRING);
                        }
                    }
                }
            }
            strncpy(statement->row_to_insert.name, tableName.c_str(), COLUMN_NAME_SIZE);
            statement->row_to_insert.name[tableName.length()] = '\0';
            strncpy(statement->row_to_insert.sql, input_buffer.c_str(), COLUMN_SQL_SIZE);
            statement->row_to_insert.sql[input_buffer.length()] = '\0';
        } else {
            cout << "syntax error, create table tableName(col1 type1, col2 type2...);\n";
            return PREPARE_SYNTAX_ERROR;
        }
        statement->table_to_create.colNames = colNames;
        statement->table_to_create.colTypes = colTypes;

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
        case (STATEMENT_CREATE):
            return execute_create(statement, table);
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
        print_tree(table->pager, 0, 0);
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

    memcpy(dest + ROOTPAGE_OFFSET, &(source->rootpage), ROOTPAGE_SIZE);
    strncpy(dest + NAME_OFFSET, source->name, NAME_SIZE);
    strncpy(dest + SQL_OFFSET, source->sql, SQL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
    char* src = static_cast<char*>(source);

    memcpy(&(destination->rootpage), src + ROOTPAGE_OFFSET, ROOTPAGE_SIZE);
    memcpy(&(destination->name), src + NAME_OFFSET, NAME_SIZE);
    memcpy(&(destination->sql), src + SQL_OFFSET, SQL_SIZE);
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
        /* Advance to next leaf node */
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
            /* This was rightmost leaf */
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));
    Row* row_to_insert = &(statement->row_to_insert);
    row_to_insert->rootpage = table->pager->num_pages - 1;
    uint32_t key_to_insert = num_cells + 1;
    Cursor* cursor = table_find(table, key_to_insert);
    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }
    leaf_node_insert(cursor, key_to_insert, row_to_insert);
    free(cursor);
    return EXECUTE_SUCCESS;
}

void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    /*
    Create a new node and move half the cells over.
    Insert the new value in one of the two nodes.
    Update parent or create a new parent.
    */
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *leaf_node_next_leaf(new_node) = *leaf_node_next_leaf(old_node);
    *leaf_node_next_leaf(old_node) = new_page_num;
    /*
    All existing keys plus new key should be divided
    evenly between old (left) and new (right) nodes.
    Starting from the right, move each key to correct position.
    */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);
        if (i == cursor->cell_num) {
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
            *leaf_node_key(destination_node, index_within_node) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }
    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;
    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
        void* parent = get_page(cursor->table->pager, parent_page_num);
        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }
}

uint32_t* node_parent(void* node) { 
    return (uint32_t*)((char*)node + PARENT_POINTER_OFFSET); 
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

/*
Until we start recycling free pages, new pages will always
go onto the end of the database file
*/
uint32_t get_unused_page_num(Pager* pager) { return pager->num_pages; }


void create_new_root(Table* table, uint32_t right_child_page_num) {
    /*
    Handle splitting the root.
    Old root copied to new page, becomes left child.
    Address of right child passed in.
    Re-initialize root page to contain the new root node.
    New root node points to two children.
    */
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);
    if (get_node_type(root) == NODE_INTERNAL) {
        initialize_internal_node(right_child);
        initialize_internal_node(left_child);
    }
    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);
    if (get_node_type(left_child) == NODE_INTERNAL) {
        void* child;
        for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
            child = get_page(table->pager, *internal_node_child(left_child,i));
            *node_parent(child) = left_child_page_num;
        }
        child = get_page(table->pager, *internal_node_right_child(left_child));
        *node_parent(child) = left_child_page_num;
    }
    /* Root node is a new internal node with one key and two children */
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) = table->root_page_num;
}

uint32_t* internal_node_num_keys(void* node) {
    return (uint32_t*)((char*)node + INTERNAL_NODE_NUM_KEYS_OFFSET);
}
uint32_t* internal_node_right_child(void* node) {
    return (uint32_t*)((char*)node + INTERNAL_NODE_RIGHT_CHILD_OFFSET);
}
uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return (uint32_t*)((char*)node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE);
}

uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
        cout << "Tried to access child_num " << child_num << " > num_keys " << num_keys << "\n";
        exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
        uint32_t* right_child = internal_node_right_child(node);
        if (*right_child == INVALID_PAGE_NUM) {
            cout << "Tried to access right child of node, but was invalid page\n";
            exit(EXIT_FAILURE);
        }
        return right_child;
    } else {
        uint32_t* child = internal_node_cell(node, child_num);
        if (*child == INVALID_PAGE_NUM) {
            cout << "Tried to access child " << child_num << " of node, but was invalid page\n";
            exit(EXIT_FAILURE);
        }
        return child;
    }
}

uint32_t* internal_node_key(void* node, uint32_t key_num) {
  // return internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
    return (uint32_t*)((char*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE);
}

uint32_t get_node_max_key(Pager* pager, void* node) {
    if (get_node_type(node) == NODE_LEAF) {
        return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
    void* right_child = get_page(pager,*internal_node_right_child(node));
    return get_node_max_key(pager, right_child);
}


bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)((char*)node + IS_ROOT_OFFSET));
    return (bool)value;
}
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)((char*)node + IS_ROOT_OFFSET)) = value;
}

/*
Return the position of the given key.
If the key is not present, return the position
where it should be inserted
*/
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);
    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}

uint32_t internal_node_find_child(void* node, uint32_t key) {
    /*
    Return the index of the child which should contain
    the given key.
    */
    uint32_t num_keys = *internal_node_num_keys(node);
    /* Binary search */
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; /* there is one more child than key */
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    return min_index;
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    /*
    Add a new child/key pair to parent that corresponds to child
    */
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    // uint32_t child_max_key = get_node_max_key(child);
    uint32_t child_max_key = get_node_max_key(table->pager, child);
    uint32_t index = internal_node_find_child(parent, child_max_key);
    uint32_t original_num_keys = *internal_node_num_keys(parent);
    // *internal_node_num_keys(parent) = original_num_keys + 1;
    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS) {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    uint32_t right_child_page_num = *internal_node_right_child(parent);
    /*
    An internal node with a right child of INVALID_PAGE_NUM is empty
    */
    if (right_child_page_num == INVALID_PAGE_NUM) {
        *internal_node_right_child(parent) = child_page_num;
        return;
    }
    void* right_child = get_page(table->pager, right_child_page_num);
    /*
    If we are already at the max number of cells for a node, we cannot increment
    before splitting. Incrementing without inserting a new key/child pair
    and immediately calling internal_node_split_and_insert has the effect
    of creating a new key at (max_cells + 1) with an uninitialized value
    */
    *internal_node_num_keys(parent) = original_num_keys + 1;
    if (child_max_key > get_node_max_key(table->pager, right_child)) {
        /* Replace right child */
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        /* Make room for the new cell */
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    uint32_t old_page_num = parent_page_num;
    void* old_node = get_page(table->pager,parent_page_num);
    uint32_t old_max = get_node_max_key(table->pager, old_node);
    void* child = get_page(table->pager, child_page_num); 
    uint32_t child_max = get_node_max_key(table->pager, child);
    uint32_t new_page_num = get_unused_page_num(table->pager);
    /*
    Declaring a flag before updating pointers which
    records whether this operation involves splitting the root -
    if it does, we will insert our newly created node during
    the step where the table's new root is created. If it does
    not, we have to insert the newly created node into its parent
    after the old node's keys have been transferred over. We are not
    able to do this if the newly created node's parent is not a newly
    initialized root node, because in that case its parent may have existing
    keys aside from our old node which we are splitting. If that is true, we
    need to find a place for our newly created node in its parent, and we
    cannot insert it at the correct index if it does not yet have any keys
    */
    uint32_t splitting_root = is_node_root(old_node);
    void* parent;
    void* new_node;
    if (splitting_root) {
        create_new_root(table, new_page_num);
        parent = get_page(table->pager,table->root_page_num);
        /*
        If we are splitting the root, we need to update old_node to point
        to the new root's left child, new_page_num will already point to
        the new root's right child
        */
        old_page_num = *internal_node_child(parent,0);
        old_node = get_page(table->pager, old_page_num);
    } else {
        parent = get_page(table->pager,*node_parent(old_node));
        new_node = get_page(table->pager, new_page_num);
        initialize_internal_node(new_node);
    }

    uint32_t* old_num_keys = internal_node_num_keys(old_node);
    uint32_t cur_page_num = *internal_node_right_child(old_node);
    void* cur = get_page(table->pager, cur_page_num);
    /*
    First put right child into new node and set right child of old node to invalid page number
    */
    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;
    *internal_node_right_child(old_node) = INVALID_PAGE_NUM;
    /*
    For each key until you get to the middle key, move the key and the child to the new node
    */
    for (int i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--) {
        cur_page_num = *internal_node_child(old_node, i);
        cur = get_page(table->pager, cur_page_num);
        internal_node_insert(table, new_page_num, cur_page_num);
        *node_parent(cur) = new_page_num;
        (*old_num_keys)--;
    }
    /*
    Set child before middle key, which is now the highest key, to be node's right child,
    and decrement number of keys
    */
    *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
    (*old_num_keys)--;
    /*
    Determine which of the two nodes after the split should contain the child to be inserted,
    and insert the child
    */
    uint32_t max_after_split = get_node_max_key(table->pager, old_node);
    uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;
    internal_node_insert(table, destination_page_num, child_page_num);
    *node_parent(child) = destination_page_num;
    update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));
    if (!splitting_root) {
        internal_node_insert(table,*node_parent(old_node),new_page_num);
        *node_parent(new_node) = *node_parent(old_node);
    }
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_keys = *internal_node_num_keys(node);
    /* Binary search to find index of child to search */
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; /* there is one more child than key */
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
        default:
            __builtin_unreachable();
    }
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    Cursor* cursor = static_cast<Cursor*>(malloc(sizeof(Cursor)));
    cursor->table = table;
    cursor->page_num = page_num;
    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }
    cursor->cell_num = min_index;
    return cursor;
}


NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)((char*)node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)((char*)node + NODE_TYPE_OFFSET)) = value;
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

ExecuteResult execute_create(Statement* statement, Table* table) {
    execute_insert(statement, table);
    return EXECUTE_SUCCESS;
}

void print_row(Row* row) {
    cout << "(" << row->rootpage << ", " << row->name << ", " << row->sql << ")" << endl;
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
        cout << "Db file is not a whole number of pages. Corrupt file." << endl;
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
        set_node_root(root_node, true);
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
    Cursor* cursor = table_find(table, 0);
    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);
    return cursor;
}

uint32_t* leaf_node_next_leaf(void* node) {
    return (uint32_t*)((char*)node + LEAF_NODE_NEXT_LEAF_OFFSET);
}

uint32_t* leaf_node_num_cells(void* node) {
    return (uint32_t*)(((char*)node + LEAF_NODE_NUM_CELLS_OFFSET));
}
void* leaf_node_cell(void* node, uint32_t cell_num) {
    return (void*)((char*)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE);
}
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return static_cast<uint32_t*>(leaf_node_cell(node, cell_num));
}
void* leaf_node_value(void* node, uint32_t cell_num) {
    return (void*)((char*)leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE);
}

void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0;  // 0 represents no sibling
}

void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;
    /*
    Necessary because the root page number is 0; by not initializing an internal 
    node's right child to an invalid page number when initializing the node, we may
    end up with 0 as the node's right child, which makes the node a parent of the root
    */
    *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
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

void indent(uint32_t level) {
    for (uint32_t i = 0; i < level; i++) {
        cout << "  ";
    }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;
    switch (get_node_type(node)) {
        case (NODE_LEAF):
            num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            cout << "- leaf (size " << num_keys << ")\n";
            for (uint32_t i = 0; i < num_keys; i++) {
                indent(indentation_level + 1);
                cout << "- " << *leaf_node_key(node, i) << "\n";
            }
            break;
        case (NODE_INTERNAL):
            num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            cout << "- internal (size " << num_keys << ")\n";
            if (num_keys > 0) {
                for (uint32_t i = 0; i < num_keys; i++) {
                    child = *internal_node_child(node, i);
                    print_tree(pager, child, indentation_level + 1);
                    indent(indentation_level + 1);
                    cout << "- key " << *internal_node_key(node, i) << "\n";
                }
                child = *internal_node_right_child(node);
                print_tree(pager, child, indentation_level + 1);
            }
            break;
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
            case (EXECUTE_DUPLICATE_KEY):
                cout << "Error: Duplicate key." << endl;
                break;
            case (EXECUTE_TABLE_FULL):
                cout << "Error: Table full." << endl;
                break;
        }
    }
}