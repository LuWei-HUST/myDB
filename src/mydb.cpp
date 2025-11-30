#include <iostream>

using namespace std;

void print_prompt();

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum { 
    PREPARE_SUCCESS, 
    PREPARE_UNRECOGNIZED_STATEMENT 
} PrepareResult;

typedef enum { 
    STATEMENT_INSERT, 
    STATEMENT_SELECT 
} StatementType;

typedef struct {
    StatementType type;
} Statement;


void print_prompt() { 
    cout << "db > "; 
}


PrepareResult prepare_statement(string input_buffer, Statement* statement) {
    if (input_buffer.substr(0, 6) == "insert") {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (input_buffer == "select") {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

int main(int argc, char* argv[]) {
    string input_buffer;

    while (true) {
        print_prompt();
        getline(cin, input_buffer);
        if (input_buffer == ".exit") {
            cout << "Bye!" << endl;
            exit(EXIT_SUCCESS);
        } else {
          cout << "Unrecognized command '" << input_buffer << "'." << endl;
        }
    }
}