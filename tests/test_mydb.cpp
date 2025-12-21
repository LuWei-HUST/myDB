#include <gtest/gtest.h>
#include <fstream>


class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 清理可能的临时文件
        std::remove("test_input.txt");
        std::remove("test_output.txt");
        std::remove("test.db");
    }
    
    void TearDown() override {
        // 清理临时文件
        std::remove("test_input.txt");
        std::remove("test_output.txt");
    }
    
    std::string runMyDB(const std::string& input) {
        // 1. 创建输入文件
        {
            std::ofstream in("test_input.txt");
            in << input << std::endl;
            in.close();
        }
        
        // 2. 运行程序（重定向输入输出）
        std::string command = "./myDB test.db < test_input.txt > test_output.txt";
        int result = std::system(command.c_str());
        
        // 3. 读取输出文件
        std::ifstream out("test_output.txt");
        if (!out.is_open()) {
            return "ERROR: Could not open output file";
        }
        
        std::stringstream buffer;
        buffer << out.rdbuf();
        out.close();
        
        return buffer.str();
    }

    std::vector<std::string> splitLines(const std::string& text) {
        std::vector<std::string> lines;
        std::stringstream ss(text);
        std::string line;
        
        while (std::getline(ss, line)) {
            lines.push_back(line);
        }
        
        return lines;
    }
};

TEST_F(DatabaseTest, inserts_and_retrieves_a_row) {
    std::string input = "insert 1 user1 person1@example.com";
    input += "\nselect";
    input += "\n.exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > "
    };
    
    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, prints_error_message_when_table_is_full) {
    std::string input = "";

    for (int i=0; i < 1400; ++i) {
        input += "insert " + std::to_string(i) + " user" + std::to_string(i) + " person" + std::to_string(i) + "@example.com\n";
    }
    input += "insert 1400 user1400 person1400@example.com";
    input += "\n.exit";

    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Error: Table full."
    };

    int i = lines.size() - 2;
    
    EXPECT_EQ(lines[i], expected[0]) 
        << "Line " << i + 1 << " mismatch.\n"
        << "Expected: \"" << expected[0] << "\"\n"
        << "Actual:   \"" << lines[i] << "\"";
}

TEST_F(DatabaseTest, allows_inserting_strings_that_are_the_maximum_length) {
    std::string long_username(32, 'a'); // 32 个 'a'
    std::string long_email(255, 'b');   // 255 个 'b
    std::string input = "";
    input += "insert 1 " + long_username + " " + long_email + "\n";
    input += "select";
    input += "\n.exit";

    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);

    // for (size_t i = 0; i < lines.size(); ++i) {
    //     std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
    // }
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > (1, " + long_username + ", " + long_email + ")",
        "Executed.",
        "db > "
    };

    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, prints_error_message_if_strings_are_too_long) {
    std::string long_username(33, 'a'); // 33 个 'a'
    std::string long_email(256, 'b');   // 256 个 'b
    std::string input = "";
    input += "insert 1 " + long_username + " " + long_email + "\n";
    input += "select";
    input += "\n.exit";

    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);

    // for (size_t i = 0; i < lines.size(); ++i) {
    //     std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
    // }
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > String is too long.",
        "db > Executed.",
        "db > "
    };

    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, keeps_data_after_closing_connection) {
    std::string input = "insert 1 user1 person1@example.com";
    input += "\n.exit";

    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > "
    };

    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";

    // 重新打开数据库并查询
    input = "select";
    input += "\n.exit";
    output = runMyDB(input);

    // 分割成行
    lines = splitLines(output);

    // 期望的输出行
    expected = {
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > "
    };

    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }

    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, allows_printing_out_the_structure_of_a_one_node_btree) {
    std::string input = "insert 3 user3 person3@example.com";
    input += "\ninsert 1 user1 person1@example.com";
    input += "\ninsert 2 user2 person2@example.com";
    input += "\n.btree";
    input += "\n.exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > Executed.",
        "db > Executed.",
        "db > Tree:",
        "leaf (size 3)",
        "  - 0 : 1",
        "  - 1 : 2",
        "  - 2 : 3",
        "db > "
    };
    
    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, meta_cmd_syntax_error) {
    std::string input = ".invalid_command";
    input += "\n.exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Unrecognized command '.invalid_command'.",
        "db > "
    };
    
    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

TEST_F(DatabaseTest, prints_an_error_message_if_there_is_a_duplicate_id) {
    std::string input = "insert 1 user1 person1@example.com";
    input += "\ninsert 1 user1 person1@example.com";
    input += "\nselect";
    input += "\n.exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > Error: Duplicate key.",
        "db > (1, user1, person1@example.com)",
        "Executed.",
        "db > ",
    };
    
    // 逐行比较
    for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}