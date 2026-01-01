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

TEST_F(DatabaseTest, creates_and_retrieves_a_row) {
    std::string input = "create table t1(id INT, val STRING);";
    input += "\nselect";
    input += "\n.exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Executed.",
        "db > (0, t1, create table t1(id INT, val STRING);)",
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
        input += "create table t" + std::to_string(i) + "(id INT, val STRING);\n";
    }
    input += "create table t1400(id INT, val STRING);\n";
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
        "db > Tried to fetch page number out of bounds. 101 > 100"
    };

    int i = lines.size() - 1;
    
    EXPECT_EQ(lines[i], expected[1]) 
        << "Line " << i + 1 << " mismatch.\n"
        << "Expected: \"" << expected[0] << "\"\n"
        << "Actual:   \"" << lines[i] << "\"";
}

TEST_F(DatabaseTest, keeps_data_after_closing_connection) {
    std::string input = "create table t1(id INT, val STRING);";
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
        "db > (0, t1, create table t1(id INT, val STRING);)",
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
    std::string input = "create table t1(col1 INT);";
    input += "\ncreate table t3(col3 INT);";
    input += "\ncreate table t2(col2 INT);";
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
        "- leaf (size 3)",
        "  - 1",
        "  - 2",
        "  - 3",
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

TEST_F(DatabaseTest, allows_printing_out_the_structure_of_a_3_leaf_node_btree) {
    std::string input = "";

    for (int i=1; i < 15; ++i) {
        input += "create table t" + std::to_string(i) + "(col" + std::to_string(i) + " INT);\n";
    }
    input += ".btree\n";
    input += "create table t15(col15 INT);\n";
    input += ".exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);

    // for (size_t i = 0; i < lines.size(); ++i) {
    //     std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
    // }
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Tree:",
        "- internal (size 1)",
        "  - leaf (size 7)",
        "    - 1",
        "    - 2",
        "    - 3",
        "    - 4",
        "    - 5",
        "    - 6",
        "    - 7",
        "  - key 7",
        "  - leaf (size 7)",
        "    - 8",
        "    - 9",
        "    - 10",
        "    - 11",
        "    - 12",
        "    - 13",
        "    - 14",
        "db > Executed.",
        "db > "
    };
    
    // 逐行比较
    for (size_t i = 14; i < std::min(lines.size(), expected.size()); ++i) {
        EXPECT_EQ(lines[i], expected[i-14]) 
            << "Line " << i + 1 << " mismatch.\n"
            << "Expected: \"" << expected[i] << "\"\n"
            << "Actual:   \"" << lines[i] << "\"";
    }
    
    // 确保行数匹配
    EXPECT_EQ(lines.size(), expected.size()+14) 
        << "Line count mismatch. Expected " << expected.size() 
        << " lines, got " << lines.size() << " lines.";
}

// TEST_F(DatabaseTest, prints_all_rows_in_a_multi_level_tree) {
//     std::string input = "";

//     for (int i=1; i < 16; ++i) {
//         input += "create table t" + std::to_string(i) + "(col INT);\n";
//     }
//     input += "select\n";
//     input += ".exit";
//     std::string output = runMyDB(input);
    
//     // 分割成行
//     std::vector<std::string> lines = splitLines(output);

//     for (size_t i = 0; i < lines.size(); ++i) {
//         std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
//     }
    
//     // 期望的输出行（根据你的程序实际输出调整）
//     std::vector<std::string> expected = {
//         "db > (0, t1, create table t1(col INT);)",
//         "(0, t2, create table t2(col INT);)",
//         "(0, t3, create table t3(col INT);)",
//         "(0, t4, create table t4(col INT);)",
//         "(0, t5, create table t5(col INT);)",
//         "(0, t6, create table t6(col INT);)",
//         "(0, t7, create table t7(col INT);)",
//         "(0, t8, create table t8(col INT);)",
//         "(0, t9, create table t9(col INT);)",
//         "(0, t10, create table t10(col INT);)",
//         "(0, t11, create table t11(col INT);)",
//         "(0, t12, create table t12(col INT);)",
//         "(0, t13, create table t13(col INT);)",
//         "(0, t14, create table t14(col INT);)",
//         "(0, t15, create table t15(col INT);)",
//         "Executed.", "db > "
//     };
    
//     // 逐行比较
//     for (size_t i = 15; i < std::min(lines.size(), expected.size()); ++i) {
//         EXPECT_EQ(lines[i], expected[i-15]) 
//             << "Line " << i + 1 << " mismatch.\n"
//             << "Expected: \"" << expected[i] << "\"\n"
//             << "Actual:   \"" << lines[i] << "\"";
//     }
    
//     // 确保行数匹配
//     EXPECT_EQ(lines.size(), expected.size()+15) 
//         << "Line count mismatch. Expected " << expected.size() 
//         << " lines, got " << lines.size() << " lines.";
// }

TEST_F(DatabaseTest, prints_constants) {
    std::string input = "";
    input += ".constants\n";
    input += ".exit";
    std::string output = runMyDB(input);
    
    // 分割成行
    std::vector<std::string> lines = splitLines(output);

    // for (size_t i = 0; i < lines.size(); ++i) {
    //     std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
    // }
    
    // 期望的输出行（根据你的程序实际输出调整）
    std::vector<std::string> expected = {
        "db > Constants:",
        "ROW_SIZE: 293",
        "COMMON_NODE_HEADER_SIZE: 6",
        "LEAF_NODE_HEADER_SIZE: 14",
        "LEAF_NODE_CELL_SIZE: 297",
        "LEAF_NODE_SPACE_FOR_CELLS: 4082",
        "LEAF_NODE_MAX_CELLS: 13",
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

// TEST_F(DatabaseTest, allows_printing_out_the_structure_of_a_4_leaf_node_btree) {
//     std::vector<std::string> input_lines = {
//         "insert 18 user18 person18@example.com",
//         "insert 7 user7 person7@example.com",
//         "insert 10 user10 person10@example.com",
//         "insert 29 user29 person29@example.com",
//         "insert 23 user23 person23@example.com",
//         "insert 4 user4 person4@example.com",
//         "insert 14 user14 person14@example.com",
//         "insert 30 user30 person30@example.com",
//         "insert 15 user15 person15@example.com",
//         "insert 26 user26 person26@example.com",
//         "insert 22 user22 person22@example.com",
//         "insert 19 user19 person19@example.com",
//         "insert 2 user2 person2@example.com",
//         "insert 1 user1 person1@example.com",
//         "insert 21 user21 person21@example.com",
//         "insert 11 user11 person11@example.com",
//         "insert 6 user6 person6@example.com",
//         "insert 20 user20 person20@example.com",
//         "insert 5 user5 person5@example.com",
//         "insert 8 user8 person8@example.com",
//         "insert 9 user9 person9@example.com",
//         "insert 3 user3 person3@example.com",
//         "insert 12 user12 person12@example.com",
//         "insert 27 user27 person27@example.com",
//         "insert 17 user17 person17@example.com",
//         "insert 16 user16 person16@example.com",
//         "insert 13 user13 person13@example.com",
//         "insert 24 user24 person24@example.com",
//         "insert 25 user25 person25@example.com",
//         "insert 28 user28 person28@example.com",
//     };

//     std::string input = "";

//     for (size_t i = 0; i < input_lines.size(); ++i) {
//         input += input_lines[i] + "\n";
//     }

//     input += ".btree\n";
//     input += ".exit";
//     std::string output = runMyDB(input);
    
//     // 分割成行
//     std::vector<std::string> lines = splitLines(output);

//     for (size_t i = 0; i < lines.size(); ++i) {
//         std::cout << "Line " << i + 1 << ": " << lines[i] << std::endl;
//     }
    
//     // // 期望的输出行（根据你的程序实际输出调整）
//     // std::vector<std::string> expected = {
//     //     "db > Constants:",
//     //     "ROW_SIZE: 293",
//     //     "COMMON_NODE_HEADER_SIZE: 6",
//     //     "LEAF_NODE_HEADER_SIZE: 14",
//     //     "LEAF_NODE_CELL_SIZE: 297",
//     //     "LEAF_NODE_SPACE_FOR_CELLS: 4082",
//     //     "LEAF_NODE_MAX_CELLS: 13",
//     //     "db > ",
//     // };
    
//     // // 逐行比较
//     // for (size_t i = 0; i < std::min(lines.size(), expected.size()); ++i) {
//     //     EXPECT_EQ(lines[i], expected[i]) 
//     //         << "Line " << i + 1 << " mismatch.\n"
//     //         << "Expected: \"" << expected[i] << "\"\n"
//     //         << "Actual:   \"" << lines[i] << "\"";
//     // }
    
//     // // 确保行数匹配
//     // EXPECT_EQ(lines.size(), expected.size()) 
//     //     << "Line count mismatch. Expected " << expected.size() 
//     //     << " lines, got " << lines.size() << " lines.";
// }

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}