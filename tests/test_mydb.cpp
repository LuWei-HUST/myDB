#include <gtest/gtest.h>
#include <fstream>


class DatabaseTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 清理可能的临时文件
        std::remove("test_input.txt");
        std::remove("test_output.txt");
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
            in << ".exit" << std::endl;  // 确保程序会退出
            in.close();
        }
        
        // 2. 运行程序（重定向输入输出）
        std::string command = "./myDB < test_input.txt > test_output.txt";
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

TEST_F(DatabaseTest, meta_cmd_syntax_error) {
    std::string input = ".invalid_command";
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

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}