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
        std::string command = "myDB.exe < test_input.txt > test_output.txt";
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
};

TEST_F(DatabaseTest, InsertCommand) {
    std::string input = "insert 1 luwei luwei@db.com";
    input += "\nselect";
    std::string output = runMyDB(input);
    
    std::cout << "=== DEBUG OUTPUT ===" << std::endl;
    std::cout << "Output (" << output.length() << " chars):" << std::endl;
    std::cout << output << std::endl;
    std::cout << "=== END DEBUG ===" << std::endl;
    
    // 检查是否有任何输出
    EXPECT_FALSE(output.empty()) << "Program should produce output";
    
    // 检查是否包含某些关键词（宽松检查）
    bool hasContent = output.find("Executed") != std::string::npos;
    
    EXPECT_TRUE(hasContent) << "Output should contain expected text. Got:\n" << output;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}