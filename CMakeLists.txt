cmake_minimum_required(VERSION 3.10)

project(myDB)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 1. 添加主程序可执行文件
add_executable(myDB src/mydb.cpp)

# 包含项目头文件目录
target_include_directories(myDB PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

# ============================================
# 2. 测试程序配置（使用系统已安装的gtest）
# ============================================

# 查找测试源文件
file(GLOB TEST_SOURCES tests/*.cpp)

if(TEST_SOURCES)
    message(STATUS "Found test sources: ${TEST_SOURCES}")
    
    # 创建测试可执行文件
    add_executable(test_mydb ${TEST_SOURCES})
    
    # 包含项目头文件目录
    target_include_directories(test_mydb PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    
    # 方法1：直接链接系统库（最简单）
    target_link_libraries(test_mydb
        gtest
        gtest_main
        pthread
    )
    
    # 方法2：使用 find_package（更规范）
    # find_package(GTest)
    # if(GTest_FOUND)
    #     target_link_libraries(test_mydb
    #         GTest::gtest
    #         GTest::gtest_main
    #     )
    # else()
    #     # 降级到方法1
    #     target_link_libraries(test_mydb
    #         gtest
    #         gtest_main
    #         pthread
    #     )
    # endif()
    
    # 添加测试目标
    add_custom_target(build_test
        DEPENDS test_mydb
        COMMENT "Building test program..."
    )
    
    add_custom_target(run_test
        DEPENDS test_mydb
        COMMAND ./test_mydb
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Running tests..."
    )
    
    add_custom_target(test_all
        DEPENDS build_test run_test
        COMMENT "Build and run all tests"
    )
    
else()
    message(WARNING "No test sources found in tests/ directory")
endif()