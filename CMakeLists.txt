cmake_minimum_required(VERSION 3.10)

# 项目名称
project(myDB)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 包含头文件目录
include_directories(include)

# 1. 添加主程序可执行文件
add_executable(myDB src/mydb.cpp)

# ============================================
# 2. 测试程序配置
# ============================================

# 设置gtest路径（假设gtest头文件和库已放在项目目录中）
set(GTEST_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/include)
set(GTEST_LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/lib)

# 查找测试源文件
file(GLOB TEST_SOURCES tests/*.cpp)

if(TEST_SOURCES)
    message(STATUS "Found test sources: ${TEST_SOURCES}")
    
    # 创建测试可执行文件 test_mydb
    add_executable(test_mydb ${TEST_SOURCES})
    
    # 设置测试程序的包含目录
    target_include_directories(test_mydb PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include  # 项目头文件
        ${GTEST_INCLUDE_DIR}                  # gtest头文件
    )
    
    # 设置库目录
    target_link_directories(test_mydb PRIVATE ${GTEST_LIB_DIR})
    
    # 链接gtest库（自动选择Debug/Release版本）
    if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        message(STATUS "Linking with debug gtest libraries")
        target_link_libraries(test_mydb
            gtestd
            gtest_maind
        )
    else()
        message(STATUS "Linking with release gtest libraries")
        target_link_libraries(test_mydb
            gtest
            gtest_main
        )
    endif()
    
    # 添加自定义目标，方便单独构建和运行测试
    add_custom_target(build_test
        DEPENDS test_mydb
        COMMENT "Building test program..."
    )
    
    add_custom_target(run_test
        DEPENDS test_mydb
        COMMAND test_mydb
        COMMENT "Running tests..."
    )
    
    # 添加所有测试目标的快捷方式
    add_custom_target(test_all
        DEPENDS build_test run_test
        COMMENT "Build and run all tests"
    )
    
else()
    message(WARNING "No test sources found in tests/ directory")
endif()