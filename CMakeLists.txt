cmake_minimum_required(VERSION 3.10)

# 项目名称
project(myDB)

# 设置C++标准
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 包含头文件目录
include_directories(include)

# 添加可执行文件
add_executable(myDB src/mydb.cpp)