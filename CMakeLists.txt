cmake_minimum_required(VERSION 3.16)

# 强制使用 Clang 编译器
set(CMAKE_C_COMPILER "clang")
set(CMAKE_CXX_COMPILER "clang++")

project(cRPC CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(ENABLE_ETCD "Enable etcd service discovery" OFF)

add_subdirectory(src)
add_subdirectory(examples)
