cmake_minimum_required(VERSION 3.15)

SET(CMAKE_CXX_FLAGS "-lcrypto -pthread")
link_libraries(crypto)

project(KVServer)
add_executable(KVServer KVServer.cpp)