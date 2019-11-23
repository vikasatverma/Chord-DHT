cmake_minimum_required()

SET(CMAKE_CXX_FLAGS "-lcrypto -pthread")
link_libraries(crypto)



project(KVServer)
add_executable(KVServer KVServer.cpp)