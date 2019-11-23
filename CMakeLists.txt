cmake_minimum_required(VERSION 3.15)
project(Chord-DHT)

set(CMAKE_CXX_STANDARD 14)
include_directories(.)

SET(CMAKE_CXX_FLAGS " -lssl -pthread -lcrypto ")

add_executable(main main.cpp)
#
#target_link_libraries(
#        main
#        ${CMAKE_CURRENT_SOURCE_DIR}/../../../openssl/lib/libcrypto.a
#        ${CMAKE_CURRENT_SOURCE_DIR}/../xr/xr_arp.a
#)
add_executable(KVClient KVClient/KVClient.cpp)