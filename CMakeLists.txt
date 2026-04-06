cmake_minimum_required(VERSION 3.10)
project(ShardedKVStore)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(kv_store src/main.cpp)

find_package(Threads REQUIRED)
target_link_libraries(kv_store PRIVATE Threads::Threads)
