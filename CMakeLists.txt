cmake_minimum_required(VERSION 3.10)
project(example)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pedantic -pthread")

add_executable(example main.cpp)
target_link_libraries(example PRIVATE -lnuma)