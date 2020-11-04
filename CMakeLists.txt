cmake_minimum_required(VERSION 3.10)
project(try)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pedantic -pthread")

add_executable(try main.cpp Queue.h DSU.h benchmark.cpp)
target_link_libraries(try -lnuma)