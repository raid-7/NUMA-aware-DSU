cmake_minimum_required(VERSION 3.10)
project(try)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pedantic -pthread")

find_package(LibCDS CONFIG REQUIRED)

add_executable(test_all test.cpp Queue.h DSU.h)
target_link_libraries(test_all PRIVATE LibCDS::cds-s -lnuma)

add_executable(benchmark benchmark.cpp Queue.h DSU.h)
target_link_libraries(benchmark PRIVATE LibCDS::cds-s -lnuma)