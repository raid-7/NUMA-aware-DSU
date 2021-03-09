cmake_minimum_required(VERSION 3.10)
project(try)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pedantic -pthread")

file(GLOB implementations_src "implementations/*")

#add_executable(test_all test.cpp DSU.h ${implementations_src})
#target_link_libraries(test_all PRIVATE -lnuma -ltbb)

add_executable(benchmark benchmark.cpp DSU.h ${implementations_src})
target_link_libraries(benchmark PRIVATE -lnuma -ltbb)