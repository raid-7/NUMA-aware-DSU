cmake_minimum_required(VERSION 3.10)
project(try)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -Wall -Wextra -pedantic -pthread")

file(GLOB implementations_src "implementations/*")

add_executable(benchmark benchmark.cpp DSU.h ${implementations_src})
target_link_libraries(benchmark PRIVATE -latomic -lnuma -ltbb)

add_executable(benchmark_components benchmark_components.cpp DSU.h ${implementations_src})
target_link_libraries(benchmark_components PRIVATE -latomic -lnuma -ltbb)

add_executable(benchmark_splitted benchmark_splitted.cpp DSU.h ${implementations_src})
target_link_libraries(benchmark_splitted PRIVATE -latomic -lnuma -ltbb)

add_executable(benchmark_mst benchmark_mst.cpp DSU.h ${implementations_src})
target_link_libraries(benchmark_mst PRIVATE -latomic -lnuma -ltbb)
#add_executable(test_local test_local.cpp)
#add_executable(debug debug.cpp)
