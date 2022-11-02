cmake_minimum_required(VERSION 3.15)
project(NUMADSU)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -pthread -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")

include(CLI11.cmake)

file(GLOB implementations_src "implementations/*")

add_executable(benchmark benchmark.cpp lib/util.cpp)
target_link_libraries(benchmark PRIVATE -latomic -lnuma)

add_executable(benchmark_components benchmark_components.cpp lib/util.cpp)
target_link_libraries(benchmark_components PRIVATE -latomic -lnuma)

add_executable(benchmark_splitted benchmark_splitted.cpp lib/util.cpp)
target_link_libraries(benchmark_splitted PRIVATE -latomic -lnuma)

add_executable(benchmark_mst benchmark_mst.cpp lib/util.cpp)
target_link_libraries(benchmark_mst PRIVATE -latomic -lnuma)

add_executable(fancy fancy.cpp lib/util.cpp)
target_link_libraries(fancy PRIVATE -lnuma CLI11::CLI11)
