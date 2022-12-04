cmake_minimum_required(VERSION 3.15)
project(NUMADSU)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic")

option(USE_LIBCPP "Use libc++ instead of libstdc++" OFF)

if (USE_LIBCPP)
    add_compile_options(-stdlib=libc++)
    add_link_options(-stdlib=libc++ -lc++abi)
endif()


find_package(Threads REQUIRED)
include(CLI11.cmake)
include(Gtest.cmake)

enable_testing()

#add_executable(benchmark benchmark.cpp lib/util.cpp)
#target_link_libraries(benchmark PRIVATE -latomic -lnuma)
#
#add_executable(benchmark_components benchmark_components.cpp lib/util.cpp)
#target_link_libraries(benchmark_components PRIVATE -latomic -lnuma)
#
#add_executable(benchmark_splitted benchmark_splitted.cpp lib/util.cpp)
#target_link_libraries(benchmark_splitted PRIVATE -latomic -lnuma)
#
#add_executable(benchmark_mst benchmark_mst.cpp lib/util.cpp)
#target_link_libraries(benchmark_mst PRIVATE -latomic -lnuma)

add_library(dsuenv lib/util.cpp)
target_link_libraries(dsuenv PUBLIC -lnuma CLI11::CLI11 Threads::Threads)

add_executable(fancy_bench fancy.cpp lib/util.cpp)
target_link_libraries(fancy_bench PRIVATE dsuenv)

add_executable(fancy_test fancy_test.cpp)
target_link_libraries(fancy_test PRIVATE dsuenv gtest_main)
add_test(NAME fancy COMMAND fancy_test)
