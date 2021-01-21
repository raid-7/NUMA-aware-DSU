cmake_minimum_required(VERSION 3.17)
project(counter)

set(CMAKE_CXX_STANDARD 14)

add_executable(counter main.cpp)
target_link_libraries(counter PRIVATE -lnuma)