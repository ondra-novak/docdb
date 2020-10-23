cmake_minimum_required(VERSION 2.8)
project (docdb)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)

include_directories(BEFORE src/imtjson/src src )
add_compile_options(-std=c++17)
add_compile_options(-Wall -Werror -Wno-noexcept-type)

add_subdirectory (src/imtjson/src/imtjson)
add_subdirectory (src/docdblib)
add_subdirectory (src/exec)
