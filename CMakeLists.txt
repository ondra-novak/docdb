cmake_minimum_required(VERSION 3.2)
project (docdb)
enable_testing()
set (CMAKE_CXX_STANDARD 20)

include(library.cmake)

LINK_LIBRARIES(pthread leveldb)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)
add_compile_options(-Wall -Wno-noexcept-type)

add_subdirectory (src/tests)
add_subdirectory (src/example)
add_subdirectory (src/tools)


