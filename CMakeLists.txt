cmake_minimum_required(VERSION 3.2)
project (docdb)
enable_testing()
set (CMAKE_CXX_STANDARD 20)

include(library.cmake)

find_package(PkgConfig REQUIRED)
pkg_check_modules(READLINE REQUIRED readline)

find_library(LEVELDB_LIB leveldb)
if(NOT LEVELDB_LIB)
  message(FATAL_ERROR "libleveldb missing (libleveldb-dev)?")
endif()


LINK_LIBRARIES(pthread ${LEVELDB_LIB})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin/)
add_compile_options(-Wall -Wno-noexcept-type)

add_subdirectory (src/tests)
add_subdirectory (src/example)
add_subdirectory (src/tools)


