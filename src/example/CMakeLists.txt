cmake_minimum_required(VERSION 3.1)

add_executable(load_table load_table.cpp)
add_executable(cache_tests cache_tests.cpp)

link_libraries(
    ${STANDARD_LIBRARIES}
)

