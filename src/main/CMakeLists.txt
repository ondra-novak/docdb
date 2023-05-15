cmake_minimum_required(VERSION 3.2)

link_libraries(
    docdb
    ${STANDARD_LIBRARIES}
)

add_executable (basics basics.cpp)
