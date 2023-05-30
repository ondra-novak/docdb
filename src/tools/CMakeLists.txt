cmake_minimum_required(VERSION 3.1)


link_libraries(
    readline
    ${STANDARD_LIBRARIES}
)


add_executable(docdb_manager manage_db.cpp ReadLine.cpp)
