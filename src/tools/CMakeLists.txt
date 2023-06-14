cmake_minimum_required(VERSION 3.1)




add_executable(docdb_manager manage_db.cpp ReadLine.cpp)

target_link_libraries(docdb_manager 
    ${READLINE_LIBRARIES}
    ${LEVELDB_LIB}
    ${STANDARD_LIBRARIES}
)
