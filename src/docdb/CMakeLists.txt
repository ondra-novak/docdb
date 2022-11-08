cmake_minimum_required(VERSION 3.2) 

set(docdb_sources 
    key.cpp
    database.cpp
    snapshot.cpp
    storage.cpp
)


add_library (docdb ${docdb_sources}) 

