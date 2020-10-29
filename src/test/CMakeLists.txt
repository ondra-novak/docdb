cmake_minimum_required(VERSION 2.8) 

add_executable (test_docdb  
	main.cpp
	)
target_link_libraries (test_docdb LINK_PUBLIC docdblib imtjson leveldb stdc++fs pthread)
