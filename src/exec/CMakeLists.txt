cmake_minimum_required(VERSION 2.8) 
add_compile_options(-std=c++17)

add_executable (docdb  
	main.cpp
	)
target_link_libraries (docdb LINK_PUBLIC docdblib stdc++fs pthread)
install(TARGETS docdb DESTINATION "bin") 
