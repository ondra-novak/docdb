cmake_minimum_required(VERSION 2.8) 
add_library (docdblib 
	compile.cpp 
#	docdb.cpp 
	exception.cpp 
	iterator.cpp
#	dociterator.cpp
	changesiterator.cpp
#	abstractview.cpp
#	attachments.cpp
	updatableobject.cpp
#	filedb.cpp
#	aggrview.cpp
#	view_iterator.cpp
	keyspace.cpp
	db.cpp
	incremental_store.cpp
	doc_store.cpp
	)
