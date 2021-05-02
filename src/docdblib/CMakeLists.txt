cmake_minimum_required(VERSION 2.8) 
add_library (docdblib 
	hash.cpp
	compile.cpp 
	exception.cpp 
	iterator.cpp
	keyspace.cpp
	db.cpp
	incremental_store.cpp
	doc_store.cpp
	doc_store_index.cpp
	doc_store_map.cpp
	json_map_view.cpp
	view.cpp
	aggregator_view.cpp
	json_map.cpp
	inspector.cpp
	inspector_html.cpp	
	inspector_server.cpp
	attachments.cpp
#	replicator.cpp
	max_key.cpp
	)
