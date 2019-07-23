/*
 * compile.cpp
 *
 *  Created on: 23. 7. 2019
 *      Author: ondra
 */




#include <type_traits>
#include "keys.h"



void test() {
	std::string s;
	docdb::Rec_SeqIndex::Key k;
	docdb::Rec_SeqIndex::Document v;
	docdb::unpack_string(s, k);
	docdb::unpack_string(s, v);
	docdb::pack_to_string(k, s);
	docdb::pack_to_string(v, s);

}

