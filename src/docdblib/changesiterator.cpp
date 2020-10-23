/*
 * changesiterator.cpp
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#include "changesiterator.h"

#include "formats.h"
#include <imtjson/binjson.tcc>

namespace docdb {

std::string_view docdb::ChangesIterator::doc() const {
	return Iterator::value();
}

SeqID docdb::ChangesIterator::seq() const {
	return string2index(Iterator::key());
}



} /* namespace docdb */

