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
	auto k = key();
	SeqID id = 0;
	for (unsigned char c: k) {
		id = (id << 8) | c;
	}
	return id;

}

ChangesIterator::ChangesIterator(Iterator &&iter, SeqID lastSeqId)
:Iterator(std::move(iter)),lastSeqId(lastSeqId)
{
}

SeqID ChangesIterator::getLastSeqId() const {
	return lastSeqId;
}

} /* namespace docdb */

