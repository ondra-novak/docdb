/*
 * dociterator.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#include "dociterator.h"

#include "docdb.h"
namespace docdb {

Document DocIterator::get() const {
	auto id = Iterator::key();
	auto data = Iterator::value();
	return DocDB::deserializeDocument(id, data);
}

DocumentRepl DocIterator::replicate() const {
	auto id = Iterator::key();
	auto data = Iterator::value();
	return DocDB::deserializeDocumentRepl(id, data);
}

} /* namespace docdb */

