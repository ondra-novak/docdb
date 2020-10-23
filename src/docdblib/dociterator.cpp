/*
 * dociterator.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#include "dociterator.h"

#include "docdb.h"
namespace docdb {

DocIterator::DocIterator() {
	// TODO Auto-generated constructor stub

}

Document DocIterator::get() const {
	auto id = key();
	auto data = value();
	return DocDB::deserializeDocument(id, data);
}

Document DocIterator::replicate() const {
	auto id = key();
	auto data = value();
	return DocDB::deserializeDocumentRepl(id, data);
}

} /* namespace docdb */

