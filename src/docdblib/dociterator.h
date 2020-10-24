/*
 * dociterator.h
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_DOCITERATOR_H_
#define SRC_DOCDBLIB_DOCITERATOR_H_

#include "document.h"
#include "iterator.h"

namespace docdb {

///Iterates through documents
/**
 *
 */
class DocIterator: private Iterator {
public:
	using Iterator::Iterator;
	using Iterator::next;

	Document get() const;
	DocumentRepl replicate() const;


};

} /* namespace docdb */

#endif /* SRC_DOCDBLIB_DOCITERATOR_H_ */

