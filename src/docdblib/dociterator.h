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
class DocIterator: public Iterator {
public:
	using Iterator::Iterator;


	Document get() const;
	DocumentRepl replicate() const;

private:

	using Iterator::key;
	using Iterator::value;



};

} /* namespace docdb */

#endif /* SRC_DOCDBLIB_DOCITERATOR_H_ */

