/*
 * changesiterator.h
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_CHANGESITERATOR_H_
#define SRC_DOCDB_CHANGESITERATOR_H_
#include "iterator.h"
#include "document.h"

namespace docdb {

class ChangesIterator: public Iterator {
public:
	using Iterator::Iterator;

	SeqID seq() const;
	std::string_view doc() const;

private:
	using Iterator::key;
	using Iterator::value;

};

} /* namespace docdb */

#endif /* SRC_DOCDB_CHANGESITERATOR_H_ */
