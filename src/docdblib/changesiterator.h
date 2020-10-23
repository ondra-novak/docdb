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

///Iterates chnage log
/**
 * Change log contains names of changed documents in order of their change. Every change
 * gets new sequence ID. You can iterate from last seen sequence ID to discover all changes
 * made after.
 *
 * ChangesIterator just retrieve SeqID and document ID. You need to call DocDB::get to retrieve
 * the document. Note that sequence also contains already deleted documents.
 *
 * Sequence is always increasing number, however there can be holes in numbering. Everytime
 * the document is changed, it receives new sequence ID, and releases old one, so hole
 * left there
 *
 * If you purge a document, the sequence ID is release, but document doesn't appear in change log,
 * so there is no way to find, that document has been deleted
 *
 */
class ChangesIterator: public Iterator {
public:
	using Iterator::Iterator;

	///Retrieve current sequence id
	SeqID seq() const;
	///Retrieve document name
	std::string_view doc() const;

private:
	using Iterator::key;
	using Iterator::value;

};

} /* namespace docdb */

#endif /* SRC_DOCDB_CHANGESITERATOR_H_ */
