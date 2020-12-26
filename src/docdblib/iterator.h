/*
 * iterator.h
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_ITERATOR_H_
#define SRC_DOCDBLIB_ITERATOR_H_

#include <memory>
#include <string_view>
#include <leveldb/iterator.h>
#include "keyspace.h"

namespace docdb {

struct KeyRange;

///Core Iterator which handle iteration, bud doesn't interpret the result
/** The iterator can advance only one direction, and cannot go back
 * You use iterator by calling next() for next item and accessing the data, which are valid
 * until next() is called
 *
 * @code
 * Iterator iter=*...create....*
 * while (iter.next()) {
 * 	 auto key = iter.key();
 * 	 auto value = iter.value();
 * }
 * @endcode
 *
 * There is no standard iterator API for this iterator (so you cannot use for-range)
 *
 * @note to access the very first item, you have to call next(). Otherwise result is undefined
 *
 */
class Iterator {
public:

	///Initialize iterator
	/**
	 * @param iter leveldb iterator object
	 * @param start_key start key
	 * @param end_key end key
	 * @param exclude_end exclude end key
	 * @param exclude_begin exclude begin key
	 *
	 * @note You don't need to create iterator by this way. Use DocDB::scan() functions
	 */
	Iterator(leveldb::Iterator *iter, const std::string_view &start_key, const std::string_view &end_key, bool exclude_begin, bool exclude_end)
		:iter(iter),end_key(end_key),descending(start_key>end_key),exclude_end(exclude_end) {

		init(start_key,exclude_begin);
	}

	///Prepare next item
	/**
	 * This function must be called to access the very first item as well
	 *
	 * @retval true next item is prepared
	 * @retval false no item available (no more results)
	 */
	bool next() {
		return (this->*advance_fn)();
	}

	///Access key (return binary representation of the key)
	KeyView key() const {
		auto sl = iter->key();
		return KeyView(sl);
	}

	///Access value (return binary representation of the value)
	std::string_view value() const {
		auto sl = iter->value();
		return std::string_view(sl.data(),sl.size());
	}


	bool empty() const;



	///Receives key range for this iterator
	/** Note, if called as first operation, it returns complete range.
	 * If called after some items has been extracted, it returns remaining range
	 *
	 * @return range of keys. Returned value can be used to call DocDB::compact
	 */
	KeyRange range() const;

protected:
	std::unique_ptr<leveldb::Iterator> iter;
	std::string end_key;
	bool descending;
	bool exclude_end;
	bool (Iterator::*advance_fn)();

	void init(const std::string_view &start_key, bool exclude_begin);
	bool initial_advance();
	bool advance();
	bool check_after_advance() const;
	bool initial_null_advance();
	bool not_valid_advance();

};


struct KeyRange {
	std::string begin;
	std::string end;
};

}

#endif /* SRC_DOCDBLIB_ITERATOR_H_ */

