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

namespace docdb {

class Iterator {
public:

	Iterator(std::shared_ptr<leveldb::Iterator> &&iter, const std::string_view &start_key, const std::string_view &end_key, bool descending, bool exclude_end)
		:iter(std::move(iter)),end_key(end_key),descending(descending),exclude_end(exclude_end) {

		init(start_key);
	}

	bool next() {
		return (this->*advance_fn)();
	}

	std::string_view key() const {
		auto sl = iter->key();
		return std::string_view(sl.data(),sl.size());
	}

	std::string_view value() const {
		auto sl = iter->key();
		return std::string_view(sl.data(),sl.size());
	}



protected:
	std::shared_ptr<leveldb::Iterator> iter;
	std::string end_key;
	bool descending;
	bool exclude_end;
	bool (Iterator::*advance_fn)();

	void init(const std::string_view &start_key);
	bool initial_advance();
	bool advance();
	bool check_after_advance();

};


}

#endif /* SRC_DOCDBLIB_ITERATOR_H_ */
