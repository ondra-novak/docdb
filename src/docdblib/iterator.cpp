/*
 * iterator.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */
#include "iterator.h"
#include "exception.h"

namespace docdb {

void Iterator::init(const std::string_view &start_key) {
	leveldb::Slice sl_start_key(start_key.data(),start_key.length());

	iter->Seek(sl_start_key);
	advance_fn = &Iterator::initial_advance;
}

bool Iterator::initial_advance() {

	advance_fn = &Iterator::advance;
	if (descending) {
		if (iter->Valid()) {
			return advance();
		} else {
			iter->SeekToLast();
			if (iter->Valid()) {
				return check_after_advance();
			} else {
				return false;
			}
		}
	} else {
		if (iter->Valid()) {
			return check_after_advance();
		} else {
			return false;
		}
	}

}

bool Iterator::advance() {
	if (descending) iter->Prev();
	else iter->Next();

	return check_after_advance();
}

bool Iterator::check_after_advance() {
	leveldb::Slice sl_end_key(end_key.data(),end_key.length());
	if (iter->Valid()) {
		auto sk = iter->key();
		int cmp = sk.compare(sl_end_key);
		if (cmp == 0) {
			return !exclude_end;
		} else if (descending) {
			return cmp > 0;
		} else {
			return cmp < 0;
		}
	} else {
		return false;
	}
}



}

