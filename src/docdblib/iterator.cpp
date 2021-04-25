/*
 * iterator.cpp
 *
 *  Created on: 23. 10. 2020
 *      Author: ondra
 */
#include "iterator.h"
#include "exception.h"

namespace docdb {

void Iterator::init(const std::string_view &start_key, bool exclude_begin) {
	leveldb::Slice sl_start_key(start_key.data(),start_key.length());

	//seek to start key
	iter->Seek(sl_start_key);
	//check status
	LevelDBException::checkStatus(iter->status());
	//found some record?
	if (iter->Valid()) {
		//yes - check whether key was exactly found
		if (iter->key() == sl_start_key) {
			//yes - is exclude begin turned on
			if (exclude_begin) {
				//yes - then advance to next item on next()
				advance_fn = &Iterator::advance;
			} else {
				//no - then return true on next() and set to advance
				advance_fn = &Iterator::initial_null_advance;
			}
		//no - not exact key
		} else if (descending) {
			//in case of descending, on next(), we need to advance back
			advance_fn = &Iterator::advance;
		} else {
			//in case of ascending, we can just check for key range and set to advance
			advance_fn = &Iterator::initial_advance;
		}
		//no - did not found a key - but for descending
	} else if (descending) {
		//we can start from last item
		iter->SeekToLast();
		//if it is valid (empty database = false)
		if (iter->Valid()) {
			//on next() check key range, then set for advance()
			advance_fn = &Iterator::initial_advance;
		} else {
			//if not valid, set for always false
			advance_fn = &Iterator::not_valid_advance;
		}
	} else {
		//in case key was not found and ascending, nowhere to advance, set always false
		advance_fn = &Iterator::not_valid_advance;
	}
}

bool Iterator::initial_advance() {

	//set for advance on next()
	advance_fn = &Iterator::advance;
	//and check key range
	return check_after_advance();
}

bool Iterator::not_valid_advance(){
	//always false
	return false;
}

bool Iterator::advance() {
	//if descending, do Prev otherwise do Next
	if (descending) iter->Prev();
	else iter->Next();
	LevelDBException::checkStatus(iter->status());
	//if record is valud
	if (iter->Valid()) {
		//check range
		return check_after_advance();
	} else {
		//otherwise, set always false
		advance_fn = &Iterator::not_valid_advance;
		//return false;
		return false;
	}
}

bool Iterator::initial_null_advance() {
	//initialize advance_fn
	advance_fn = &Iterator::advance;
	//and this is success for now
	return true;
}

bool Iterator::check_after_advance() const {
	//pick end_key
	leveldb::Slice sl_end_key(end_key.data(),end_key.length());
	//read current key (assume Valid())
	auto sk = iter->key();
	//compare found key with end_key
	int cmp = sk.compare(sl_end_key);
	//if they are match
	if (cmp == 0) {
		//we can return true only if exclude_end is not set
		return !exclude_end;
	//for descending
	} else if (descending) {
		//valid key is greater then end_key
		return cmp > 0;
	} else {
		//for ascending, valid key is smaller then end_key
		return cmp < 0;
	}
}

bool Iterator::empty() const {
	return !iter->Valid() || !check_after_advance();
}

KeyRange Iterator::range() const {
	return {
		descending?end_key:iter->Valid()?std::string(iter->key().data(), iter->key().size()):end_key,
		!descending?end_key:iter->Valid()?std::string(iter->key().data(), iter->key().size()):end_key
	};
}

bool Iterator::removeFilter() {
	if (filter != nullptr) {
		filter = std::unique_ptr<IFilter>(filter->detach());
		return true;
	} else {
		return false;
	}
}

void Iterator::removeAllFilters() {
	filter = nullptr;
}

bool Iterator::peek_null_advance() {
	return true;
}
}
