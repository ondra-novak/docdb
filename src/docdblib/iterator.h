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
 * for (Iterator iter=*...create....*; iter.next();) {
 * 	 auto key = iter.key();
 * 	 auto value = iter.value();
 * }
 * @endcode
 *
 *
 *
 * There is no standard iterator API for this iterator (so you cannot use for-range)
 *
 * @note to access the very first item, you have to call next(). Otherwise result is undefined
 *
 */
class Iterator {
public:

	class IFilter {
	public:
		virtual bool filter() const = 0;
		virtual IFilter *detach() = 0;
		virtual ~IFilter() {}
	};


	using IteratorImpl = leveldb::Iterator;

	struct RangeDef {
		std::string_view start_key;
		std::string_view end_key;
		bool exclude_begin;
		bool exclude_end;
	};
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
	Iterator(IteratorImpl *iter, const RangeDef &rdef)
		:iter(iter),end_key(rdef.end_key),descending(rdef.start_key>rdef.end_key),exclude_end(rdef.exclude_end) {

		init(rdef.start_key,rdef.exclude_begin);
	}

	///Prepare next item
	/**
	 * This function must be called to access the very first item as well
	 *
	 * @retval true next item is prepared
	 * @retval false no item available (no more results)
	 */
	bool next() {
		bool r =  (this->*advance_fn)();
		while (r && filter != nullptr && !filter->filter()) {
			r =  (this->*advance_fn)();
		}
		return r;
	}

	///Prepare next item but doesn't advance the pointer
	/**
	 * Works same way as next() but ensures, that futher next will not advance to next item. Allows
	 * to peek item without invalidating them.
	 *
	 * @retval true next item is prepared
	 * @retval false no more items
	 *
	 * @note when peek returns true, the first next call of peek() or next() also returns true (so you don't need
	 * to check its return value)
	 */
	bool peek() {
		bool r = next();
		if (r) this->advance_fn = &Iterator::peek_null_advance;
		return r;
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

	///Sets filter
	/**
	 * Filter allows to skip record before they become accessible by the caller.
	 *
	 * @param fn filter function. Function doesn't accept any argument, however, you can pass reference to the iterator as
	 * function's clousure. When filter is called, the iterator has already prepared item to examination, so function
	 * can access the item and return true if pass the item to the called, or false to skip
	 *
	 * This function replaces previous filter. If you need to add filter, call addFilter
	 */
	template<typename Fn>
	void setFilter(Fn &&fn);

	///Adds filter
	/**
	 * Filter allows to skip record before they become accessible by the caller.
	 *
	 * @param fn filter function. Function doesn't accept any argument, however, you can pass reference to the iterator as
	 * function's clousure. When filter is called, the iterator has already prepared item to examination, so function
	 * can access the item and return true if pass the item to the called, or false to skip
	 *
	 * This function adds new filter to the stack of filters. To remove filter, call removeFilter();
	 *
	 */
	template<typename Fn>
	void addFilter(Fn &&fn);

	///Removes last added filter
	bool removeFilter();

	///Removes all filters
	void removeAllFilters();


protected:
	std::unique_ptr<leveldb::Iterator> iter;
	std::string end_key;
	bool descending;
	bool exclude_end;
	bool (Iterator::*advance_fn)();
	std::size_t processed = 0;
	mutable std::size_t stored_size = 0;

	void init(const std::string_view &start_key, bool exclude_begin);
	bool initial_advance();
	bool advance();
	bool check_after_advance() const;
	bool initial_null_advance();
	bool not_valid_advance();
	bool peek_null_advance();

	std::unique_ptr<IFilter> filter;
};


struct KeyRange {
	std::string begin;
	std::string end;
};

template<typename Fn>
void Iterator::setFilter(Fn &&fn) {

	class Flt: public IFilter {
	public:
		Flt(Fn &&fn):fn(std::move(fn)) {}
		virtual bool filter() const {return fn();}
		virtual IFilter *detach() {return nullptr;}
	protected:
		Fn fn;
	};

	filter = std::make_unique<Flt>(std::move(fn));
}


template<typename Fn>
void Iterator::addFilter(Fn &&fn) {

	class Flt: public IFilter {
	public:
		Flt(Fn &&fn, std::unique_ptr<IFilter> &&next):fn(std::move(fn)),next(std::move(next)) {}
		virtual bool filter() const {return fn() && next->filter();}
		virtual IFilter *detach() {return next.release();}
	protected:
		Fn fn;
		std::unique_ptr<IFilter> next;
	};

	if (filter == nullptr) {
		setFilter(std::move(fn));
	}
	else {
		filter = std::make_unique<Flt>(std::move(fn), std::move(filter));
	}
}


}


#endif /* SRC_DOCDBLIB_ITERATOR_H_ */

