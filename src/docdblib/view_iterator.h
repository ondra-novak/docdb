/*
 * view_iterator.h
 *
 *  Created on: 13. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_VIEW_ITERATOR_H_
#define SRC_DOCDB_SRC_DOCDBLIB_VIEW_ITERATOR_H_
#include <imtjson/value.h>
#include "iterator.h"


namespace docdb {

///Helps to translate value in the view to the JSON object
/**
 * In most cases, you don't need to implement custom translator. This interface is used
 * in aggregators to compute aggregated values (so the content of the view doesn't contain
 * actual value, but data needs to compute aggregation
 *
 * This also allows to compute aggrated value only when it is needed
 */
class IValueTranslator {
public:
	virtual json::Value translate(const std::string_view &key, const std::string_view &value) const = 0;
	virtual ~IValueTranslator() {};
};


///Iterates over results returned by a search operation of the view
class ViewIterator: private Iterator {
public:
	///Don't call directly, you don't need to create iterator manually
	ViewIterator(Iterator &&iter, IValueTranslator &translator);

	///Allows to change translator
	ViewIterator(ViewIterator &&iter, IValueTranslator &translator);

	///Prepares next result
	/**You need to call this as the very first operation of the iterator */
	bool next();
	///Retrieves curreny key
	json::Value key() const;
	///Retrieves curreny value
	/**
	 * @return value
	 *
	 * @note Function can return undefined, when key was deleted. This can happen, when
	 * aggregator has nothing to aggregate. You need to handle this situation when
	 * scanning content of aggregated view.
	 */
	json::Value value() const;
	///Retrieves documeny id - source of this record
	const std::string_view id() const;

	///Retrieves original KeyView to easy modify record in the view
	auto orig_key() const {return Iterator::key();}

	using Iterator::empty;
protected:
	IValueTranslator &translator;
	mutable std::string_view docid;
	mutable json::Value ukey;
	mutable json::Value val;
	mutable bool need_parse = true;
	void parseKey() const;

};

class DefaultJSONTranslator: public IValueTranslator {
public:
	virtual json::Value translate(const std::string_view &key, const std::string_view &value) const override ;
	static DefaultJSONTranslator &getInstance();

};


}


#endif /* SRC_DOCDB_SRC_DOCDBLIB_VIEW_ITERATOR_H_ */
