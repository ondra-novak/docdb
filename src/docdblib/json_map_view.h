/*
 * json_map_view.h
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_JSON_MAP_VIEW_H_
#define SRC_DOCDBLIB_JSON_MAP_VIEW_H_
#include "db.h"
#include "keyspace.h"

namespace docdb {


///View wh
class JsonMapView {
public:

	JsonMapView(const DB &db, const std::string_view &name);

	///Perform fast lookup for a value
	/**
	 * @param key key to lookup
	 * @param set_docid if set to true, the result is returned with key which contains document's id related to the result
	 * @return found value. If key doesn't exists, returns undefined value. If there are multiple results, it selects only one (first in the set).
	 *
	 */
	json::Value lookup(const json::Value &key, bool set_docid = false) const;


	class Iterator: public ::docdb::Iterator {
	public:
		using Super = ::docdb::Iterator;
		Iterator(Super &&src);

		///Retrieve global key value
		/** Useful if you need to access key-value at database level */
		KeyView global_key();

		///Retrieve key
		/**
		 * @return key value
		 *
		 * @note parsers and packs key to json::Value object. If key is multicolumn key and you
		 * need just value of single column, use key(index)
		 */
		json::Value key() const;
		///Retrieve single value of multicolumn key
		/**
		 * Function is much faster than parse whole key and read single value. However it
		 * is much slower to enumerate all columns of the key.
		 *
		 * @param index index of column
		 * @return returns key at given column, or undefined, if column doesn't exist
		 */
		json::Value key(unsigned int index) const;

		///Retrieves value
		json::Value value() const;
		///Retrieves single value of multicolumn value
		json::Value value(unsigned int index) const;

		///Allows to filter result by predicate testing specified key
		/**
		 * @param pred predicate. The function receives single argument - key - as json::Value.
		 * It should return true to include the row to result or false to exclude
		 */
		template<typename Pred>
		void addFilter_ifKey(Pred &&pred);
		///Allows to filter result by predicate testing specified key
		/**
		 * @param index specifies which field to test
		 * @param pred predicate. The function receives single argument - key - as json::Value.
		 * It should return true to include the row to result or false to exclude
		 */
		template<typename Pred>
		void addFilter_ifKey(unsigned int index, Pred &&pred);
		///Allows to filter result by predicate testing specified value
		/**
		 * @param pred predicate. The function receives single argument - value - as json::Value.
		 * It should return true to include the row to result or false to exclude
		 */
		template<typename Pred>
		void addFilter_ifValue(Pred &&pred);
		///Allows to filter result by predicate testing specified value
		/**
		 * @param index specifies which field to test
		 * @param pred predicate. The function receives single argument - value - as json::Value.
		 * It should return true to include the row to result or false to exclude
		 */
		template<typename Pred>
		void addFilter_ifValue(unsigned int index, Pred &&pred);
	};

	///find for given key
	/**
	 * @param key key to find
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(json::Value key) const;


	///Search for range
	/**
	 * @param fromKey starting key
	 * @param toKey ending key
	 * @return iterator
	 *
	 * @note if fromKey > toKey, result is in reversed order. Note that upper bound is excluded
	 * whatever upper bound is. So if the fromKey > toKey, there will be no result
	 * containing fromKey.
	 */
	Iterator range(json::Value fromKey, json::Value toKey) const;

	///Search for range
	/**
	 * @param fromKey starting key
	 * @param toKey ending key
	 * @param include_upper_bound specify whether to include upper bound
	 * @return iterator
	 *
	 * @note if fromKey > toKey, result is in reversed order. Note that upper bound is always
	 * the key which is above to other
	 */
	Iterator range(json::Value fromKey, json::Value toKey, bool include_upper_bound) const;

	///Search for prefix
	/**
	 * @param key key to search, this should be array or string. If array is used, then
	 * all result with matching columns are returned. if string is used, then all string
	 * keys with matching prefix
	 * @return iterator
	 */
	Iterator prefix(json::Value key) const;

	///Search for prefix
	/**
	 * @param key key to search, this should be array or string. If array is used, then
	 * all result with matching columns are returned. if string is used, then all string
	 * keys with matching prefix
	 * @param backward specify direction
	 * @return iterator
	 */
	Iterator prefix(json::Value key, bool backward) const;


	///Scan entire DB
	Iterator scan() const;

	///Scan entire view specify direction
	Iterator scan(bool backward) const;

	///Scan entire view
	Iterator scan(json::Value fromKey, bool backward) const;

	static json::Value parseKey(const KeyView &key);
	static json::Value parseValue(const std::string_view &value);
	static json::Value parseValue(std::string_view &&value);
	static json::Value extractSubKey(unsigned int index, const KeyView &key);
	static json::Value extractSubValue(unsigned int index, const std::string_view &key);


	Key createKey(const json::Value &val) const;
	Key createKey(const std::initializer_list<json::Value> &val) const;
	static void createValue(const json::Value &val, std::string &out);
	static void createValue(const std::initializer_list<json::Value> &val, std::string &out);



protected:
	DB db;
	KeySpaceID kid;
};

template<typename Pred>
inline void JsonMapView::Iterator::addFilter_ifKey(Pred &&pred) {
	addFilter([pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(parseKey(key));
	});
}
template<typename Pred>
inline void JsonMapView::Iterator::addFilter_ifKey(unsigned int index, Pred &&pred) {
	addFilter([index, pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(extractSubKey(index, key));
	});
}

template<typename Pred>
inline void JsonMapView::Iterator::addFilter_ifValue( Pred &&pred) {
	addFilter([pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(parseValue(value));
	});
}

template<typename Pred>
inline void JsonMapView::Iterator::addFilter_ifValue(unsigned int index, Pred &&pred) {
	addFilter([index, pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(extractSubValue(index, value));
	});
}


}



#endif /* SRC_DOCDBLIB_JSON_MAP_VIEW_H_ */
