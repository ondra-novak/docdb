/*
 * view.h
 *
 *  Created on: 3. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_VIEW_H_
#define SRC_DOCDBLIB_VIEW_H_

#include "db.h"
#include "keyspace.h"

namespace docdb {

class View {
public:
	View(const DB &db, const std::string_view &name);

	class Iterator: public ::docdb::Iterator {
	public:
		using Super = ::docdb::Iterator;
		Iterator(Super &&src);

		bool next();
		bool peek();

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

		///Retrieves id of document, which is source of current row
		std::string_view id() const;

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
	protected:
		mutable std::optional<std::pair<json::Value, std::string_view> > cache;
	};


	static std::pair<json::Value, std::string_view> parseKey(const KeyView &key);
	static json::Value parseValue(const std::string_view &value);
	static json::Value extractSubKey(unsigned int index, const KeyView &key);
	static json::Value extractSubValue(unsigned int index, const std::string_view &key);



protected:
	KeySpaceID kid;
	DB db;


};

template<typename Pred>
inline void View::Iterator::addFilter_ifKey(Pred &&pred) {
	addFilter([pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(parseKey(key));
	});
}
template<typename Pred>
inline void View::Iterator::addFilter_ifKey(unsigned int index, Pred &&pred) {
	addFilter([index, pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(extractSubKey(index, key));
	});
}

template<typename Pred>
inline void View::Iterator::addFilter_ifValue( Pred &&pred) {
	addFilter([pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(parseValue(value));
	});
}

template<typename Pred>
inline void View::Iterator::addFilter_ifValue(unsigned int index, Pred &&pred) {
	addFilter([index, pred = std::move(pred)](const KeyView &key, const std::string_view &value){
		return pred(extractSubValue(index, value));
	});
}



}

#endif /* SRC_DOCDBLIB_VIEW_H_ */
