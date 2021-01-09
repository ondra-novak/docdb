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
#include "observable.h"

namespace docdb {

class View {
public:
	View(const DB &db, const std::string_view &name);

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

	///find for given key
	/**
	 * @param key key to find
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(json::Value key) const;

	///find for given key
	/**
	 * @param key key to find
	 * @param backward specify whether to walk forward or backward
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(json::Value key, bool backward) const;


	///find for given key allows access to specified page
	/**
	 * @param key key to find
	 * @param fromDoc specifies starting document. The document is excluded (so it
	 * should contain ID of last returned document.
	 * @param backward specify whether to walk forward or backward
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(json::Value key, const std::string_view &fromDoc, bool backward) const;

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
	///Search for range
	/**
	 * @param fromKey starting key
	 * @param toKey ending key
	 * @param fromDoc starting doc - document where to start. The document is always excluded. Note
	 * that document must be related to fromKey, otherwise it is ignored
	 * @param include_upper_bound specify whether to include upper bound. If the upper bound is fromKey, it is included only when fromDoc is empty, otherwise fromDoc is used
	 * @return iterator
	 *
	 * @note if fromKey > toKey, result is in reversed order. Note that upper bound is always
	 * the key which is above to other
	 */
	Iterator range(json::Value fromKey, json::Value toKey, const std::string_view &fromDoc, bool include_upper_bound) const;

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

	///Search for prefix
	/**
	 * @param key key to search, this should be array or string. If array is used, then
	 * all result with matching columns are returned. if string is used, then all string
	 * keys with matching prefix
	 * @param fromKey allows to start by specified key
	 * @param fromDoc allows to start by specified document (must be in relation to fromKey) - always excluded
	 * @param backward specify direction
	 * @return iterator
	 */
	Iterator prefix(json::Value key, json::Value fromKey, const std::string_view &fromDoc, bool backward) const;

	///Scan entire DB
	Iterator scan() const;

	///Scan entire view specify direction
	Iterator scan(bool backward) const;

	///Scan entire view
	Iterator scan(json::Value fromKey, const std::string_view &fromDoc, bool backward) const;



	static std::pair<json::Value, std::string_view> parseKey(const KeyView &key);
	static json::Value parseValue(const std::string_view &value);
	static json::Value extractSubKey(unsigned int index, const KeyView &key);
	static json::Value extractSubValue(unsigned int index, const std::string_view &key);


	Key createKey(const json::Value &val, const std::string_view &doc) const;
	Key createKey(const std::initializer_list<json::Value> &val, const std::string_view &doc) const;
	Key createDocKey(const std::string_view &doc) const;


	///Determines, whether document has key(s) in this view
	/**
	 * @param docId document identifier
	 * @retval true document is indexed by this view
	 * @retval false document is not indexed by this view
	 */
	bool isDocumentInView(const std::string_view &docId) const;

	///Used to list keys generated by given document
	/**
	 * Some views must remember all keys emited by documents on document-id basis. This
	 * allows to list keys for given document. This is important feature for aggregators,
	 * they need to determine, which keys has been updated for futher aggregation
	 */
	class DocKeyIterator {
	public:
		///Retrieve next result
		/** This function must be called as very first to receive first result
		 *
		 * @retval true next result is ready
		 * @retval false no more results
		 */
		bool next();
		///Peek for next result
		bool peek();
		///Retrieve current key
		json::Value key() const;
		///returns true, if result is empty
		bool empty() const;
	protected:
		friend class View;
		DocKeyIterator(const DB db, const Key &key);
		std::string buffer;
		unsigned int rdidx;
		json::Value rdkey;
	};

	DocKeyIterator getDocKeys(const std::string_view &docid) const;



protected:
	DB db;
	KeySpaceID kid;


};

template<typename Derived>
class UpdatableView: public View {
public:
	using View::View;

	using View::lookup;
	json::Value lookup(const json::Value &key, bool set_docid = false);
	using View::find;
	Iterator find(json::Value key) ;
	Iterator find(json::Value key, bool backward) ;
	Iterator find(json::Value key, const std::string_view &fromDoc, bool backward) ;
	using View::range;
	Iterator range(json::Value fromKey, json::Value toKey) ;
	Iterator range(json::Value fromKey, json::Value toKey, bool include_upper_bound) ;
	Iterator range(json::Value fromKey, json::Value toKey, const std::string_view &fromDoc, bool include_upper_bound) ;
	using View::prefix;
	Iterator prefix(json::Value key) ;
	Iterator prefix(json::Value key, bool backward) ;
	Iterator prefix(json::Value key, json::Value fromKey, const std::string_view &fromDoc, bool backward) ;
	using View::scan;
	Iterator scan() ;
	Iterator scan(bool backward) ;
	Iterator scan(json::Value fromKey, const std::string_view &fromDoc, bool backward) ;
	using View::isDocumentInView;
	bool isDocumentInView(const std::string_view &docId) ;
	using View::getDocKeys;
	DocKeyIterator getDocKeys(const std::string_view &docid) ;

	void update();


protected:
	using Obs = Observable<Batch &, const std::vector<json::Value> &>;
	std::mutex lock;
	Obs observers;

	void callUpdate() {static_cast<Derived *>(this)->update();}

public:
	template<typename Fn>
	auto addKeyUpdateObserver(Fn &&fn)  {
		std::lock_guard _(lock);
		return observers.addObserver(std::forward<Fn>(fn));
	}
	void removeKeyUpdateObserver(Obs::Handle h) {
		std::lock_guard _(lock);
		observers.removeObserver(h);
	}


	struct AggregatorAdapter {
		using IteratorType = Iterator;
		using SourceType = UpdatableView<Derived>;

		static const DB &getDB(const SourceType &src) {return src.db;}
		template<typename Fn>
		static auto observe(SourceType &src, Fn &&fn) {
			return src.addKeyUpdateObserver(std::move(fn));
		}
		static void stopObserving(SourceType &src, Obs::Handle h) {
			src.removeKeyUpdateObserver(h);
		}
		static IteratorType find(const SourceType &src, const json::Value &key) {
			return src.find(key);
		}
		static IteratorType prefix(const SourceType &src, const json::Value &key) {
			return src.prefix(key);
		}
		static IteratorType range(const SourceType &src, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound ) {
			return src.range(fromKey, toKey, include_upper_bound);
		}

	};

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

template<typename Derived>
json::Value UpdatableView<Derived>::lookup(const json::Value &key, bool set_docid) {
	callUpdate();return View::lookup(key, set_docid);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(json::Value key) {
	callUpdate();return View::find(key);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(json::Value key, bool backward) {
	callUpdate();return View::find(key, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(json::Value key,
		const std::string_view &fromDoc, bool backward) {
	callUpdate();return View::find(key, fromDoc, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(json::Value fromKey, json::Value toKey) {
	callUpdate();return View::range(fromKey, toKey);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(json::Value fromKey, json::Value toKey, bool include_upper_bound) {
	callUpdate();return View::range(fromKey, toKey, include_upper_bound);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(json::Value fromKey,
		json::Value toKey, const std::string_view &fromDoc,
		bool include_upper_bound) {
	callUpdate();return View::range(fromKey, toKey, fromDoc, include_upper_bound);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(json::Value key) {
	callUpdate();return View::prefix(key);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(json::Value key, bool backward) {
	callUpdate();return View::prefix(key, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(json::Value key,
		json::Value fromKey, const std::string_view &fromDoc, bool backward) {
	callUpdate();return View::prefix(key, fromKey, fromDoc, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::scan() {
	callUpdate();return View::scan();
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::scan(bool backward) {
	callUpdate();return View::scan(backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::scan(json::Value fromKey,
		const std::string_view &fromDoc, bool backward) {
	callUpdate();return View::scan(fromKey, fromDoc, backward);
}

template<typename Derived>
bool UpdatableView<Derived>::isDocumentInView(const std::string_view &docId) {
	callUpdate();return isDocumentInView(docId);
}

template<typename Derived>
View::DocKeyIterator UpdatableView<Derived>::getDocKeys(const std::string_view &docid) {
	callUpdate();return getDocKeys(docid);
}




}

#endif /* SRC_DOCDBLIB_VIEW_H_ */
