/*
 * json_map_view.h
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_JSON_MAP_VIEW_H_
#define SRC_DOCDBLIB_JSON_MAP_VIEW_H_
#include <docdblib/observable.h>
#include "db.h"
#include "keyspace.h"

namespace docdb {


///View wh
class JsonMapView {
public:

	JsonMapView(DB db, const std::string_view &name);

	JsonMapView(DB db, ClassID classId, const std::string_view &name);

	JsonMapView(const JsonMapView &other, const DB &snapshot);

	///Perform fast lookup for a value
	/**
	 * @param key key to lookup
	 * @param set_docid if set to true, the result is returned with key which contains document's id related to the result
	 * @return found value. If key doesn't exists, returns undefined value. If there are multiple results, it selects only one (first in the set).
	 *
	 */
	json::Value lookup(const json::Value &key) const;


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


	DB getDB() const {return db;}


	template<typename Fn>
	auto addObserver(Fn &&fn)  {
		observable->addObserver(std::forward<Fn>(fn));
	}
	void removeObserver(AbstractObservable::Handle h) {
		observable->removeObserver(h);
	}

protected:
	DB db;
	KeySpaceID kid;

	using Obs = Observable<Batch &, const std::vector<json::Value> &>;
	json::RefCntPtr<Obs> observable = db.getObservable<Obs>(kid);
};


///JsonMap which can be automaticaly updated
template<typename Derived>
class UpdatableMap: public JsonMapView {
public:

	UpdatableMap(const DB &db, const std::string_view &name)
	:JsonMapView(db, name) {
		this->db.keyspaceLock(kid, true);
	}

	UpdatableMap(const DB &db, ClassID classId, const std::string_view &name)
	:JsonMapView(db, classId, name) {
		this->db.keyspaceLock(kid, true);
	}
	~UpdatableMap() {
		db.keyspaceLock(kid, false);
	}



	json::Value lookup(const json::Value &key);
	using JsonMapView::lookup;
	Iterator find(json::Value key);
	using JsonMapView::find;
	Iterator range(json::Value fromKey, json::Value toKey);
	Iterator range(json::Value fromKey, json::Value toKey, bool include_upper_bound);
	using JsonMapView::range;
	Iterator prefix(json::Value key);
	Iterator prefix(json::Value key, bool backward);
	using JsonMapView::prefix;
	Iterator scan();
	Iterator scan(bool backward);
	Iterator scan(json::Value fromKey, bool backward);
	using JsonMapView::scan;

	class IndexBatch: public Batch {
	public:
		Key k;
		std::vector<json::Value> keys;
		std::string valbuf;

		void emit(const json::Value &key, const json::Value &value) {
			k.append(key);
			JsonMapView::createValue(value, valbuf);
			valbuf.clear();
			put(k,valbuf);
			k.clear();
			valbuf.clear();
			keys.push_back(key);
		}

		IndexBatch() {}
	};

	void update();

	///Erase key
	/**
	 * @param batch batch
	 * @param key to erase
	 */
	void erase(Batch &batch, const json::Value &key);

	///Starts to index a single document
	/**
	 * Function is generic, it doesn't defines format of the document.
	 *
	 * @param docId document id, an identification of the document (unique ID)
	 * @param batch instance of IndexState. This object can be reused between each index to save
	 * memory and time, because it reuses already allocated buffers
	 *
	 * Function resets content of the batch.
	 */
	void beginIndex(IndexBatch &batch);
	///Commits changes collected into index state
	/**
	 * @param state to be commited
	 * @param batch_more if set to true, nothing is written to database, the batch must be commited manually
	 * using DB:commitBatch(); Default value commits batch to the database now
	 */
	void commitIndex(IndexBatch &batch, bool batch_more = false);




protected:

	void callUpdate() {static_cast<Derived *>(this)->update();}

public:


	struct AggregatorAdapter {
		using IteratorType = Iterator;
		using SourceType = UpdatableMap<Derived>;

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
		static json::Value getKey(IteratorType &iter) {
			return json::Value(iter.key());
		}

	};

};

template<typename Derived>
inline json::Value UpdatableMap<Derived>::lookup(const json::Value &key) {
	callUpdate();return JsonMapView::lookup(key);
}


template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::range(json::Value fromKey, json::Value toKey) {
	callUpdate();return JsonMapView::range(fromKey, toKey);
}



template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::range(json::Value fromKey, json::Value toKey, bool include_upper_bound) {
	callUpdate();return JsonMapView::range(fromKey, toKey, include_upper_bound);
}

template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::prefix(json::Value key) {
	callUpdate();return JsonMapView::prefix(key);
}

template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::prefix(json::Value key, bool backward) {
	callUpdate();return JsonMapView::prefix(key, backward);
}

template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::scan() {
	callUpdate();return JsonMapView::scan();
}

template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::scan(bool backward) {
	callUpdate();return JsonMapView::scan(backward);
}

template<typename Derived>
inline typename UpdatableMap<Derived>::Iterator UpdatableMap<Derived>::scan(json::Value fromKey, bool backward) {
	callUpdate();return JsonMapView::scan(fromKey, backward);
}

template<typename Derived>
inline void UpdatableMap<Derived>::erase(Batch &batch, const json::Value &key) {
	auto k = createKey(key);
	batch.erase(k);
}

template<typename Derived>
inline void UpdatableMap<Derived>::beginIndex(IndexBatch &batch) {
	batch.k.transfer(kid);
	batch.k.clear();
	batch.valbuf.clear();
	batch.keys.clear();
}

template<typename Derived>
inline void UpdatableMap<Derived>::commitIndex(IndexBatch &batch, bool batch_more) {
	//broadcast keys to observers
	observable->broadcast(batch, batch.keys);
	//commit batch if batch_more is false
	if (!batch_more) {
		db.commitBatch(batch);
	}
}

}
#endif /* SRC_DOCDBLIB_JSON_MAP_VIEW_H_ */
