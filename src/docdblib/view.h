/*
 * view.h
 *
 *  Created on: 3. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_VIEW_H_
#define SRC_DOCDBLIB_VIEW_H_

#include <vector>
#include <imtjson/value.h>

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
	json::Value lookup(const json::Value &key) const;

	json::Value lookup(const json::Value &key, json::Value &docId) const;


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
		json::Value id() const;

		///Retrieves value
		json::Value value() const;
		///Retrieves single value of multicolumn value
		json::Value value(unsigned int index) const;


	protected:
		mutable std::optional<std::pair<json::Value, std::string_view> > cache;
	};

	///find for given key
	/**
	 * @param key key to find
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(const json::Value &key) const;

	///find for given key
	/**
	 * @param key key to find
	 * @param backward specify whether to walk forward or backward
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(const json::Value &key, bool backward) const;


	///find for given key allows access to specified page
	/**
	 * @param key key to find
	 * @param fromDoc specifies starting document. The document is excluded (so it
	 * should contain ID of last returned document.
	 * @param backward specify whether to walk forward or backward
	 * @return iterator which can iterator through rows containing the same key
	 */
	Iterator find(const json::Value &key, const json::Value &fromDoc, bool backward) const;

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
	Iterator range(const json::Value &fromKey, const json::Value &toKey) const;

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
	Iterator range(const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound) const;
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
	Iterator range(const json::Value &fromKey, const json::Value &fromDocId, const json::Value &toKey, const json::Value &toDocId) const;

	///Search for prefix
	/**
	 * @param key key to search, this should be array or string. If array is used, then
	 * all result with matching columns are returned. if string is used, then all string
	 * keys with matching prefix
	 * @return iterator
	 */
	Iterator prefix(const json::Value &key) const;

	///Search for prefix
	/**
	 * @param key key to search, this should be array or string. If array is used, then
	 * all result with matching columns are returned. if string is used, then all string
	 * keys with matching prefix
	 * @param backward specify direction
	 * @return iterator
	 */
	Iterator prefix(const json::Value &key, bool backward) const;

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
	Iterator prefix(const json::Value &key, const json::Value &fromKey, const json::Value &fromDoc, bool backward) const;

	///Scan entire DB
	Iterator scan() const;

	///Scan entire view specify direction
	Iterator scan(bool backward) const;

	///Scan entire view
	Iterator scan(const json::Value &fromKey, const json::Value &fromDoc, bool backward) const;



	static std::pair<json::Value, std::string_view> parseKey(const KeyView &key);
	static json::Value parseValue(const std::string_view &value);
	static json::Value extractSubKey(unsigned int index, const KeyView &key);
	static json::Value extractSubValue(unsigned int index, const std::string_view &key);


	Key createKey(const json::Value &val, const json::Value &docId) const;
	Key createKey(const std::initializer_list<json::Value> &val, const json::Value &docId) const;
	Key createDocKey(const json::Value &docId) const;


	///Determines, whether document has key(s) in this view
	/**
	 * @param docId document identifier
	 * @retval true document is indexed by this view
	 * @retval false document is not indexed by this view
	 */
	bool isDocumentInView(const json::Value &docId) const;

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

	DocKeyIterator getDocKeys(const json::Value &docid) const;


	static void serializeDocKeys(const std::vector<json::Value> &keys, std::string &buffer);

	template<typename Fn>
	auto addKeyUpdateObserver(Fn &&fn)  {
		return observable.addObserver(std::forward<Fn>(fn));
	}
	void removeKeyUpdateObserver(IObservable::Handle h) {
		observable.removeObserver(h);
	}


protected:
	DB db;
	KeySpaceID kid;

	using Obs = Observable<Batch &, const std::vector<json::Value> &>;
	Obs &observable = db.getObservable<Obs>(kid);


};


///IndexBatch supports to build index which is stored in the View. It is used with object UpdatableView
/**
 * You can initialize this object using UpdatableView::beginIndex.
 * Once it is initialized, you can call emit() function to generate key-value pair which will be
 * stored to the index. Once you are done with signle document, you have to call UpdatableView::commitIndex
 *
 */
class IndexBatch: public Batch {
public:
	IndexBatch():key(0) {}
	///Writes key-value pair to the index
	/**
	 * @param key key, which must be unique for single document. However it is possible to have multiple documents
	 * emiting same key.
	 * @param value a value associated with the key
	 */
	void emit(const json::Value &key, const json::Value &value);

	///Commit index to the current batch (but doesn't write to the database)
	/** @note this function also doesn't broadcast key changes. Do not call directly, use commitIndex() */

	void commit();
	//current document ID
	json::Value docId;
	//list of modified keys in JSON form
	std::vector<json::Value> keys;
	//list of previous keys for this document (filled by beginIndex)
	std::vector<json::Value> prev_keys;
	//temporary key used to build key during emit - to avoid memory reallocation
	Key key;
	//temporary buffer to store value
	std::string buffer;
	//document key (serialized docid into buffer);
	std::string dockey;
};


template<typename Derived>
class UpdatableView: public View {
public:
	using View::View;

	using View::lookup;
	json::Value lookup(const json::Value &key);
	json::Value lookup(const json::Value &key, json::Value &docId);
	using View::find;
	Iterator find(const json::Value &key) ;
	Iterator find(const json::Value &key, bool backward) ;
	Iterator find(const json::Value &key, const json::Value &fromDoc, bool backward) ;
	using View::range;
	Iterator range(const json::Value &fromKey, const json::Value &toKey) ;
	Iterator range(const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound) ;
	Iterator range(const json::Value &fromKey, const json::Value &fromDocId, const json::Value &toKey, const json::Value &toDocId) ;
	using View::prefix;
	Iterator prefix(const json::Value &key);
	Iterator prefix(const json::Value &key, bool backward);
	Iterator prefix(const json::Value &key, const json::Value &fromKey, const json::Value &fromDoc, bool backward);
	using View::scan;
	Iterator scan() ;
	Iterator scan(bool backward) ;
	Iterator scan(const json::Value &fromKey, const json::Value &fromDoc, bool backward);
	using View::isDocumentInView;
	bool isDocumentInView(const  json::Value &docId) ;
	using View::getDocKeys;
	DocKeyIterator getDocKeys(const  json::Value &docid) ;

	void update();

	///Erase all keys associated with given document
	/**
	 * @param batch batch
	 * @param docId document ID
	 */
	void erase(Batch &batch, const json::Value &docId);



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
	void beginIndex(const json::Value &docId, IndexBatch &batch);
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
		static json::Value getKey(IteratorType &iter) {
			return json::Value(iter.key());
		}


	};

};


template<typename Derived>
json::Value UpdatableView<Derived>::lookup(const json::Value &key) {
	callUpdate();return View::lookup(key);
}
template<typename Derived>
json::Value UpdatableView<Derived>::lookup(const json::Value &key, json::Value &docId) {
	callUpdate();return View::lookup(key, docId);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(const json::Value &key) {
	callUpdate();return View::find(key);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(const json::Value &key, bool backward) {
	callUpdate();return View::find(key, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::find(const json::Value &key, const json::Value &fromDoc, bool backward) {
	callUpdate();return View::find(key, fromDoc, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(const json::Value &fromKey, const json::Value &toKey) {
	callUpdate();return View::range(fromKey, toKey);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound) {
	callUpdate();return View::range(fromKey, toKey, include_upper_bound);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::range(const json::Value &fromKey, const json::Value &fromDocId, const json::Value &toKey, const json::Value &toDocId) {
	callUpdate();return View::range(fromKey, fromDocId, toKey, toDocId);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(const json::Value &key) {
	callUpdate();return View::prefix(key);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(const json::Value &key, bool backward) {
	callUpdate();return View::prefix(key, backward);
}

template<typename Derived>
View::Iterator UpdatableView<Derived>::prefix(const json::Value &key, const json::Value &fromKey, const json::Value &fromDoc, bool backward) {
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
View::Iterator UpdatableView<Derived>::scan(const json::Value &fromKey, const json::Value &fromDoc, bool backward) {
	callUpdate();return View::scan(fromKey, fromDoc, backward);
}

template<typename Derived>
bool UpdatableView<Derived>::isDocumentInView(const json::Value &docId) {
	callUpdate();return isDocumentInView(docId);
}

template<typename Derived>
View::DocKeyIterator UpdatableView<Derived>::getDocKeys(const  json::Value &docid) {
	callUpdate();return getDocKeys(docid);
}

template<typename Derived>
inline void UpdatableView<Derived>::beginIndex(const json::Value &docId, IndexBatch &batch) {
	batch.docId = docId;
	batch.buffer.clear();
	batch.key.clear();
	batch.key.transfer(kid);
	batch.key.append(docId);
	batch.dockey = batch.key.content();
	batch.key.clear();
	batch.prev_keys.clear();
	batch.keys.clear();
	auto kiter = getDocKeys(docId);
	while (kiter.next()) batch.prev_keys.push_back(kiter.key());
}

template<typename Derived>
inline void UpdatableView<Derived>::erase(Batch &batch, const json::Value &docId) {
	Key k(kid);
	DocKeyIterator iter = getDocKeys(docId);
	if (!iter.empty()) {
		k.append(docId);
		std::string dock = k.content();
		while (iter.next()) {
			k.clear();
			k.append(iter.key());
			k.append(dock);
			batch.Delete(k);
		}
		k.clear();
		k.push_back(0);
		k.append(dock);
		batch.Delete(k);
	}
}

template<typename Derived>
inline void docdb::UpdatableView<Derived>::commitIndex(IndexBatch &batch, bool batch_more) {
	//commit index to batch
	batch.commit();
	//broadcast keys to observers
	observable.broadcast(batch, batch.keys);
	//commit batch if batch_more is false
	if (!batch_more) {
		db.commitBatch(batch);
	}
}



}


#endif /* SRC_DOCDBLIB_VIEW_H_ */
