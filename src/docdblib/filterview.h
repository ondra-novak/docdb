/*
 * filterview.h
 *
 *  Created on: 6. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_FILTERVIEW_H_
#define SRC_DOCDBLIB_FILTERVIEW_H_
#include <mutex>

#include "db.h"
#include "keyspace.h"
#include "observable.h"


namespace docdb {

///FilterView is simplified view, where key is also documen-id
/** FilterView allows to create set of documents matching some criteria and
 *  also allows to set values to these documents (stored separatedly). FilterView
 *  doesn't have key, because the key is always document id.

 */
class FilterView {
public:

	FilterView(const DB &db, const std::string_view &name);

	json::Value lookup(const std::string_view &docId) const;


	class Iterator: public ::docdb::Iterator {
	public:
		using Super = ::docdb::Iterator;
		Iterator(Super &&src);

		///Retrieve global key value
		/** Useful if you need to access key-value at database level */
		KeyView global_key();

		///Retrieve key
		/**
		 * @return key
		 *
		 * @note key is exactly same as document id, it is only converted to json::Value
		 */
		json::Value key() const;

		///Retrieves id of document, which is source of current row
		std::string_view id() const;

		///Retrieves value
		json::Value value() const;
		///Retrieves single value of multicolumn value
		json::Value value(unsigned int index) const;


	};
	///find for given documen id
	/**
	 * @param docid document id
	 * @return value stored along with document id, undefined if not found
	 */
	json::Value find(const std::string_view &docid) const;

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
	Iterator range(const std::string_view &fromKey, const std::string_view &toKey) const;

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
	Iterator range(const std::string_view &fromKey, const std::string_view &toKey, bool include_upper_bound) const;

	///Search for prefix
	/**
	 */
	Iterator prefix(const std::string_view &prefix) const;

	///Search for prefix
	/**
	 */
	Iterator prefix(const std::string_view &prefix, bool backward) const;

	///Scan entire DB
	Iterator scan() const;

	///Scan entire view specify direction
	Iterator scan(bool backward) const;

	///Scan entire view
	Iterator scan(const std::string_view &fromDoc, bool backward) const;



	static json::Value parseValue(const std::string_view &value);
	static json::Value extractSubValue(unsigned int index, const std::string_view &key);
	static const std::string &createValue(const json::Value v);


	Key createKey(const json::Value &doc) const;

	bool isDocumentInView(const json::Value &docId) const;
protected:
	DB db;
	KeySpaceID kid;
};

template<typename Derived>
class UpdatableFilterView: public FilterView {
public:
	using FilterView::FilterView;
	json::Value lookup(const std::string_view &docId) ;
	json::Value find(const std::string_view &docid) ;
	Iterator range(const std::string_view &fromKey, const std::string_view &toKey) ;
	Iterator range(const std::string_view &fromKey, const std::string_view &toKey, bool include_upper_bound) ;
	Iterator prefix(const std::string_view &prefix) ;
	Iterator prefix(const std::string_view &prefix, bool backward) ;
	Iterator scan() ;
	Iterator scan(bool backward) ;
	Iterator scan(const std::string_view &fromDoc, bool backward) ;
	bool isDocumentInView(const std::string_view &docId) ;

	json::Value get(const std::string_view &docId) const;
	void set(const std::string_view &id, json::Value value);
	void erase(const std::string_view &id);
	void set(Batch &b, const std::string_view &id, json::Value value);
	void erase(Batch &b, const std::string_view &id);

protected:
	using Obs = Observable<Batch &,const std::string_view &>;
	Obs observers;
	std::mutex lock;

	void callUpdate() {
		static_cast<Derived *>(this)->update();
	}

public:
	void update();

	template<typename Fn>
	auto addKeyUpdateObserver(Fn &&fn){
		std::lock_guard _(lock);
		return observers.addObserver(std::forward<Fn>(fn));
	}
	void removeKeyUpdateObserver(Obs::Handle h) {
		std::lock_guard _(lock);
		observers.removeObserver(h);
	}
};


	template<typename Derived>
	json::Value UpdatableFilterView<Derived>::lookup(const std::string_view &docId) {
		callUpdate();return FilterView::lookup(docId);
	}

	template<typename Derived>
	json::Value UpdatableFilterView<Derived>::find(const std::string_view &docid) {
		callUpdate();return FilterView::find(docid);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::range(
			const std::string_view &fromKey, const std::string_view &toKey) {
		callUpdate(); return FilterView::range(fromKey, toKey);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::range(
			const std::string_view &fromKey, const std::string_view &toKey,
			bool include_upper_bound) {
		callUpdate(); return FilterView::range(fromKey, toKey, include_upper_bound);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::prefix(
			const std::string_view &prefix) {
		callUpdate(); return FilterView::prefix(prefix);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::prefix(
			const std::string_view &prefix, bool backward) {
		callUpdate(); return FilterView::prefix(prefix, backward);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::scan() {
		callUpdate(); return FilterView::scan();
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::scan(bool backward) {
		callUpdate(); return FilterView::scan(backward);
	}

	template<typename Derived>
	FilterView::Iterator UpdatableFilterView<Derived>::scan(
			const std::string_view &fromDoc, bool backward) {
		callUpdate(); return FilterView::scan(fromDoc, backward);
	}

	template<typename Derived>
	bool UpdatableFilterView<Derived>::isDocumentInView(const std::string_view &docId) {
		callUpdate(); return FilterView::isDocumentInView(docId);
	}

	template<typename Derived>
	json::Value UpdatableFilterView<Derived>::get(const std::string_view &docId) const {
		return FilterView::lookup(docId);
	}

	template<typename Derived>
	void UpdatableFilterView<Derived>::set(const std::string_view &id, json::Value value) {
		Batch b;
		set(b, id, value);
		db.commitBatch(b);
	}

	template<typename Derived>
	void UpdatableFilterView<Derived>::erase(const std::string_view &id) {
		Batch b;
		erase(b, id);
		db.commitBatch(b);
	}

	template<typename Derived>
	void UpdatableFilterView<Derived>::set(Batch &b, const std::string_view &id, json::Value value) {
		b.Put(createKey(id), createValue(value));
	}

	template<typename Derived>
	void UpdatableFilterView<Derived>::erase(Batch &b, const std::string_view &id) {
		b.Delete(createKey(id));
	}



}


#endif /* SRC_DOCDBLIB_FILTERVIEW_H_ */
