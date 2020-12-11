/*
 * abstractview.h
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_ABSTRACTVIEW_H_
#define SRC_DOCDB_ABSTRACTVIEW_H_
#include "docdb.h"
#include "iterator.h"
#include "updatableobject.h"

namespace docdb {

///Function defined for View to emit data for index
class EmitFn {
public:
	///Call this function to emit key and value
	/**
	 * @param key key
	 * @param value value
	 */
	virtual void operator()(const json::Value &key, const json::Value &value) const = 0;
	virtual ~EmitFn() {};
};

class ViewIterator;
class ViewList;

///Basic interface which descibes minimal implemenation for full materialized map
class IViewMap {
public:
	///Function must be implemented by child class
	/**
	 * @param doc document to map
	 * @param emit function used to emit key-value records from the document
	 *
	 * @note Function must emit unique keys. Duplicated keys are discarded.
	 * However multiple dokuments can emit duplicate keys (which means, there
	 * can be max same amount duplicates as indexed documents)
	 */
	virtual void map(const Document &doc, const EmitFn &emit) const = 0;
	virtual ~IViewMap() {}
};

class IKeyUpdateObservable;

///Interface intended to observer updated keys
/**
 * Important part of materialized derived views is to observer keys changed (updated)
 * in source view to allow recalculation of derived view. Every derived view must implement
 * this interface
 */
class IKeyUpdateObserver {
public:
	///Called when a keys are updated in source view
	/**
	 * @param batch current write batch - if the view is in the same db, it should
	 * use this batch to store updates
	 * @param Contains updated key.
	 * @param deleted the field is set to true, if the key has been deleted. Note that
	 * function can be called twice, first to delete keys, second to create them again (or different)
	 */
	virtual void updatedKey(WriteBatch &batch, const json::Value &key, bool deleted) = 0;
	///Called when the destructor of the observable object finds still registered observers
	/**
	 * @param obs pointer to observable object which is being destroyed. This observer
	 * can perform any action knowing that observable source is no loger exists
	 */
	virtual void release(const IKeyUpdateObservable *obs) = 0;
	virtual ~IKeyUpdateObserver() {}
};

///Interface which specified functions to register to observe updated keys
class IKeyUpdateObservable {
public:
	///Register the observer
	/**
	 * @param obs pointer to observer
	 */
	virtual void addObserver(IKeyUpdateObserver *obs) = 0;
	///Unregister the observer
	/**
	 * @param obs pointer to observer to unregister
	 *
	 * @note if update is done in the different thread, there still can be pending
	 * update which can arrive after removal. Watch for it if you destroying
	 * the observer (handle locks properly).
	 */
	virtual void removeObserver(IKeyUpdateObserver *obs) = 0;

	///Just virtual destructor
	virtual ~IKeyUpdateObservable() {}
};

///Minimal implementation of observable object which registers and removes observers
class KeyUpdateObservableImpl: public IKeyUpdateObservable {
public:

	virtual void addObserver(IKeyUpdateObserver *obs) override {
		observers.push_back(obs);
	}

	virtual void removeObserver(IKeyUpdateObserver *obs) override {
		auto iter = std::remove(observers.begin(),observers.end(),obs);
		observers.erase(iter, observers.end());
	}

	void updatedKey(WriteBatch &batch, const json::Value &key, bool deleted) {
		for (auto &&x : observers) x->updatedKey(batch, key, deleted);
	}

	bool empty() const {return observers.empty();}
	~KeyUpdateObservableImpl() {
		std::vector<IKeyUpdateObserver *> list;
		std::swap(observers, list);
		for (const auto &x : list) x->release(this);
	}
protected:
	std::vector<IKeyUpdateObserver *> observers;


};

///Implements view above existing data. Doesn't support update of the view
/** Object can be used to explore existing view, without ability to update the
 * view. However, this doesn't act as snapshot, if there is other way to update
 * the view, all the updates are visible in this view.
 *
 * The class is used as base class of all other views. This defines common
 * interface for any access to the view
 */
class StaticView {
public:


	using ViewID = DocDB::ViewIndexKey;

	StaticView(DocDB &db, ViewID &&viewid);


	///Searches for single key
	/** Because there can be multiple records for single key, result is iterator
	 *
	 * @param key key to scan
	 * @param backward show results in descending order
	 * @return iterator
	 */
	ViewIterator find(const json::Value &key, bool backward = false);

	///Searches for single key
	/** Because there can be multiple records for single key, result is iterator
	 *
	 * @param key key to scan
	 * @param from_doc last seen document (document is excluded). This allows to implement paging based on last seen document
	 * @param backward show results in descending order
	 * @return iterator
	 */
	ViewIterator find(const json::Value &key, const std::string_view &from_doc, bool backward = false);


	///Perform fast lookup for a value
	/**
	 * @param key key to lookup
	 * @return found value. If key doesn't exists, returns undefined value. If there are multiple results, it selects only one.
	 */
	json::Value lookup(const json::Value &key);

	///Scans whole view
	ViewIterator scan();

	///Scan for range
	/**
	 * @param from from key
	 * @param to to key
	 * @return iterator
	 *
	 * @note ordering is not strictly defined. Keys based on numbers are ordered, strings
	 * are ordered using binary ordering, but other json objects can have any arbitrary order.
	 */
	ViewIterator scanRange(const json::Value &from, const json::Value &to, bool exclude_end);

	///Scan for range
	/**
	 * @param from from key
	 * @param to to key
	 * @param from_doc enables to skip some document for 'from key' iterator starts by next document after this document
	 * @return iterator
	 *
	 * @note ordering is not strictly defined. Keys based on numbers are ordered, strings
	 * are ordered using binary ordering, but other json objects can have any arbitrary order.
	 */
	ViewIterator scanRange(const json::Value &from, const json::Value &to, const std::string_view &from_doc, bool exclude_end);


	///Scans for prefix
	/**
	 * @param prefix prefix to search
	 * @param backward enumerate backward
	 * @return iterator
	 *
	 * @note Prefix scan is defined if the part of the key is exact same. This can be achieved
	 * using strings, numbers and arrays (allows to search for all items starting by a item
	 * in the array), however objects wont work
	 */
	ViewIterator scanPrefix(const json::Value &prefix, bool backward);

	///Scans view from given key, returns iterator which will iterate rest of the view
	/**
	 * @param key key to start. If the key doesn't exist, it starts by first item in selected direction
	 * @param backward set true to scan backward. In backward scanning, all record of starting key are also included (in backward order)
	 * @return iterator
	 */
	ViewIterator scanFrom(const json::Value &key, bool backward);

	///Scans view for given key, returns iterator which will iterate rest of the view
	/**
	 * @param key key to start. If the key doesn't exist, it starts by first item in selected direction
	 * @param backward set true to scan backward
	 * @param from_doc specify exact starting point by document id. The document must be one of records
	 *    that belongs to the starting key. The document is excluded from the result.
	 * @return iterator
	 */
	ViewIterator scanFrom(const json::Value &key, bool backward, const std::string &from_doc);

	DocDB &getDB() const {return db;}

protected:
	///Derived class need to implement this function to update view when it is needed
	/** Function is called everytime the view is accessed. The derived
	 * class can perform update operation before the view is searched
	 */
	virtual void update() {}

	DocDB &db;

	ViewID viewid;

};

///Abstract updatable view - expects that view is updated somehow, however, it doesn't define how.
/** This class inherits StaticView and IKeyUpdateObservable. This allows to observer
 * the view for changes. However, there is still no way how to update the view
 *
 * However, the function update is now mandatory and must be implemented by a derived class
 */
class AbstractUpdatableView: public StaticView,
							 public IKeyUpdateObservable {
public:
	virtual void update() = 0;
	using StaticView::StaticView;
};

///Abstract view - the basic building block for materialized views.
/** Basic view is updated directly from database. If there is any
 * update in underlying database, the view perform incremental indexing (calls
 * map function for newly created or updated documents).
 *
 * All modified keys are distributed through IKeyUpodateObserver to derived views
 *
 */
class AbstractView: public UpdatableObject,
					public AbstractUpdatableView,
					public IViewMap
					{

	using ViewDocID = DocDB::ViewDocIndexKey;

public:

	///Initialize view
	/**
	 * Initialize or create view
	 *
	 * @param db database object
 	 * @param name name of the view. Name must be unique
	 * @param serial_nr serial number of the view. If serial number doesn't match the stored
	 * serial number, the content of view is deleted and the view is rebuilded
	 *
	 * Second and third argument is in most cases filled by class which implements this view.
	 * There is no reason to pass these arguments to public interface, because the name and
	 * the serial number is tied to implementation
	 *
	 */
	AbstractView(DocDB &db, std::string &&name, uint64_t serial_nr);

	virtual ~AbstractView() {}

	///remove all items from view
	/** Note : operation is not atomical - while content of view is deleted you can still browse it */
	void clear();

	///Remove document from the view
	/** if you purged document from the main database, this also removes document from the view
	 *
	 * @param docid document to remove
	 */
	void purgeDoc(std::string_view docid);

	///Enforce update
	/** Note update is protected by mutex, only one thread can perform update
	 * During update, search is not possible. If you need to search in not
	 * yet updated view, you need to create StaticView over it and use it
	 * to search while the view is being updated
	 *
	 */
	virtual void update();

	///Enforce of update of single document
	/** this allows to feed the view with own documents regardless on underlying
	 * database
	 * @param doc
	 */
	void updateDoc(const Document &doc);

	///add IKeyUpdateObserver
	virtual void addObserver(IKeyUpdateObserver *obs) override;
	///remove IKeyUpdateObserver
	virtual void removeObserver(IKeyUpdateObserver *obs)override;

protected:
	using UpdatableObject::db;
	std::string name;
	std::uint64_t serialNr;
	ViewDocID viewdocid;

	std::mutex wrlock;
	KeyUpdateObservableImpl keylistener;

	class UpdateDoc;

	virtual void storeState() override;
	virtual SeqID scanUpdates(ChangesIterator &&iter) override;
	void updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted);

};

class ViewTools {
public:
	ViewTools(DocDB &db);

	///List all initialized views
	ViewList list();

	///Complete erase the selected view
	/**
	 * @param name name of the view to erase.
	 *
	 * @note ensure, that view is not opened, otherwise, the result can be undefined
	 */
	void erase(const std::string_view &name);

	struct State {
		std::uint64_t serialNr;
		SeqID seq;
	};

	///Reads view state (this function is called by AbstractView during initialization)
	State getViewState(const std::string_view &viewName) const;
	///Updates view state (this function is called by AbstractView after update
	void setViewState(const std::string_view &viewName, const State &state);

	static std::string getViewID(const std::string_view &viewName);

protected:
	DocDB &db;

};

///Iterates over results returned by a search operation of the view
class ViewIterator: private MapIterator {
public:
	///Don't call directly, you don't need to create iterator manually
	ViewIterator(MapIterator &&iter);

	///Prepares next result
	/**You need to call this as the very first operation of the iterator */
	bool next();
	///Retrieves curreny key
	json::Value key() const;
	///Retrieves curreny value
	json::Value value() const;
	///Retrieves documeny id - source of this record
	const std::string_view id() const;
	using MapIterator::orig_key;
	using MapIterator::empty;
protected:
	mutable std::string_view docid;
	mutable json::Value ukey;
	mutable bool need_parse = true;
	void parseKey() const;

};

///Iterates through list of views. To access this iterator, use ViewTools::list()
class ViewList: private MapIterator {
public:
	ViewList(MapIterator &&iter);

	using MapIterator::next;
	const std::string_view name() const;
	std::uint64_t serialNr() const;
	SeqID seqID() const;
};


///Helps to construct instance using single map function specified in the constructor
class View: public AbstractView {
public:
	typedef void (*MapFn)(const Document &, const EmitFn &);
	///Constructor
	/**
	 * @param db database object, where the view resides
	 * @param name name of the view
	 * @param serialnr serial number - The number is stored and compared. If the
	 *   there is difference, the content of the view is invalidated, and the
	 *   view is rebuilt complete (which can take a long time)
	 * @param mapFn function which maps documents to the index
	 */
	View(DocDB &db, std::string_view name, std::uint64_t serialnr, MapFn mapFn);
protected:
	MapFn mapFn;
	virtual void map(const Document &doc, const EmitFn &emit) const override;
};



} /* namespace docdb */

#endif /* SRC_DOCDB_ABSTRACTVIEW_H_ */

