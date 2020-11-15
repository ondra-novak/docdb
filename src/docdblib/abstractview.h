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

class IViewMap {
public:
	///Function must be implemented by child class
	virtual void map(const Document &doc, const EmitFn &emit) const = 0;
	virtual ~IViewMap() {}
};

class IReduceObserver {
public:
	virtual void updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted) = 0;
	virtual ~IReduceObserver() {}
};

class AbstractViewBase {
public:


	using ViewID = DocDB::ViewIndexKey;

	AbstractViewBase(DocDB &db, ViewID &&viewid);


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


protected:
	virtual void update() = 0;
	DocDB &db;
	ViewID viewid;

};

class AbstractView: public UpdatableObject,
					public AbstractViewBase,
					public IViewMap,
					protected IReduceObserver {

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


	virtual void update();


	void updateDoc(const Document &doc);

	void registerObserver(IReduceObserver *obs);

	void unregisterObserver(IReduceObserver *obs);

protected:
	using UpdatableObject::db;
	std::string name;
	std::uint64_t serialNr;
	ViewDocID viewdocid;

	std::mutex wrlock;
	std::vector<IReduceObserver *> reduceObservers;


	class UpdateDoc;

	virtual void storeState() override;
	virtual SeqID scanUpdates(ChangesIterator &&iter) override;
	virtual void updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted) override;


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

class ViewIterator: private MapIterator {
public:
	ViewIterator(MapIterator &&iter);

	bool next();
	json::Value key() const;
	json::Value value() const;
	const std::string_view id() const;
	using MapIterator::orig_key;
protected:
	mutable std::string_view docid;
	mutable json::Value ukey;
	mutable bool need_parse = true;
	void parseKey() const;

};

class ViewList: private MapIterator {
public:
	ViewList(MapIterator &&iter);

	using MapIterator::next;
	const std::string_view name() const;
	std::uint64_t serialNr() const;
	SeqID seqID() const;
};


class View: public AbstractView {
public:
	typedef void (*MapFn)(const Document &, const EmitFn &);
	View(DocDB &db, std::string_view name, std::uint64_t serialnr, MapFn mapFn);
protected:
	MapFn mapFn;
	virtual void map(const Document &doc, const EmitFn &emit) const override;
};

class AbstractReduceView: public AbstractViewBase, public IReduceObserver {
public:
	/**
	 * @param viewMap source map
	 * @param name name of the view
	 * @param maxGroupLevel specify maximum grouplevel of array keys for reduce.
	 * 			This can help reduce space of the reduce map if more levels are not necesery
	 * @param reduceAll generate reduceAll result (key null)
	 *
	 */
	AbstractReduceView(AbstractView &viewMap, const std::string &name, unsigned int maxGroupLevel = 99, bool reduceAll = false);
	~AbstractReduceView();

	virtual void update();

	struct KeyValue {
		std::string docId;
		json::Value key;
		json::Value value;
	};

	virtual json::Value reduce(const std::vector<KeyValue> &items, bool rereduce) const = 0;

protected:
	AbstractView &viewMap;
	std::string name;
	unsigned int maxGroupLevel;
	bool reduceAll;


	virtual void updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted);
};

class ReduceView: public AbstractReduceView {
public:
	typedef json::Value (*ReduceFn)(const std::vector<KeyValue> &items, bool rereduce);
	ReduceView(AbstractView &viewMap, const std::string &name, ReduceFn reduceFn,
					unsigned int maxGroupLevel = 99, bool reduceAll = false);
protected:
	ReduceFn reduceFn;
	virtual json::Value reduce(const std::vector<KeyValue> &items, bool rereduce) const override;

};

} /* namespace docdb */

#endif /* SRC_DOCDB_ABSTRACTVIEW_H_ */

