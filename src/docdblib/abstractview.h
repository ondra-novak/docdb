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

};

class AbstractView: public IViewMap {
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

	///Update view by recent changes in the database
	void update();

	///Complete rebuild view
	void rebuild();

	///remove all items from view
	void clear();

	///Remove document from the view
	/** if you purged document from the main database, this also removes document from the view
	 *
	 * @param docid document to remove
	 */
	void purgeDoc(std::string_view docid);

	///Update view from different DB
	/** careful with this function. Don't mix two DBs into single view.
	 *  You can put views to different DB to easy separate views and data later
	 *
	 * @param db
	 */
	void update(DocDB &db);


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

	///retrieves current DB used for update content of view
	/**
	 * @return pointer to database object. The pointer can be null in case, that automatic update is disabled (you need to call update() manually)
	 */
	DocDB *getUpdateDB() const {
		return updateDB;
	}

	///Sets database used to scan for updates
	/**
	 * @param updateDb reference to database. Ensure that database is not destroed during lifetime of this view.
	 * Default value is origin database
	 *
	 * @noce careful with this feature. Never mix two databases into single view.
	 */
	void setUpdateDB(DocDB &updateDb) {
		this->updateDB = &updateDb;
	}

	///Clears updateDB and disables automatic update
	void clearUpdateDB() {
		this->updateDB = nullptr;
	}

	///Retrieve last sequence ID
	SeqID getSeqId() const {
		return seqId;
	}

	///Retrieve database object
	DocDB& getDB() const {
		return db;
	}


protected:
	DocDB &db;
	SeqID seqId;
	std::uint64_t serialNr;
	DocDB *updateDB;

	std::string name;
	std::uint64_t viewid;

	std::mutex wrlock;

	class UpdateDoc;

	void storeState();



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

	static std::uint64_t getViewKey(const std::string_view &viewName);

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



} /* namespace docdb */

#endif /* SRC_DOCDB_ABSTRACTVIEW_H_ */

