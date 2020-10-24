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
	virtual void operator()(const json::Value &key, const json::Value &value) = 0;
	virtual ~EmitFn() {};
};

class ViewIterator;
class ViewList;

class AbstractView {
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
	 * @return iterator
	 */
	ViewIterator find(const json::Value &key);

	///Searches for single key
	/** Because there can be multiple records for single key, result is iterator
	 *
	 * @param key key to scan
	 * @param from_doc last seen document (document is excluded). This allows to implement paging based on last seen document
	 * @return iterator
	 */
	ViewIterator find(const json::Value &key, const std::string_view &from_doc);


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
	ViewIterator scanRange(const json::Value &from, const json::Value &to);

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
	ViewIterator scanRange(const json::Value &from, const json::Value &to, const std::string_view &from_doc);


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

	///Scans for prefix
	/**
	 * @param prefix prefix to search
	 * @param backward enumerate backward
	 * @param from_doc enables to skip some document for 'from key' iterator starts by next document after this document
	 * @return iterator
	 *
	 * @note Prefix scan is defined if the part of the key is exact same. This can be achieved
	 * using strings, numbers and arrays (allows to search for all items starting by a item
	 * in the array), however objects wont work
	 */
	ViewIterator scanPrefix(const json::Value &prefix, bool backward, std::string_view from_doc);


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
	void clearUpdateDB(DocDB &updateDb) {
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
	DocDB *updateDB;

	std::string name;
	std::uint64_t viewid;

	///Function must be implemented by child class
	virtual void map(const Document &doc, const EmitFn &emit) const = 0;

	class UpdateDoc;




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

