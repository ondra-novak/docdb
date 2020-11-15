/*
 * updatableobject.h
 *
 *  Created on: 1. 11. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_UPDATABLEOBJECT_H_
#define SRC_DOCDBLIB_UPDATABLEOBJECT_H_
#include <mutex>
#include <string>

#include "docdb.h"

namespace docdb {

///Base of object which performs lazy updates depends on changes (similar to replication)
class UpdatableObject {
public:
	UpdatableObject(DocDB &db);
	virtual ~UpdatableObject();

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

	///Update view by recent changes in the database
	virtual void update();


	///Update view from different DB
	/** careful with this function. Don't mix two DBs into single view.
	 *  You can put views to different DB to easy separate views and data later
	 *
	 * @param db
	 */
	virtual void update(DocDB &db);


	void rebuild();


protected:
	DocDB &db;
	SeqID seqId;
//	std::uint64_t serialNr;
	DocDB *updateDB;

//	std::string name;

	std::mutex wrlock;

	///Called after update to stare state
	virtual void storeState()  = 0;
	///Called to scan for updates
	virtual SeqID scanUpdates(ChangesIterator &&iter)  = 0;

};

}



#endif /* SRC_DOCDBLIB_UPDATABLEOBJECT_H_ */
