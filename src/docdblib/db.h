/*
 * db.h
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_DB_H_
#define SRC_DOCDB_SRC_DOCDBLIB_DB_H_
#include <memory>
#include <string>
#include "dbconfig.h"
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <mutex>

#include <imtjson/value.h>
#include "shared/refcnt.h"
#include "iterator.h"
#include "keyspace.h"

namespace docdb {

class KeySpaceIterator;

///Contains information about keyspace
struct KeySpaceInfo {
	///identifier
	KeySpaceID id;
	///which class allocated this keyspace
	ClassID class_id;
	///name of the keyspace
	std::string name;
	///metadata
	json::Value metadata;
};

using Batch = leveldb::WriteBatch;

enum SnapshotMode {
	///write to the snapshot causes an exception
	writeError,
	///write to the snapshot is silently ignored - no exception, written data are lost
	writeIgnore,
	///all writes are forwarded to live database (but will not appear in snapshot)
	writeForward

};

class DBCore;
using PDBCore = ondra_shared::RefCntPtr<DBCore>;

class DBCore: public ondra_shared:: RefCntObj {
public:

	virtual ~DBCore();
	virtual void commitBatch(Batch &b) = 0;
	virtual Iterator createIterator(const Key &from, const Key &to, bool exclude_begin, bool exclude_end) const = 0;
	virtual std::optional<std::string> get(const Key &key) const = 0;
	virtual PDBCore getSnapshot(SnapshotMode mode = writeError) = 0;
	virtual void compact() = 0;
	virtual json::Value getStats() const = 0;

};




///Core Database
/**
 * Main feature of the database object is to manage keyspaces, batches, and
 * iterators. To access documents, views, maps, etc, you need also create instance
 * of appropriate classes.
 */
class DB: public ondra_shared::RefCntObj {
public:


	///Open database on path with leveldb options
	/**
	 * @param path path to the database
	 */
	DB(const std::string &path, const Config &opt = Config());

	///Allocate a keyspace. It is identified by its class_id and the name
	/**
	 * @param class_id class id - each database class knows its class_id
	 * @param name any arbitrary name of the key space
	 * @return id of allocated keyspace - you need to use this result to create a Key
	 * object
	 *
	 * @note if the keyspace already exists, its identifier is returned. Note that
	 * there is limited count of keyspaces (255). When there is no available keyspace,
	 * the function throws an exception. If you no longer need a keyspace, you
	 * need to free it (freeKeyspace).
	 *
	 * @exception TooManyKeyspaces No more keyspaces can be allocated
	 */
	KeySpaceID allocKeyspace(ClassID class_id, const std::string_view &name);
	///Releases a keyspace
	/** Function also removes content of the keyspace, which can cause a lot
	 * of processing before the function returns. Ensure, that nobody is using
	 * keyspace which is being released
	 *
	 * @param id id of keyspace to free
	 */
	void freeKeyspace(KeySpaceID id);

	///Enumerates all allocated keyspaces
	KeySpaceIterator listKeyspaces() const;

	///Retrieves information about specified keyspace
	/**
	 * @param id id of keyspace to retrieve
	 * @return Returns informations, when keyspace is allocated, or empty result, if not
	 */
	std::optional<KeySpaceInfo> getKeyspaceInfo(KeySpaceID id) const;
	///Retrieves information about specified keyspace
	/**
	 *
	 * @param class_id class id - each database class knows its class_id
	 * @param name any arbitrary name of the key space
	 * @return Returns informations, when keyspace is allocated, or empty result, if not
	 */
	std::optional<KeySpaceInfo> getKeyspaceInfo(ClassID class_id, const std::string_view &name) const;


	///Stores any arbitrary metadata along with keyspace definition
	/**
	 * @param id id of keyspace
	 * @param data metadata to store
	 */
	void storeKeyspaceMetadata(KeySpaceID id, const json::Value &data) ;

	///Stores any arbitrary metadata along with keyspace definition
	/** Data are stored in batch, which can be commited later
	 * @param b batch
	 * @param id id of keyspace
	 * @param data metadata to store
	 */
	void storeKeyspaceMetadata(Batch &b, KeySpaceID id, const json::Value &data) const;

	///Commits batch (and also clears its content)
	void commitBatch(Batch &b);

	///Create cache (to config)
	static PCache createCache(std::size_t size);

	///Create iterator
	/**
	 * @param from begin of iteration
	 * @param to end of iteration
	 * @param exclude_begin set true if you need to skip begin of the range
	 * @param exclude_end set true if you need to stop iterating before end of the range
	 * @return iterator
	 */
	Iterator createIterator(const Key &from, const Key &to, bool exclude_begin, bool exclude_end) const;

	///Get value at specified key
	std::optional<std::string> get(const Key &key) const;

	///Create snapshot from current DB state
	PDBCore createSnapshot();


	operator PDBCore() const;

protected:
	PDBCore core;

	std::mutex lock;

	static constexpr KeySpaceID keyspaceManager = ~KeySpaceID(0);
	KeySpaceID free_ks = 0;

	static Key getKey(ClassID class_id, const std::string_view &name);
	static Key getKey(KeySpaceID id);
};

class KeySpaceIterator: public Iterator {
public:
	KeySpaceIterator(Iterator &&iter);
	using Iterator::next;

	KeySpaceInfo getInfo() const;
	KeySpaceID getID() const;
protected:


};



}




#endif /* SRC_DOCDB_SRC_DOCDBLIB_DB_H_ */
