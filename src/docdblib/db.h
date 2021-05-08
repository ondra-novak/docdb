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
#include "classes.h"
#include "shared/refcnt.h"
#include "iterator.h"
#include "keyspace.h"
#include "observable.h"

namespace docdb {

class KeySpaceIterator;

class Batch : public ::leveldb::WriteBatch {
public:
	using ::leveldb::WriteBatch::WriteBatch;
};

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


///Abstract interface defines minimal set of function defined at core level
class DBCore: public ondra_shared:: RefCntObj {
public:

	virtual ~DBCore() {}
	///Commits a batch (and clears its content)
	/** The all writes all done through the batches. There are no other ways to write new keys.
	 *
	 * @param b batch to commit
	 */
	virtual void commitBatch(Batch &b) = 0;
	///Creates iterator over keys
	/**
	 * @param from start key
	 * @param to end key
	 * @param exclude_begin exclude starting key if matches
	 * @param exclude_end exclude ending key if matches
	 * @return iterator
	 */
	virtual Iterator createIterator(const Iterator::RangeDef &rangeDef) const = 0;
	///Retrieve value from the database
	/**
	 * @param key key to retrieve
	 * @param val the variable is filled with the value. To reduce reallocations, it is good idea to
	 * reuse single buffer to retrieve multiple values
	 * @retval true key found
	 * @retval false key not found
	 */
	virtual bool get(const Key &key, std::string &val) const = 0;
	///Creates a snapshot.
	/**
	 * Function creates a snapshot of current db. Returns new instance of this interface which
	 * represents the snapshot.
	 *
	 * @param mode defines how writes are handled in the snapshot
	 * @return snapshot
	 */
	virtual PDBCore getSnapshot(SnapshotMode mode = writeError) const = 0;
	///Compacts database
	virtual void compact() = 0;
	///Retrieves database statistics
	virtual json::Value getStats() const = 0;
	///Allocates keyspace
	/**
	 * @param class_id specified ID which identifies format of data stored in the keyspace. IDs
	 * are defined by each class which manipulates with the keyspace. This prevents to change
	 * purpose of the keyspace.
	 * @param name name of the keyspace specified by the user
	 * @return If the keyspace is already allocated, it returns its identifier. If the keyspace is
	 * not allocated, it is allocated now and new identifier is returned. Note that allocation
	 * is persistently stored in database. To release keyspace, you need to call freeKeyspace
	 */
	virtual KeySpaceID allocKeyspace(ClassID class_id, const std::string_view &name) = 0;
	///Releases keyspace
	/**
	 * Releases keyspace and deletes all data in it
	 *
	 * @param class_id class of the keyspace
	 * @param name name of the keyspace
	 * @retval true success
	 * @retval false keyspace not exists
	 */
	virtual bool freeKeyspace(ClassID class_id, const std::string_view &name) = 0;


	virtual void getApproximateSizes(const std::pair<Key,Key> *ranges, int nranges, std::uint64_t *sizes) = 0;

	virtual IObservable &getObservable(KeySpaceID kid, ObservableFactory factory) = 0;


};




///Core Database
/**
 * Main feature of the database object is to manage keyspaces, batches, and
 * iterators. To access documents, views, maps, etc, you need also create instance
 * of appropriate classes.
 */
class DB {
public:

	DB(const PDBCore &core):core(core) {}

	///Open database on path with leveldb options
	/**
	 * @param path path to the database
	 */
	DB(const std::string &path, const Config &opt = Config());
	///Commits batch (and also clears its content)
	void commitBatch(Batch &b);

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

	KeySpaceID allocKeyspace(KeySpaceClass class_id, const std::string_view &name);
	///Releases a keyspace
	/** Function also removes content of the keyspace, which can cause a lot
	 * of processing before the function returns. Ensure, that nobody is using
	 * keyspace which is being released
	 *
	 * @param id id of keyspace to free
	 */
	bool freeKeyspace(ClassID class_id, const std::string_view &name);


	///Stores any arbitrary metadata along with keyspace definition
	/**
	 * @param id id of keyspace
	 * @param data metadata to store
	 */
	void keyspace_putMetadata(KeySpaceID id, const json::Value &data) ;

	///clear whole keyspace (can take a time)
	void clearKeyspace(KeySpaceID id);


	///Stores any arbitrary metadata along with keyspace definition
	/** Data are stored in batch, which can be commited later
	 * @param b batch
	 * @param id id of keyspace
	 * @param data metadata to store
	 */
	static void keyspace_putMetadata(Batch &b, KeySpaceID id, const json::Value &data);

	///Retrieve any arbitrary metadata stored along with keyspace definition
	/**
	 * @param id keyspace identifier
	 * @return returns associated data as JSON. Returns undefined, when metadata was not yet stored
	 */
	json::Value keyspace_getMetadata(KeySpaceID id) const;


	///Create cache (to config)
	static PCache createCache(std::size_t size);

	///Creates iterator over keys
	/**
	 * @param from start key
	 * @param to end key
	 * @param exclude_begin exclude starting key if matches
	 * @param exclude_end exclude ending key if matches
	 * @return iterator
	 */
	Iterator createIterator(const Iterator::RangeDef &rangeDef) const;
	///Retrieve value from the database
	/**
	 * @param key key to retrieve
	 * @param val the variable is filled with the value. To reduce reallocations, it is good idea to
	 * reuse single buffer to retrieve multiple values
	 * @retval true key found
	 * @retval false key not found
	 */
	bool get(const Key &key, std::string &val) const;
	///Creates a snapshot.
	/**
	 * Function creates a snapshot of current db. Returns new instance of this interface which
	 * represents the snapshot.
	 *
	 * @param mode defines how writes are handled in the snapshot
	 * @return snapshot
	 */
	DB getSnapshot(SnapshotMode mode = writeError) const;
	///Retrieves database statistics
	json::Value getStats() const;

	///Enumerates all allocated keyspaces
	KeySpaceIterator listKeyspaces() const;

	///Retrieves a single buffer for reading data;
	/**
	 * There is one buffer for thread. Returned buffer is always cleared.
	 * Function is MT safe. You should not pass returned reference to different thread
	 * @return reference to local buffer
	 */
	static std::string &getBuffer();

	std::uint64_t getKeyspaceSize(KeySpaceID kod) const;

	void compact() {core->compact();}


	///Retrieves observable object for given keyspace
	template<typename X>
	X &getObservable(KeySpaceID kid) {
		return dynamic_cast<X &>(core->getObservable(kid, X::getFactory()));
	}

protected:
	PDBCore core;

};

class KeySpaceIterator: public Iterator {
public:
	KeySpaceIterator(Iterator &&iter);
	using Iterator::next;

	///Retrieves ID of keyspace
	KeySpaceID getID() const;
	///Retrieves class ID of the keyspace
	ClassID getClass() const;
	///Retrieves name of the keyspace
	std::string_view getName() const;

};


inline leveldb::Slice string2slice(json::StrViewA data) {
	return leveldb::Slice(data.data, data.length);
}

}




#endif /* SRC_DOCDB_SRC_DOCDBLIB_DB_H_ */
