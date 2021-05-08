/*
 * incremental_store.h
 *
 *  Created on: 2. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_INCREMENTAL_STORE_H_
#define SRC_DOCDBLIB_INCREMENTAL_STORE_H_
#include <docdblib/observable.h>
#include <condition_variable>
#include <mutex>

#include "db.h"
#include "document.h"

namespace docdb {


///View to an IncrementalStore.
/** This class is view only, cannot modify content. It optimal to access data through
 * a snapshot
 */
class IncrementalStoreView {
public:

	IncrementalStoreView(DB db, const std::string_view &name);
	///Initialize view for specified snapshot
	IncrementalStoreView(const IncrementalStoreView &src, DB snapshot);
	///Retrieve document under specified sequence id
	/**
	 * @param id sequence id
* @return returns stored document. Returns undefined, if nothing is stored under this id
	 */
	json::Value get(SeqID id) const;


	///Iterates inserts
	class Iterator: public ::docdb::Iterator {
	public:
		using Super = ::docdb::Iterator;
		Iterator(::docdb::Iterator &&args):Super(std::move(args)) {}

		///Return document at given ID
		json::Value value() const;
		///Returns sequence id
		SeqID seqId() const;
	};

	///Scans for all inserts with SeqID higher then argument
	/**
	 * @param from starting sequence id. It is excluded, so function returns documents starting
	 * from number "from+1". You can pass sequence id of last seen document to list only
	 * new inserts. To iterate from the beginning, you need to pass 0.
	 * @return iterator which is able to iterate new inserts
	 */
	Iterator scanFrom(SeqID from) const;

	///Creates global-wide key for given sequence id
	Key createKey(SeqID seqId) const;

	DB getDB() const {return db;}

	///Add observer
	/**
	 * @param fn funciton, which receives three arguments (Batch &, SeqID, Value). The first argument
	 * is reference to Batch, which can be used to include additional updates to other tables. Second argument
	 * is sequence id of the newly inserted object, third argument is stored value
	 *
	 * @return handle for removing observer
	 */
	template<typename Fn>
	auto addObserver(Fn &&fn)  {
		return observers->addObserver(std::forward<Fn>(fn));
	}
	///Removes observer
	/**
	 * @param h handle returned by addUpdateObserver
	 */
	void removeObserver(AbstractObservable::Handle h) {
		observers->removeObserver(h);
	}

protected:

	DB db;
	///keyspace id
	KeySpaceID kid;

	using Obs = Observable<Batch &, SeqID, json::Value>;
	json::RefCntPtr<Obs> observers = db.getObservable<Obs>(kid);

};

///Incremental store is store, where every JSON document is stored under a single number.
/**
 * Everytime the JSON document is stored, it receives new number. The number is called sequence
 * number, SeqID. The main benefit if this store is to maintain history of inserts and wait
 * for any insert, notify other threads about inserts.
 *
 */
class IncrementalStore: public IncrementalStoreView {
public:

	///Inicialize incrementar store
	/**
	 * @param db database object
	 * @param name name of the incremental store.
	 */
	IncrementalStore(const DB &db, const std::string_view &name);

	~IncrementalStore();

	///Put document to the store
	/**
	 * @param object object to put
	 * @return sequence id of the inserted document
	 */
	SeqID put(json::Value object);

	///Put document to the store - allows to batch multiple puts into single write
	/**
	 * @param batch batch - you need to call createBatch() to obtain this object
	 * @param object JSON object to put
	 * @return sequence id of the inserted document
	 *
	 * @note You need to commit batch otherwise, nothing is written. However without commiting
	 * the batch, sequence IDs are already reserved and notification is sent.
	 */
	SeqID put(Batch &batch, json::Value object);



	///Erase specified id
	/** Erase operation is not subject of notification */
	void erase(SeqID id);

	///Erase specified id
	/**
	 * @param b batch. Can be any ordinary batch, including batch created by createBatch();
	 * @param id id to erase
	 */
	void erase(::docdb::Batch &b, SeqID id);


	///Create write batch - you can put multiple writes as single batch
	/**
	 * Note during batch is active, internal lock is held, blocking any thread waiting
	 * for changes. All threads are released once the batch is destroyed
	 *
	 * @return
	 */
	Batch createBatch();

	///Retrieves current sequence id
	SeqID getSeq() const {return lastSeqId;}

protected:

	std::atomic<SeqID> lastSeqId;

	SeqID findLastID() const;

protected:

public:


};

}
#endif /* SRC_DOCDBLIB_INCREMENTAL_STORE_H_ */

