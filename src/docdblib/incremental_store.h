/*
 * incremental_store.h
 *
 *  Created on: 2. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_INCREMENTAL_STORE_H_
#define SRC_DOCDBLIB_INCREMENTAL_STORE_H_
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

	IncrementalStoreView(DB &db, const std::string_view &name);
	///Initialize view for specified snapshot
	IncrementalStoreView(const IncrementalStoreView &src, DB &snapshot);
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
		json::Value doc() const;
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
	Iterator scanFrom(SeqID from);

	///Creates global-wide key for given sequence id
	Key createKey(SeqID seqId) const;

	DB getDB() const {return db;}

protected:

	DB db;
	///keyspace id
	KeySpaceID kid;

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


	///Incremental storage's batch
	class Batch: public ::docdb::Batch {
	public:
		~Batch();
		///commits all writes and clears the batch
		void commit();
		Batch(const Batch &) = delete;
		Batch(Batch &&) = delete;

	protected:
		Batch(std::mutex &mx, DB &db);

		friend class IncrementalStore;
		std::mutex &mx;
		DB &db;
	};


	///Inicialize incrementar store
	/**
	 * @param db database object
	 * @param name name of the incremental store.
	 */
	IncrementalStore(DB &db, const std::string_view &name);

	///Destroy incremental store
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




	///Listen for changes
	/** Called from separated thread listens for changes. The function returns when
	 * change is detected or timeout ellapsed whichever comes first
	 *
	 * @param id last seen sequence id.
	 * @param period period to wait
	 * @return Function returns last sequence ID. This value should be above the first argument if
	 * there is a change. It returns same sequence id, when time ellapsed. It returns 0, when
	 * no waiting is longer possible - because the object is being destroyed. In this case, the
	 * caller must not call any function of this object.
	 *
	 * @note because function cancelListen can be used anytime, the function can return earlier then
	 * specified timeout. In this case returned value will be equal to specified sequenced ID
	 */
	template<typename _Rep, typename _Period>
	SeqID listenFor(SeqID id, const std::chrono::duration<_Rep, _Period> &period);

	///Listen for changes
	/** Called from separated thread listens for changes. The function returns when
	 * change is detected or timeout ellapsed whichever comes first
	 *
	 * @param id last seen sequence id.
	 * @param __atime absolute time of timeout
	 * @return Function returns last sequence ID. This value should be above the first argument if
	 * there is a change. It returns same sequence id, when time ellapsed. It returns 0, when
	 * no waiting is longer possible - because the object is being destroyed. In this case, the
	 * caller must not call any function of this object.
	 *
	 * @note because function cancelListen can be used anytime, the function can return earlier then
	 * specified timeout. In this case returned value will be equal to specified sequenced ID
	 *
	 */
	template<typename _Clock, typename _Duration>
	SeqID listenUntil(SeqID id, const std::chrono::time_point<_Clock, _Duration>& __atime);

	///cancels all pending listens
	void cancelListen();

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
	unsigned int spin = 1;
	unsigned int listeners = 0;

	std::mutex lock;
	std::condition_variable listen;
	std::string write_buff;

	SeqID findLastID() const;

};


template<typename _Rep, typename _Period>
inline docdb::SeqID docdb::IncrementalStore::listenFor(SeqID id, const std::chrono::duration<_Rep, _Period> &period) {
	std::unique_lock lk(lock);
	if (lastSeqId == 0) return lastSeqId;
	unsigned int s = spin;
	listeners++;
	listen.wait_for(lk, period, [&]{
		return lastSeqId != id || spin != s;
	});
	if (listeners-- == 0) listen.notify_all();
	return lastSeqId;
}

template<typename _Clock, typename _Duration>
inline docdb::SeqID docdb::IncrementalStore::listenUntil(SeqID id, const std::chrono::time_point<_Clock, _Duration> &atime) {
	std::unique_lock lk(lock);
	if (lastSeqId == 0) return lastSeqId;
	unsigned int s = spin;
	listeners++;
	listen.wait_until(lk, atime, [&]{
		return lastSeqId != id || spin != s;
	});
	if (listeners-- == 0) listen.notify_all();
	return lastSeqId;
}


}
#endif /* SRC_DOCDBLIB_INCREMENTAL_STORE_H_ */

