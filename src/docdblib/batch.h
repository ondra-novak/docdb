/*
 * batch.h
 *
 *  Created on: 8. 5. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_
#define SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_

#include <leveldb/write_batch.h>
#include "keyspace.h"
#include "callback.h"

namespace docdb {



///Container of changes applied at once (atomically)
/** This object is used internally to accumulate changes that are commited at once
 *
 * To commit the batch, you can call DB::commitBatch (which also clears content of the batch)
 *
 * Reusing the Batch instance slightly helps with performance, because it reuses
 * already allocated buffers.
 *
 * Note that Batch instance is not MT safe. It also cannot be copied, just moved
 *
 * You can install a callback handler which is called when the batch is commited (note:
 * once installed handler cannot be removed)
 *
 *
 */
class Batch  {
public:
	Batch() {}

	///Supports batch moving
	Batch(Batch &&other)
		: b(std::move(other.b))
		,cbList(std::move(cbList)) {}

	///Virtual destructor allows to implement own version of the batch instance
	virtual ~Batch() {
		cbList.broadcast(false);
	}
	///Clears content of the batch
	/**
	 * This also clears all registered callbacks
	 */
	virtual void clear() {
		b.Clear();
		cbList.broadcast(false);
		cbList.clear();
	}

	///Called when batch is finalized (commited or rejected)
	/**
	 * @param fn a function void(bool) receives true when batch was commited,
	 * 			or false when rejected. The rejection of the batch is made by
	 * 			clearing or destroying the batch without commiting. So note
	 * 			that function can be called during destruction and should
	 * 			not throw an exception
	 */
	template<typename Fn,
				typename = decltype(std::declval<Fn>()(std::declval<bool>()))>
	void onFinalize(Fn &&fn) {
		cbList.add(std::forward<Fn>(fn));
	}


	///sets batch to commited state
	/**
	 * Note function doesn't know, whether batch was really commited. This function
	 * is called by DBCore object after writes has been commit to the DB. Function calls
	 * all registered callbacks (and also clears the batch);
	 */
	virtual void commited() {
		cbList.broadcast(true);
		cbList.clear();
		b.Clear();
	}

	///Determines, whether batch is already large and should be commited
	/**
	 * When many many changes are stored to the batch, it starts to occupy a lot of memory.
	 * In such case when atomicity is not important and only benefit of the using the
	 * batch is to reduce API overhead, it would be good idea to commit the batch time to time
	 * to release some memory. This function helps to determine, whether the batch
	 * is large enough to be commited. You can control the size of large batch by tweaking
	 * largeBatch field.
	 *
	 * @retval false not large
	 * @retval true is large, you should commit changes
	 */
	bool isLarge() const {
		return b.ApproximateSize() >= largeBatch;
	}

	///allows to set threashold how large is large batch. The default value is 1MB
	static std::size_t largeBatch;

	void put(const std::string_view& key, const std::string_view& value) {
		b.Put(::leveldb::Slice(key.data(), key.size()), ::leveldb::Slice(value.data(), value.size()));
	}

	void erase(const std::string_view& key) {
		b.Delete(::leveldb::Slice(key.data(), key.size()));
	}

	auto getBatchObject() {return &b;}

	///Returns temporary buffer
	/**
	 * @return reference to a temporary buffer. The reference is valid until the Batch instance
	 * is destroyed
	 *
	 * @note there is only one temporary buffer. Each time the function is called, it
	 * clears the buffer and returns its reference. You can use it as serialization target
	 * for complex values before they are put to the database. If you need two or more
	 * buffers, you need to look elswhere
	 */
	std::string &getTmpBuffer() {
		tmp_value.clear();
		return tmp_value;
	}


	///Returns temporary key
	/**
	 * @param kid keyspace id
	 * @return reference to temporary key buffer
	 *
	 * @note there is only one temporary key buffer. If you need another, you need to look
	 * elswhere. Everytime the function is callled, content of the key is erased.
	 */
	Key &getTmpKey(KeySpaceID kid) {
		tmp_key.clear();
		tmp_key.transfer(kid);
		return tmp_key;
	}


protected:

	::leveldb::WriteBatch b;
	CallbackList<void(bool)> cbList;
	///General purpose key, can be used to store temporary key during processing
	Key tmp_key;
	///General purpose value buffer
	std::string tmp_value;
};


using GenericBatch = Batch;

}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_ */
