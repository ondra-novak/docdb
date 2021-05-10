/*
 * batch.h
 *
 *  Created on: 8. 5. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_
#define SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_

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
class Batch : public ::leveldb::WriteBatch {
public:
	using ::leveldb::WriteBatch::WriteBatch;

	///Supports batch moving
	Batch(Batch &&other)
		: ::leveldb::WriteBatch(std::move(other))
		,cbList(std::move(cbList)) {}

	///Virtual destructor allows to implement own version of the batch instance
	virtual ~Batch() {}
	///Clears content of the batch
	/**
	 * This also clears all registered callbacks
	 */
	virtual void Clear() {
		::leveldb::WriteBatch::Clear();
		cbList.clear();
	}

	///Register callback which is called after the commit
	/** The callback function doesn't get any arguments and doesn't return. To add
	 * some arguments, you need to put these arguments to the clousure
	 *
	 * @param fn a function void(). The function must be movable (don't need to be copyable)
	 *
	 * @note you cannot remove already registered callback. Callbacks are called once only
	 * for commit. If batch is destroyed without the commit, the callback is not called
	 */
	template<typename Fn, typename = decltype(std::declval<Fn>()())>
	void onCommit(Fn &&fn) {
		cbList.add(std::forward<Fn>(fn));
	}
	///Register callback which is called after the commit
	/** The callback function doesn't get any arguments and doesn't return. To add
	 * some arguments, you need to put these arguments to the clousure
	 *
	 * @param fn a function void(). The function must be movable (don't need to be copyable)
	 * @param cb a callback function void(size_t) which is called during callback destruction.
	 * The function is always called for rejected batch. The argument contains 1 for commited
	 * batch or 0 for rejected batch.
	 *
	 * @note you cannot remove already registered callback. Callbacks are called once only
	 * for commit. If batch is destroyed without the commit, the callback is not called
	 */
	template<typename Fn, typename DestructCallback,
				typename = decltype(std::declval<Fn>()()),
				typename = decltype(std::declval<DestructCallback>()(std::declval<std::size_t>()))>
	void onCommit(Fn &&fn, DestructCallback &cb) {
		cbList.add(std::forward<Fn>(fn),std::forward<DestructCallback>(cb));
	}

	///Called when batch is finalized
	/**
	 * @param fn a function void(bool) receives true when batch was commited, or false when rejected
	 */
	template<typename Fn,
				typename = decltype(std::declval<Fn>()(std::declval<bool>()))>
	void onFinalize(Fn &&fn) {
		cbList.add([]{},[fn=std::move(fn)](std::size_t cnt){fn(cnt != 0);});
	}


	void commited() {
		cbList.broadcast();
	}

protected:
	CallbackList<void()> cbList;
};


using GenericBatch = Batch;

}



#endif /* SRC_DOCDB_SRC_DOCDBLIB_BATCH_H_ */
