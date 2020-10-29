/*
 * docdb.h
 *
 *  Created on: 20. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_DOCDB_H_
#define SRC_DOCDBLIB_DOCDB_H_

#include <unordered_set>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <exception>
#include <memory>
#include <mutex>

#include <imtjson/string.h>
#include "document.h"

namespace leveldb {
	class Env;
}
namespace docdb {

using PLevelDB = std::unique_ptr<leveldb::DB>;
using WriteBatch = leveldb::WriteBatch;
using PEnv = std::unique_ptr<leveldb::Env>;


class ChangesIterator;
class DocIterator;
class MapIterator;

enum InMemoryEnum {
	inMemory
};

class DocDB {
public:
	///Convert leveldb database
	DocDB(PLevelDB &&db);
	///Open database on path
	DocDB(const std::string &path);
	///Open database on path with leveldb options
	DocDB(const std::string &path, const leveldb::Options &opt);
	///Create database in memory
	DocDB(InMemoryEnum);
	///Create database in memory - you can adjust underlying dabase options
	DocDB(InMemoryEnum, const leveldb::Options &opt);
	///Open database on specified storage
	virtual ~DocDB();


	///Stores document
	/**
	 * @param doc document to store
	 * @retval true stored
	 * @retval false conflict
	 */
	bool put(const Document &doc);

	///Stores document
	/**
	 * @param doc document to store. Function modifies revision if the document when successfully written
	 * @retval true stored
	 * @retval false conflict
	 */
	bool put(Document &doc);

	///Stores replication item
	/**
	 * @param doc document to write
	 * @retval true stored
	 * @retval false conflict
	 */
	bool put(const DocumentRepl &doc);


	bool erase(const std::string_view &id, DocRevision rev);

	///Flushes all pending writes
	/** Documents are stored into batch, and flushed when buffer is full, or
	 * before any read. This function flushes all writes manually ensures, that
	 * everythings is written on disk
	 */
	void flush();


	///Retrieves document
	/**
	 * Function retrieves single document. Function also success, when document doesn't exists.
	 * In this case, content of document is undefined.
	 *
	 * @param id document to retrieve
	 * @return document object
	 */
	Document get(const std::string_view &id) const;


	///Retrieves document for replication
	/**
	 * @param id document to retrieve
	 * @return document object
	 */
	DocumentRepl replicate(const std::string_view &id) const;

	///Retrieves change iterator from given id
	/**
	 * @param fromId specifies SeqID of last seen change. Use 0 to start from the beginning
	 * @return iterator which can be used to receive changes written in order.
	 * Used to replicate data
	 */
	ChangesIterator getChanges(SeqID fromId) const;

	SeqID getLastSeqID() const;

	///Iterates whole database
	DocIterator scan() const;

	///Iterates all deleted documents
	/** You can iterate through tombstones if you need to purge very old tombstones */
	DocIterator scanGraveyard() const;


	///Iterates in specified range
	/**
	 * @param from start with this ID
	 * @param to end with this ID
	 * @param exclude_end don't include last item (if exists)
	 * @return iterator
	 *
	 * @note if from > to, it iterates backward
	 */
	DocIterator scanRange(const std::string_view &from, const std::string_view &to, bool exclude_end) const;

	///Iterates for given prefix
	/**
	 * @param prefix prefix
	 * @param backward set true to iterate backward
	 * @return iterator
	 */
	DocIterator scanPrefix(const std::string_view &prefix, bool backward = false) const;

	///Retrieves size of batch after flush is automatically triggered
	std::size_t getFlushTreshold() const {
		return flushTreshold;
	}

	///Changes size of batch after flush is autoatically triggered
	void setFlushTreshold(std::size_t flushTreshold = 256 * 1024) {
		this->flushTreshold = flushTreshold;
	}

	///Determines whether syncWrites is enabled
	bool isSyncWritesEnabled() const {
		return syncWrites;
	}

	///Enables or disables synchronous writes
	/** By default, this is set to true, so any write is stored immediatelly to disk
	 *  (this happens when flush() or automatic flush is invoked, otherwise, writes are
	 *  stored in memory
	 *
	 *  You can speed writes by turning off syncWrites.
	 *
	 * @param syncWrites
	 */
	void setSyncWrites(bool syncWrites ) {
		this->syncWrites = syncWrites;
	}


	///Sets item in map
	/**DocDB supports unspecified map feature, which can use database as key-value storage
	 * However, items in this storage cannot be replicated and doesn't record order. Its
	 * purpose is to enable various features, such the materialized views
	 *
	 * @param key new key to set or replace
	 * @param value new value
	 */
	void mapSet(const std::string_view &key, const std::string_view &value);

	///Works same as mapSet, however expect one extra byte at the beginning of the key, which is modified by the call
	void mapSet_pk(std::string &&key, const std::string_view &value);

	static void mapSet(WriteBatch &batch, const std::string_view &key, const std::string_view &value);

	///Works same as mapSet, however expect one extra byte at the beginning of the key, which is modified by the call
	static void mapSet_pk(WriteBatch &batch, std::string &&key, const std::string_view &value);


	///Retrieve item from map
	/**DocDB supports unspecified map feature, which can use database as key-value storage
	 * However, items in this storage cannot be replicated and doesn't record order. Its
	 * purpose is to enable various features, such the materialized views
	 *
	 * @param key new key to set or replace
	 * @param returned value
	 * @retval true found
	 * @retval false not found - content of value is not changed
	 */
	bool mapGet(const std::string &key, std::string &value);

	bool mapGet_pk(std::string &&key, std::string &value);

	///Erase item in map
	/**DocDB supports unspecified map feature, which can use database as key-value storage
	 * However, items in this storage cannot be replicated and doesn't record order. Its
	 * purpose is to enable various features, such the materialized views
	 *
	 * @param key key to erase
	 *
	 */
	void mapErase(const std::string_view &key);

	void mapErase_pk(std::string &&key);

	static void mapErase(WriteBatch &batch, const std::string_view &key);

	static void mapErase_pk(WriteBatch &batch, std::string &&key);

	void flushBatch(WriteBatch &batch, bool sync);

	static constexpr unsigned int excludeBegin = 1;
	static constexpr unsigned int excludeEnd = 2;

	///Scan map from key to key (exclusive)
	/**
	 * @param from
	 * @param to
	 * @param exclude_end don't include last item (if exists)
	 * @return map iterator
	 */
	MapIterator mapScan(const std::string_view &from, const std::string_view &to, unsigned int exclude = 0);

	MapIterator mapScan_pk(std::string &&from, std::string &&to, unsigned int exclude = 0);

	///Scan map from prefix
	/**
	 * @param prefix
	 * @param backward scan backward
	 * @return map iterator
	 */
	MapIterator mapScanPrefix(const std::string_view &prefix, bool backward);

	MapIterator mapScanPrefix_pk(std::string &&prefix, bool backward);

	///Erase all items by prefix
	void mapErasePrefix(const std::string_view &prefix);

	void mapErasePrefix_pk(std::string &&prefix);

	///Retrieves maximum revision history
	std::size_t getMaxRevHistory() const {
		return maxRevHistory;
	}

	///Changes maximum revision history
	/**
	 * Revision history is used during replication to detect changes made on
	 * other node and correctly connect the change to current revision. If there
	 * are more changes than revision history, the change is reported as conflict. Default
	 * value is 1000. Each record has 9 bytes, so complete history allocates 9kB of data when
	 * it is fully used
	 *
	 * @param maxRevHistory
	 */
	void setMaxRevHistory(std::size_t maxRevHistory = 1000) {
		this->maxRevHistory = maxRevHistory;
	}

	void purgeDoc(std::string_view &id);


	static Document deserializeDocument(const std::string_view &id, const std::string_view &data);
	static DocumentRepl deserializeDocumentRepl(const std::string_view &id, const std::string_view &data);

	///increases key by one
	/** This allows to enumerate by prefix, this calculates end of range, while begin of range is prefix itself
	 *
	 * @param key key to increase - function modifies content
	 * @retval true success
	 * @retval false unable to increase, empty key, or key is '\xFF' which cannot be increased
	 */
	static bool increaseKey(std::string &key);

protected:
	///override function to stream log messages
	virtual void logOutput(const char* format, va_list ap);
	///override function to supply own implementation of time
	virtual Timestamp now() const;

protected:

	using DocMap = std::unordered_set<std::string>;



	class Logger;

	std::unique_ptr<Logger> logger;
	PEnv env;
	PLevelDB db;
	leveldb::WriteBatch batch;
	DocMap pendingWrites;
	SeqID nextSeqID;
	std::size_t flushTreshold=256*1024;
	std::size_t maxRevHistory=1000;
	bool syncWrites = true;
	std::recursive_mutex wrlock;


	void checkFlushAfterWrite();

	static constexpr char changes_index = 0;
	static constexpr char doc_index = 1;
	static constexpr char graveyard = 2;
	static constexpr char map_index = 3;



	bool put_impl(const Document &doc, DocRevision &rev);

	template<typename Fn>
	bool put_impl_t(const DocumentBase &doc, Fn &&fn);

	struct GetResult {
		json::Value header;
		json::Value content;
	};

	GetResult get_impl(const std::string_view &id) const;
	static GetResult deserialize_impl(std::string_view &&val);


	SeqID findNextSeqID();

	void openDB(const std::string &path, leveldb::Options &opts);

};


} /* namespace docdb */

#endif /* SRC_DOCDBLIB_DOCDB_H_ */
