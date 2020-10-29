/*
 * docdb.cpp
 *
 *  Created on: 20. 10. 2020
 *      Author: ondra
 */

#include <leveldb/env.h>
#include <leveldb/comparator.h>
#include <imtjson/binjson.tcc>
#include "docdb.h"

#include <leveldb/helpers/memenv.h>
#include <cstdio>
#include "changesiterator.h"
#include "dociterator.h"
#include "exception.h"
#include "formats.h"
#include "iterator.h"

namespace docdb {

/*
 * Database sections
 *
 * 0 - changes
 * 1 - active documents
 * 2 - deleted documents
 * 3 - map
 *
 *
 * Format 0 - changes
 *
 * Key - 0,<id> - always 9 bytes
 * Value - <documentId>
 *
 * Format 1 - active documents
 *
 * Key - 1,<documentId>
 * Value - <header><data>
 *
 * <header> = [[... revision history ], seqID]
 * <data> = any arbitrary json (excluding undefined)
 *
 * Format 2 - deleted documents
 *
 * Key - 2,<documentId>
 * Value - <header>
 *
 * <header> = [[... revision history ], seqID]
 *
 * Format 3 - map
 *
 * Key - 3,<key>
 * Value - <any arbitrary binary string>
 *
 */

static leveldb::ReadOptions defReadOpts{true};
static leveldb::ReadOptions iteratorReadOpts{true,false,nullptr};


DocDB::DocDB(PLevelDB &&db):db(std::move(db)) {
	nextSeqID = findNextSeqID();
}

DocDB::DocDB(const std::string &path) {
	leveldb::Options opts;
	opts.create_if_missing = true;
	opts.max_open_files = 100;
	opts.max_file_size = 4*1024*1024;
	opts.paranoid_checks=true;
	openDB(path,opts);
}

DocDB::DocDB(const std::string &path, const leveldb::Options &opt) {
	leveldb::Options opt_copy =opt;
	openDB(path,opt_copy);
}

class DocDB::Logger: public leveldb::Logger {
public:
	Logger(DocDB &owner):owner(owner) {}
	DocDB &owner;

	virtual void Logv(const char* format, va_list ap) {
		owner.logOutput(format, ap);
	}
};

DocDB::DocDB(InMemoryEnum inMemoryEnum) {
	env = PEnv(leveldb::NewMemEnv(leveldb::Env::Default()));
	leveldb::Options opts;
	opts.env = env.get();
	opts.create_if_missing = true;
	openDB("",opts);
}

DocDB::DocDB(InMemoryEnum inMemoryEnum, const leveldb::Options &opt) {
	leveldb::Options opt_copy =opt;
	env = PEnv(leveldb::NewMemEnv(leveldb::Env::Default()));
	opt_copy.env = env.get();
	openDB("",opt_copy);
}

bool DocDB::increaseKey(std::string &x) {
	if (!x.empty())  {
		char c = x.back();
		++c;
		if (c == 0)
			if (!increaseKey(x)) return false;
		x.push_back(c);
		return true;
	} else {
		return false;
	}

}

void DocDB::logOutput(const char *, va_list ) {}

Timestamp DocDB::now() const {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

bool DocDB::erase(const std::string_view &id, DocRevision rev) {
	Document doc{std::string(id), json::Value(), 0, rev};
	return put(doc);
}

void DocDB::openDB(const std::string &path, leveldb::Options &opts) {
	opts.comparator = leveldb::BytewiseComparator();
	if (opts.info_log == nullptr) {
		logger = std::make_unique<Logger>(*this);
		opts.info_log = logger.get();
	}
	leveldb::DB *db;
	LevelDBException::checkStatus(leveldb::DB::Open(opts,path,&db));
	this->db = PLevelDB(db);

	nextSeqID = findNextSeqID();
}

DocDB::~DocDB() {
	flush();
}

void DocDB::flush() {
	std::lock_guard _(wrlock);
	if (!pendingWrites.empty()) {
		leveldb::WriteOptions opt;
		opt.sync = syncWrites;
		LevelDBException::checkStatus(db->Write(opt, &batch));
		batch.Clear();
		pendingWrites.clear();
	}
}

bool DocDB::put(const Document &doc) {
	DocRevision ignore;
	return put_impl(doc, ignore);

}

void DocDB::checkFlushAfterWrite() {
	if (batch.ApproximateSize() > flushTreshold) {
		flush();
	}
}


bool DocDB::put_impl(const Document &doc, DocRevision &rev) {
	return put_impl_t(doc,  [&rev,doc](json::Value &revisions){

		DocRevision lastRev = revisions[0].getUIntLong();
		if (lastRev != doc.rev) return 0;
		std::hash<json::Value> hash;
		DocRevision newRev = hash(doc.content);
		if (newRev == 0) newRev = 1;
		rev = newRev;
		if (newRev == lastRev) return 1;
		revisions.unshift(newRev);
		return -1;

	});
}


template<typename Fn>
bool DocDB::put_impl_t(const DocumentBase &doc,Fn &&fn) {
	std::lock_guard _(wrlock);

	if (pendingWrites.find(doc.id) != pendingWrites.end()) {
		flush();
	}
	std::string key(&doc_index,1);
	std::string val;
	json::Value hdr;
	key.append(doc.id);
	if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
		hdr = string2json(val);
	} else {
		key[0] = graveyard;
		if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
			hdr = string2json(val);
		}
	}
	json::Value revisions = hdr[0];
	SeqID seqId = hdr[1].getUIntLong();

	int r = fn(revisions);
	if (r>=0) return r == 1;

	if (revisions.size()>maxRevHistory) revisions = revisions.slice(0,maxRevHistory);
	SeqID newSeqId = nextSeqID++;
	json::Value newHdr = {revisions,newSeqId,now()};
	val.clear();
	json2string(newHdr,val);
	json2string(doc.content, val);
	if (doc.content.defined()) {
		key[0] = doc_index;
		batch.Put(key, val);
	} else {
		key[0] = doc_index;
		batch.Delete(key);
		key[0] = graveyard;
		batch.Put(key, val);
	}
	val.clear(); index2string(seqId,val);
	batch.Delete(val);
	val.clear(); index2string(newSeqId,val);
	batch.Put(val, doc.id);
	pendingWrites.insert(doc.id);
	checkFlushAfterWrite();
	return true;
}



bool DocDB::put(const DocumentRepl &doc) {
	return put_impl_t(doc, [doc]( json::Value &revisions){

		DocRevision lastRev = revisions[0].getUIntLong();

		auto revpos = doc.revisions.indexOf(lastRev);
		if (revpos == json::Value::npos) return 0;
		if (revpos == 0) return 1;

		revisions = doc.revisions;
		return -1;

	});

}

bool DocDB::put(Document &doc) {
	return put_impl(doc, doc.rev);
}

Document DocDB::get(const std::string_view &id) const {
	GetResult r = get_impl(id);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0][0].getUIntLong()};
}

DocumentRepl DocDB::replicate(const std::string_view &id) const {
	GetResult r = get_impl(id);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0]};
}

DocDB::GetResult DocDB::get_impl(const std::string_view &id) const {
	const_cast<DocDB *>(this)->flush();

	std::string key(&doc_index,1);
	std::string val;
	key.append(id);
	if (!LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
		key[0] = graveyard;
		if (!LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
			return {};
		}
	}
	return deserialize_impl(val);
}

DocDB::GetResult DocDB::deserialize_impl(std::string_view &&val) {
	json::Value header = string2json(std::move(val));
	json::Value content = string2json(std::move(val));
	return {header, content};
}

void DocDB::mapSet(const std::string_view &key, const std::string_view &value) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(0);
	k.append(key);
	mapSet_pk(std::move(k), value);
}
void DocDB::mapSet_pk(std::string &&key, const std::string_view &value) {
	std::lock_guard _(wrlock);

	key[0] = map_index;
	batch.Put(key,leveldb::Slice(value.data(), value.length()));
	checkFlushAfterWrite();
}

bool DocDB::mapGet(const std::string &key, std::string &value) {
	flush();
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(0);
	k.append(key);
	return mapGet_pk(std::move(k), value);
}
bool DocDB::mapGet_pk(std::string &&key, std::string &value) {
	key[0] = map_index;
	return LevelDBException::checkStatus_Get(db->Get(defReadOpts, key, &value));
}

void DocDB::mapErase(const std::string_view &key) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(0);
	k.append(key);
	mapErase_pk(std::move(k));
}
void DocDB::mapErase_pk(std::string &&key) {
	std::lock_guard _(wrlock);

	key[0] = map_index;
	batch.Delete(key);
	checkFlushAfterWrite();
}

void DocDB::purgeDoc(std::string_view &id) {
	std::lock_guard _(wrlock);

	if (pendingWrites.find(std::string(id)) != pendingWrites.end()) {
		flush();
	}
	std::string key(&doc_index,1);
	std::string val;
	json::Value hdr;
	key.append(id);
	if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
		hdr = string2json(val);
	} else {
		key[0] = graveyard;
		if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
			hdr = string2json(val);
		}
	}
	SeqID seq = hdr[1].getUIntLong();
	if (seq) {
		val.clear();
		index2string(seq,val);
		batch.Delete(val);
		batch.Delete(key);
	}
}

DocumentRepl DocDB::deserializeDocumentRepl(const std::string_view &id, const std::string_view &data) {
	auto r = deserialize_impl(std::string_view(data));
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0]};
}

Document DocDB::deserializeDocument(const std::string_view &id, const std::string_view &data) {
	auto r = deserialize_impl(std::string_view(data));
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0][0].getUIntLong()};
}

DocIterator DocDB::scan() const {
	const_cast<DocDB *>(this)->flush();
	char end_index = doc_index+1;
	return DocIterator(db->NewIterator(iteratorReadOpts),
			std::string_view(&doc_index, 1),std::string_view(&end_index,1),false,true);

}

DocIterator DocDB::scanRange(const std::string_view &from,
		const std::string_view &to, bool exclude_end) const {
	const_cast<DocDB *>(this)->flush();

	std::string key1;
	key1.reserve(from.length()+1);
	key1.push_back(doc_index);
	key1.append(from);
	std::string key2;
	key2.reserve(to.length()+1);
	key2.push_back(doc_index);
	key2.append(to);
	return DocIterator(db->NewIterator(iteratorReadOpts),key1, key2, false, exclude_end);
}


DocIterator DocDB::scanPrefix(const std::string_view &prefix, bool backward) const {
	const_cast<DocDB *>(this)->flush();
	std::string key1;
	key1.reserve(prefix.length()+1);
	key1.push_back(doc_index);
	key1.append(prefix);
	std::string key2(key1);
	increaseKey(key2);
	if (backward) {
		return DocIterator(db->NewIterator(iteratorReadOpts), key2, key1, true, false);
	} else {
		return DocIterator(db->NewIterator(iteratorReadOpts), key1, key2, false, true);
	}
}

DocIterator DocDB::scanGraveyard() const {
	const_cast<DocDB *>(this)->flush();
	char end_index = graveyard+1;
	return DocIterator(db->NewIterator(iteratorReadOpts),
			std::string_view(&graveyard, 1),std::string_view(&end_index,1),false,true);

}

MapIterator DocDB::mapScan(const std::string_view &from, const std::string_view &to, unsigned int exclude) {
	std::string key1;
	key1.reserve(from.length()+1);
	key1.push_back(0);
	key1.append(from);
	std::string key2;
	key2.reserve(to.length()+1);
	key2.push_back(0);
	key2.append(to);
	return mapScan_pk(std::move(key1), std::move(key2), exclude);
}

MapIterator DocDB::mapScan_pk(std::string &&from, std::string &&to, unsigned int exclude) {
	from[0] = map_index;
	to[0] = map_index;
	return MapIterator(db->NewIterator(iteratorReadOpts),from, to, (exclude & excludeBegin)!=0, (exclude & excludeEnd) != 0);

}


MapIterator DocDB::mapScanPrefix(const std::string_view &prefix, bool backward) {
	std::string key1;
	key1.reserve(prefix.length()+1);
	key1.push_back(0);
	key1.append(prefix);
	return mapScanPrefix_pk(std::move(key1), backward);
}

MapIterator DocDB::mapScanPrefix_pk(std::string &&prefix, bool backward) {
	prefix[0] = map_index;
	std::string key2(prefix);
	increaseKey(key2);
	if (backward) {
		return MapIterator(db->NewIterator(iteratorReadOpts), key2, prefix, true,false);
	} else {
		return MapIterator(db->NewIterator(iteratorReadOpts), prefix, key2, false, true);
	}

}



ChangesIterator DocDB::getChanges(SeqID fromId) const {
	const_cast<DocDB *>(this)->flush();
	std::string key;
	key.reserve(9);
	index2string(fromId+1,key);
	char end_key_mark = 1;
	std::string_view end_key(&end_key_mark, 1);
	return ChangesIterator(db->NewIterator(iteratorReadOpts), key, end_key, false, true);
}

SeqID DocDB::getLastSeqID() const {
	return nextSeqID-1;
}

void DocDB::mapErasePrefix(const std::string_view &prefix) {
	std::string key;
	key.reserve(prefix.length()+1);
	key.push_back(0);
	key.append(prefix);
}

void DocDB::mapErasePrefix_pk(std::string &&prefix) {
	prefix[0] = map_index;
	std::string key2(prefix);
	increaseKey(key2);
	Iterator iter(db->NewIterator(iteratorReadOpts), prefix, key2, true, false);
	while (iter.next()) {
		auto key = iter.key();
		batch.Delete(leveldb::Slice(key.data(), key.length()));
		checkFlushAfterWrite();
	}
}

void DocDB::mapSet(WriteBatch &batch, const std::string_view &key,const std::string_view &value) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(0);
	k.append(key);
	mapSet_pk(batch, std::move(k), value);
}

void DocDB::mapSet_pk(WriteBatch &batch, std::string &&key, const std::string_view &value) {
	key[0] = map_index;
	batch.Put(key, leveldb::Slice(value.data(), value.length()));
}

void DocDB::mapErase(WriteBatch &batch, const std::string_view &key) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(0);
	k.append(key);
	mapErase_pk(batch, std::move(k));

}

void DocDB::mapErase_pk(WriteBatch &batch, std::string &&key) {
	key[0] = map_index;
	batch.Delete(key);
}

void DocDB::flushBatch(WriteBatch &batch, bool sync) {
	db->Write({sync}, &batch);
	batch.Clear();
}


SeqID DocDB::findNextSeqID() {
	std::unique_ptr<leveldb::Iterator> iter(db->NewIterator({}));
	char section = changes_index+1;
	iter->Seek(leveldb::Slice(&section,1));
	if (iter->Valid()) {
		iter->Prev();
	}
	if (iter->Valid()) {
		auto k = iter->key();
		return string2index(std::string_view(k.data(), k.size()));
	} else {
		return 1;
	}
}

}
