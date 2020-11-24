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
static leveldb::ReadOptions iteratorReadOpts{true,true,nullptr};


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

DocDB::DocDB(InMemoryEnum) {
	env = PEnv(leveldb::NewMemEnv(leveldb::Env::Default()));
	leveldb::Options opts;
	opts.env = env.get();
	opts.create_if_missing = true;
	openDB("",opts);
}

DocDB::DocDB(InMemoryEnum, const leveldb::Options &opt) {
	leveldb::Options opt_copy =opt;
	env = PEnv(leveldb::NewMemEnv(leveldb::Env::Default()));
	opt_copy.env = env.get();
	openDB("",opt_copy);
}

bool DocDB::increaseKey(std::string &x) {
	if (!x.empty())  {
		char c = x.back();
		x.pop_back();
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
	Document doc{std::string(id), json::Value(), 0, true, rev};
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
	LockEx _(wrlock);
	flush_lk();
}

void DocDB::flush_lk() {
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
		flush_lk();
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


bool DocDB::isPending(const GenKey &key) const{
	return pendingWrites.find(key) != pendingWrites.end();
}

void DocDB::flushIfPending_lk(const GenKey &key) const {
	if (isPending(key)) {
		const_cast<DocDB *>(this)->flush_lk();
	}
}

void DocDB::flushIfPending(const GenKey &key) const {
	LockSh sh(wrlock);
	if (isPending(key)) {
		sh.unlock();
		LockEx ex(wrlock);
		const_cast<DocDB *>(this)->flush_lk();
	}
}

void DocDB::markPending(const GenKey &key) {
	pendingWrites.insert(key);
}
void DocDB::markPending(GenKey &&key) {
	pendingWrites.insert(std::move(key));
}


template<typename Fn>
bool DocDB::put_impl_t(const DocumentBase &doc,Fn &&fn) {
	std::lock_guard _(wrlock);

	DocIndexKey key(doc.id);
	flushIfPending_lk(key);

	std::string val;
	json::Value hdr;
	bool wasdeleted;
	bool existed;
	if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
		hdr = string2json(val);
		wasdeleted = false;
		existed = true;
	} else {
		key[0] = graveyard;
		if (LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
			wasdeleted = true;
			existed = false;
			hdr = string2json(val);
		} else {
			existed = false;
			wasdeleted = false;
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
	if (doc.deleted) {
		if (wasdeleted) {
			key[0] = graveyard;
			batch.Delete(key);
		}
		key[0] = doc_index;
		batch.Put(key, val);
	} else {
		if (existed) {
			key[0] = doc_index;
			batch.Delete(key);
		}
		key[0] = graveyard;
		batch.Put(key, val);
	}
	val.clear(); index2string(seqId,val);
	batch.Delete(val);
	val.clear(); index2string(newSeqId,val);
	batch.Put(val, doc.id);
	key[0] = doc_index;
	markPending(std::move(key));
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
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.deleted, r.header[0][0].getUIntLong()};
}

DocumentRepl DocDB::replicate(const std::string_view &id) const {
	GetResult r = get_impl(id);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.deleted, r.header[0]};
}

DocDB::GetResult DocDB::get_impl(const std::string_view &id) const {
	DocIndexKey key(id);
	flushIfPending(key);

	std::string val;
	bool deleted = false;
	if (!LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
		key[0] = graveyard;
		if (!LevelDBException::checkStatus_Get(db->Get(defReadOpts,key,&val))) {
			return {};
		} else {
			deleted = true;
		}
	}
	return deserialize_impl(val, deleted);
}

DocDB::GetResult DocDB::deserialize_impl(std::string_view &&val, bool deleted) {
	json::Value header = string2json(std::move(val));
	json::Value content = string2json(std::move(val));
	return {header, content, deleted};
}

void DocDB::mapSet(const GenKey &key, const std::string_view &value) {
	db->Put({}, key,leveldb::Slice(value.data(), value.length()));
}

bool DocDB::mapGet(const GenKey &key, std::string &value) {
	return LevelDBException::checkStatus_Get(db->Get(defReadOpts, key, &value));
}

void DocDB::mapErase(const GenKey &key) {
	std::lock_guard _(wrlock);

	db->Delete({},key);
	checkFlushAfterWrite();
}

void DocDB::purgeDoc(std::string_view &id) {
	std::lock_guard _(wrlock);
	DocIndexKey key(id);
	flushIfPending_lk(key);

	std::string val;
	json::Value hdr;
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
		key[0] = doc_index;
		markPending(std::move(key));
		checkFlushAfterWrite();
	}
}

DocumentRepl DocDB::deserializeDocumentRepl(const std::string_view &id, const std::string_view &data) {
	auto r = deserialize_impl(std::string_view(data), false);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.deleted,r.header[0]};
}

Document DocDB::deserializeDocument(const std::string_view &id, const std::string_view &data) {
	auto r = deserialize_impl(std::string_view(data), false);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.deleted,r.header[0][0].getUIntLong()};
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
	DocIndexKey key1(from);
	DocIndexKey key2(to);
	return DocIterator(db->NewIterator(iteratorReadOpts),key1, key2, false, exclude_end);
}


DocIterator DocDB::scanPrefix(const std::string_view &prefix, bool backward) const {
	const_cast<DocDB *>(this)->flush();
	DocIndexKey key1(prefix);
	DocIndexKey key2(key1);
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

MapIterator DocDB::mapScan(const GenKey &from, const GenKey &to, unsigned int exclude) {
	//const_cast<DocDB *>(this)->flush();
	return MapIterator(db->NewIterator(iteratorReadOpts),from, to, (exclude & excludeBegin)!=0, (exclude & excludeEnd) != 0);
}

MapIterator DocDB::mapScanPrefix(const GenKey &prefix, bool backward) {
	//const_cast<DocDB *>(this)->flush();
	GenKey key2(prefix);
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

void DocDB::mapErasePrefix(const GenKey &prefix) {
	GenKey key2(prefix);
	increaseKey(key2);
	Iterator iter(db->NewIterator(iteratorReadOpts), prefix, key2, true, false);
	while (iter.next()) {
		auto key = iter.key();
		batch.Delete(leveldb::Slice(key.data(), key.length()));
		checkFlushAfterWrite();
	}
}

void DocDB::mapSet(WriteBatch &batch, const GenKey &key,const std::string_view &value) {
	batch.Put(key, leveldb::Slice(value.data(), value.length()));
}

void DocDB::mapErase(WriteBatch &batch, const GenKey &key) {
	batch.Delete(key);
}

void DocDB::flushBatch(WriteBatch &batch) {
	db->Write({this->syncWrites}, &batch);
	batch.Clear();
}

bool DocDB::mapExist(const GenKey &key) {
	std::string dummy;
	return LevelDBException::checkStatus_Get(db->Get({}, key, &dummy));
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

DocDB::GenKey::GenKey(char type) {
	push_back(type);
}

DocDB::GenKey::GenKey(char type, const char *key) {
	push_back(type); append(key);
}

DocDB::GenKey::GenKey(char type, const std::string &key) {
	push_back(type); append(key);
}


DocDB::GenKey::GenKey(char type, const std::string_view &key) {
	push_back(type); append(key);
}

DocDB::GenKey::GenKey(char type, const leveldb::Slice &key) {
	push_back(type); append(key.data(), key.size());
}

DocDB::GenKey::operator leveldb::Slice() const {
	return leveldb::Slice(data(), length());
}

void DocDB::GenKey::clear() {
	resize(1);
}

void DocDB::GenKey::set(const std::string &key) {
	resize(1); append(key);
}

void DocDB::GenKey::set(const std::string_view &key) {
	resize(1); append(key);
}

void docdb::DocDB::GenKey::set(const leveldb::Slice &key) {
	resize(1); append(key.data(), key.size());
}

}
