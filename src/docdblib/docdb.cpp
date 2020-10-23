/*
 * docdb.cpp
 *
 *  Created on: 20. 10. 2020
 *      Author: ondra
 */

#include <imtjson/binjson.tcc>
#include "docdb.h"

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
}

DocDB::DocDB(const std::string &path) {
	using namespace leveldb;
	DB *db;
	Options opt;
	opt.create_if_missing = true;
	LevelDBException::checkStatus(leveldb::DB::Open(opt,path,&db));
	this->db = PLevelDB(db);

	nextSeqID = findNextSeqID();
}

DocDB::DocDB(const std::string &path, const leveldb::Options &opt) {
	using namespace leveldb;
	DB *db;
	LevelDBException::checkStatus(leveldb::DB::Open(opt,path,&db));
	this->db = PLevelDB(db);
}

DocDB::~DocDB() {
	flush();
}

void DocDB::flush() {
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
/*
bool DocDB::put_impl(const Document &doc, DocRevision &rev) {
	if (pendingWrites.find(doc.id) != pendingWrites.end()) {
		flush();
	}
	std::string key(&doc_index,1);
	std::string gkey(&graveyard,1);
	std::string val;
	json::Value hdr;
	key.append(doc.id.str());
	gkey.append(doc.id.str());
	if (checkStatus_Get(db->Get({},key,&val))) {
		hdr = string2json(val);
	} else if (checkStatus_Get(db->Get({},gkey,&val))) {
		hdr = string2json(val);
	}
	json::Value revisions = hdr[0];
	SeqID seqId = hdr[1].getUIntLong();
	DocRevision lastRev = revisions[0].getUIntLong();
	if (lastRev != doc.rev) return false;

	std::hash<json::Value> hash;
	DocRevision newRev = hash(doc.content);
	if (newRev == 0) newRev = 1;
	rev = newRev;
	if (newRev == lastRev) return true;

	SeqID newSeqId = nextSeqID++;

	revisions.unshift(newRev);
	while (revisions.size()>maxRevHistory) revisions.pop();

	json::Value newHdr = {revisions,newSeqId,now()};
	val.clear();
	json2string(newHdr,val);
	json2string(doc.content, val);
	if (doc.content.defined()) {
		batch.Put(key, val);
	} else {
		batch.Delete(key);
		batch.Put(gkey, val);
	}
	val.clear(); index2string(seqId,val);
	batch.Delete(val);
	val.clear(); index2string(newSeqId,val);
	json::StrViewA docid = doc.id.str();
	batch.Put(val, leveldb::Slice(docid.data, docid.length));
	pendingWrites.insert(doc.id);
	return true;
}
*/


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

DocDB::GetResult DocDB::deserialize_impl(const std::string_view &val) {
	json::Value header = string2json(std::move(val));
	json::Value content = string2json(std::move(val));
	return {header, content};
}

void DocDB::mapSet(const std::string &key, const std::string &value) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(map_index);
	k.append(key);
	batch.Put(k,value);
	checkFlushAfterWrite();
}

bool DocDB::mapGet(const std::string &key, std::string &value) {
	flush();
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(map_index);
	k.append(key);
	return LevelDBException::checkStatus_Get(db->Get(defReadOpts, key, &value));
}

void DocDB::mapErase(const std::string &key) {
	std::string k;
	k.reserve(key.length()+1);
	k.push_back(map_index);
	k.append(key);
	batch.Delete(k);
	checkFlushAfterWrite();

}

void DocDB::purgeDoc(std::string_view &id) {
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
	auto r = deserialize_impl(data);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0]};
}

Document DocDB::deserializeDocument(const std::string_view &id, const std::string_view &data) {
	auto r = deserialize_impl(data);
	return {std::string(id), r.content,r.header[2].getUIntLong(),r.header[0][0].getUIntLong()};
}

DocIterator DocDB::scan() const {
	char end_index = doc_index+1;
	return DocIterator(db->NewIterator(iteratorReadOpts),
			std::string_view(&doc_index, 1),std::string_view(&end_index,1),false,true);

}

DocIterator DocDB::scanRange(const std::string_view &from,
		const std::string_view &to, bool exclude_end) const {

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

static void increase(std::string &x) {
	if (!x.empty())  {
		char c = x.back();
		++c;
		if (c == 0) increase(x);
		x.push_back(c);
	}
}

DocIterator DocDB::scanPrefix(const std::string_view &prefix, bool backward) const {
	std::string key1;
	key1.reserve(prefix.length()+1);
	key1.push_back(doc_index);
	key1.append(prefix);
	std::string key2(key1);
	increase(key2);
	if (backward) {
		return DocIterator(db->NewIterator(iteratorReadOpts), key2, key1, true, false);
	} else {
		return DocIterator(db->NewIterator(iteratorReadOpts), key1, key2, false, true);
	}
}

DocIterator DocDB::scanGraveyard() const {
	char end_index = graveyard+1;
	return DocIterator(db->NewIterator(iteratorReadOpts),
			std::string_view(&graveyard, 1),std::string_view(&end_index,1),false,true);

}

MapIterator DocDB::mapScan(const std::string &from, const std::string &to,
		bool exclude_end) {
	std::string key1;
	key1.reserve(from.length()+1);
	key1.push_back(map_index);
	key1.append(from);
	std::string key2;
	key2.reserve(to.length()+1);
	key2.push_back(map_index);
	key2.append(to);
	return MapIterator(db->NewIterator(iteratorReadOpts),key1, key2, false, exclude_end);

}

MapIterator DocDB::mapScanPrefix(const std::string &prefix, bool backward) {
	std::string key1;
	key1.reserve(prefix.length()+1);
	key1.push_back(map_index);
	key1.append(prefix);
	std::string key2(key1);
	increase(key2);
	if (backward) {
		return MapIterator(db->NewIterator(iteratorReadOpts), key2, key1, true,false);
	} else {
		return MapIterator(db->NewIterator(iteratorReadOpts), key1, key2, false, true);
	}
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
