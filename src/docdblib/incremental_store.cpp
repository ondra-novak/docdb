/*
 * incremental_store.cpp
 *
 *  Created on: 2. 1. 2021
 *      Author: ondra
 */

#include "incremental_store.h"

#include "classes.h"
#include "formats.h"
namespace docdb {



IncrementalStoreView::IncrementalStoreView(DB db, const std::string_view &name)
	:db(db), kid(db.allocKeyspace(KeySpaceClass::incremental_store, name))
{

}

IncrementalStoreView::IncrementalStoreView(const IncrementalStoreView &src, DB snapshot)
	:db(snapshot),kid(src.kid) {

}

IncrementalStore::IncrementalStore(const DB &db, const std::string_view &name)
	:IncrementalStoreView(db, name)
	 ,lastSeqId(findLastID())
{
	this->db.keyspaceLock(kid, true);
}


IncrementalStore::~IncrementalStore() {
	this->db.keyspaceLock(kid, false);
}



SeqID IncrementalStore::put(json::Value object) {
	Batch b;
	SeqID r = put(b, object);
	db.commitBatch(b);
	return r;
}
SeqID IncrementalStore::put(Batch &b, json::Value object) {
	SeqID id = ++lastSeqId;
	std::string &buff = db.getBuffer();
	json2string(object, buff);
	b.Put(createKey(id), buff);
	observers->broadcast(b, id, object);
	return id;
}


json::Value IncrementalStoreView::get(SeqID id) const {
	auto &buff = DB::getBuffer();
	if (!db.get(createKey(id), buff)) return json::Value();
	else return string2json(buff);

}

void IncrementalStore::erase(SeqID id) {
	::docdb::Batch b;
	erase(b, id);
	db.commitBatch(b);
}

void IncrementalStore::erase(::docdb::Batch &b, SeqID id) {
	b.Delete(createKey(id));
}

IncrementalStore::Iterator IncrementalStoreView::scanFrom(SeqID from) const{
	Key k1 = createKey(from);
	Key k2(kid+1);
	return db.createIterator(Iterator::RangeDef{
		k1,k2,true,true
	});
}

json::Value IncrementalStore::Iterator::value() const {
	return string2json(::docdb::Iterator::value());
}

SeqID IncrementalStore::Iterator::seqId() const {
	auto k = key();
	return string2index(k.content());
}

Key IncrementalStoreView::createKey(SeqID seqId) const {
	Key out(kid, 8);
	Index2String_Push<8>::push(seqId, out);
	return out;
}


SeqID IncrementalStore::findLastID() const {
	Key k1(kid);
	Key k2(kid+1);
	Iterator iter(db.createIterator(Iterator::RangeDef{
		k2,k1,true,true
	}));
	if (iter.next()) {
		return iter.seqId();
	} else {
		return 1;
	}
}

}


