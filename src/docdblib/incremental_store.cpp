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

void IncrementalStore::Batch::commit() {
	db.commitBatch(*this);
}

IncrementalStore::Batch::Batch(std::mutex &mx, DB &db)
: mx(mx),db(db)
{
	mx.lock();
}

IncrementalStore::Batch::~Batch() {
	mx.unlock();
}

IncrementalStoreView::IncrementalStoreView(DB &db, const std::string_view &name)
	:db(db), kid(db.allocKeyspace(KeySpaceClass::incremental_store, name))
{

}

IncrementalStoreView::IncrementalStoreView(const IncrementalStoreView &src, DB &snapshot)
	:db(snapshot),kid(src.kid) {

}

IncrementalStore::IncrementalStore(DB &db, const std::string_view &name)
	:IncrementalStoreView(db, name)
	 ,lastSeqId(findLastID())
{

}

IncrementalStore::~IncrementalStore() {
	std::unique_lock lk(lock);
	lastSeqId = 0;
	listen.notify_all();
	listen.wait(lk, [&]{
		return listeners == 0;
	});
}

SeqID IncrementalStore::put(json::Value object) {
	Batch b = createBatch();
	SeqID r = put(b, object);
	b.commit();
	return r;
}
SeqID IncrementalStore::put(Batch &b, json::Value object) {
	auto seqId = ++lastSeqId;
	write_buff.clear();
	json2string(object, write_buff);
	b.Put(createKey(seqId),write_buff);
	listen.notify_all();
	return seqId;
}


json::Value IncrementalStoreView::get(SeqID id) const {
	auto buff = DB::getBuffer();
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

IncrementalStore::Iterator IncrementalStoreView::scanFrom(SeqID from) {
	Key k1 = createKey(from);
	Key k2(kid+1);
	return db.createIterator(Iterator::RangeDef{
		k1,k2,true,true
	});
}

void IncrementalStore::cancelListen() {
	std::unique_lock lk(lock);
	spin++;
	listen.notify_all();
}


json::Value IncrementalStore::Iterator::doc() const {
	return string2json(value());
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

IncrementalStore::Batch IncrementalStore::createBatch() {
	return Batch(lock, db);
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


