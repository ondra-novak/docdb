/*
 * doc_store_valuemap.cpp
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#include "doc_store_map.h"

#include "formats.h"
namespace docdb {

DocStoreMap::DocStoreMap(const DB &db, const std::string_view &name, std::size_t revision, MapFn &&mapFn)
:UpdatableFilterView(db, name)
,revision(revision)
,mapFn(std::move(mapFn))
{

}

DocStoreMap::DocStoreMap(const DocStore &store, const std::string_view &name,
		std::size_t revision, MapFn &&mapFn)
:DocStoreMap(store.getDB(), name, revision, std::move(mapFn))
{
}

void DocStoreMap::setSource(const DocStore &store) {
	source = &store;
	lastSeqID = 0;
	auto md = db.keyspace_getMetadata(kid);
	if (md.hasValue()) {
		std::size_t curRevision = md[index_revision].getUIntLong();
		if (curRevision == revision) {
			SeqID seqId = md[index_sequence].getUIntLong();
			SeqID srcSeqId = store.getSeq();
			if (srcSeqId >= seqId) {
				lastSeqID = seqId;
			}
		}
	}

}

void DocStoreMap::update() {
	if (source && source->getSeq() > lastSeqID) {
		std::unique_lock _(lock);
		if (lastSeqID == 0) {
			db.keyspace_putMetadata(kid, {revision, lastSeqID});
			db.clearKeyspace(kid);
		}
		Batch b;
		for (auto iter = source->scanChanges(lastSeqID);iter.next();){
			indexDoc(b, iter.get());
			lastSeqID = iter.seqId();
			if (b.ApproximateSize()>65536) db.commitBatch(b);
		}
		db.keyspace_putMetadata(b,kid, {revision, lastSeqID});
		db.commitBatch(b);
	}

}

void DocStoreMap::mapDoc(const Document &doc) {
	std::unique_lock _(lock);
	Batch b;
	indexDoc(b, doc);
	db.commitBatch(b);
}

void DocStoreMap::clear() {
	std::unique_lock _(lock);
	lastSeqID = 0;
	db.clearKeyspace(kid);
	db.keyspace_putMetadata( kid, {revision, lastSeqID});
}

void DocStoreMap::purgeDoc(std::string_view docid) {
	std::unique_lock _(lock);
	Batch b;
	b.Delete(createKey(docid));
	db.commitBatch(b);
}

void DocStoreMap::indexDoc(Batch &emitBatch, const Document &doc) {
	auto res = mapFn(doc);
	if (res.defined()) {
		auto &buff = DB::getBuffer();
		json2string(res, buff);
		emitBatch.Put(createKey(doc.id), buff);
	} else {
		emitBatch.Delete(createKey(doc.id));
	}
	observers.broadcast(emitBatch,doc.id);

}

}



