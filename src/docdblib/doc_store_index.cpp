/*
 * doc_store_index.cpp
 *
 *  Created on: 6. 1. 2021
 *      Author: ondra
 */

#include "doc_store_index.h"
#include "formats.h"

namespace docdb {

DocStoreIndex::DocStoreIndex(const DB &db, const std::string_view &name,
									std::size_t revision, IndexFn &&indexFn)
:UpdatableView(db, name)
,revision(revision)
,indexFn(std::move(indexFn))

{

}

DocStoreIndex::DocStoreIndex(const DocStore &store,
		const std::string_view &name, std::size_t revision, IndexFn &&indexFn)
:DocStoreIndex(store.getDB(), name, revision, std::move(indexFn))
{
	setSource(store);
}

void DocStoreIndex::setSource(const DocStore &store) {
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


void DocStoreIndex::update() {
	if (source && source->getSeq() > lastSeqID) {
		std::unique_lock _(lock);
		if (lastSeqID == 0) {
			db.keyspace_putMetadata(kid, {revision, lastSeqID});
			db.clearKeyspace(kid);
		}
		IndexBatch ibatch;
		for (auto iter = source->scanChanges(lastSeqID);iter.next();){
			indexDoc(ibatch, iter.get());
			lastSeqID = iter.seqId();
		}
		db.keyspace_putMetadata(kid, {revision, lastSeqID});
	}
}

void DocStoreIndex::indexDoc(const Document &doc) {
	std::unique_lock _(lock);
	IndexBatch ibatch;
	indexDoc(ibatch, doc);
}

void DocStoreIndex::clear() {
	std::unique_lock _(lock);
	lastSeqID = 0;
	db.clearKeyspace(kid);
	db.keyspace_putMetadata( kid, {revision, lastSeqID});
}

void DocStoreIndex::purgeDoc(std::string_view docid) {
	indexDoc(Document{std::string(docid),json::Value(),0,true,0});
}


void DocStoreIndex::indexDoc(IndexBatch &emitBatch, const Document &doc) {

	beginIndex(doc.id, emitBatch);
	class Emit: public EmitFn {
	public:
		IndexBatch &b;
		virtual void operator()(const json::Value &key, const json::Value &value) override {
			b.emit(key, value);
		}
		Emit(IndexBatch &b):b(b) {}
	};

	Emit emit(emitBatch);
	if (!doc.deleted) indexFn(doc, emit);
	commitIndex(emitBatch);
}

}

