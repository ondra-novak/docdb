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

class DocStoreIndex::EmitService: public EmitFn, public Batch {
public:

	EmitService( KeySpaceID kid):kid(kid),tmpkey(kid),  tmpdockey(kid) {}

	virtual void operator ()(const json::Value &key,const json::Value &value) override;

	void setDocID(const std::string_view &docId) {
		tmpkey.clear();
		tmpval.clear();
		tmpdockey.clear();
		tmpdockey.push(0);
		tmpdockey.append(docId);
		keys.clear();
		this->docId = docId;
	}


	const KeySpaceID kid;
	std::string_view docId;
	std::vector<json::Value> keys;
	Key tmpkey, tmpdockey;
	std::string tmpval;

};


void DocStoreIndex::update() {
	if (source && source->getSeq() > lastSeqID) {
		std::unique_lock _(lock);
		if (lastSeqID == 0) {
			db.keyspace_putMetadata(kid, {revision, lastSeqID});
			db.clearKeyspace(kid);
		}
		EmitService emit(kid);
		for (auto iter = source->scanChanges(lastSeqID);iter.next();){
			indexDoc(emit, iter.get());
			db.commitBatch(emit);
			lastSeqID = iter.seqId();
		}
		db.keyspace_putMetadata(kid, {revision, lastSeqID});
	}
}

void DocStoreIndex::indexDoc(const Document &doc) {
	std::unique_lock _(lock);
	EmitService emitBatch(kid);
	indexDoc(emitBatch, doc);
	db.commitBatch(emitBatch);
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


void DocStoreIndex::indexDoc(EmitService &emitBatch, const Document &doc) {
	emitBatch.setDocID(doc.id);
	bool rowsDeleted = false;
	if (db.get(emitBatch.tmpdockey, emitBatch.tmpval)) {
		std::string_view kstr(emitBatch.tmpval);
		while (!kstr.empty()) {
			json::Value key = string2json(kstr);
			emitBatch.keys.push_back(key);
			jsonkey2string(key, emitBatch.tmpkey);
			emitBatch.tmpkey.append(doc.id);
			emitBatch.Delete(emitBatch.tmpkey);
			emitBatch.tmpkey.clear();
		}

		rowsDeleted = true;
	}
	if (!doc.deleted) {
		auto kl1 = emitBatch.keys.size();

		indexFn(doc, emitBatch);

		auto kl2 = emitBatch.keys.size();
		if (kl1 < kl2) {
			emitBatch.tmpval.clear();
			while (kl1 < kl2) {
				json2string(emitBatch.keys[kl1], emitBatch.tmpval);
				++kl1;
			}
			emitBatch.Put(emitBatch.tmpdockey, emitBatch.tmpval);
		}
	} else if (rowsDeleted) {
		emitBatch.Delete(emitBatch.tmpdockey);
	}

	if (!emitBatch.keys.empty() && !observers.empty()) {
		observers.broadcast(emitBatch, emitBatch.keys);
	}

}

}

void docdb::DocStoreIndex::EmitService::operator ()(
		const json::Value &key,
		const json::Value &value)  {
	keys.push_back(key);
	jsonkey2string(key, tmpkey);
	json2string(value, tmpval);
	tmpkey.append(docId);
	Put(tmpkey, tmpval);
	tmpkey.clear();
	tmpval.clear();
}
