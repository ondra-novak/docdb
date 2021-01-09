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

	EmitService( KeySpaceID kid):tmpkey(kid),  dockey(kid) {}

	virtual void operator ()(const json::Value &key,const json::Value &value) override;

	void setDocID(const std::string_view &docId) {
		tmpkey.clear();
		buffer.clear();
		dockey.clear();
		dockey.push(0);
		dockey.append(docId);
		keys.clear();
		binkeys.clear();
		this->docId = docId;
	}


	//current document ID
	std::string_view docId;
	//list of modified keys in JSON form
	std::vector<json::Value> keys;
	//list of emited keys in binary form (jsonkey2string)
	std::string binkeys;
	//temporary key used to build key during emit - to avoid memory reallocation
	Key tmpkey;
	//document key initialized by setDocID
	Key dockey;
	//temporary buffer to store value
	std::string buffer;

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
	//indexing the document
	//1 - find whether the document is already indexed
	//2 - if indexed, remove all records from the index for the document
	//3 - call index function and store keys and values
	//4 - store list of emited keys along with document
	//5 - call observers and report modified keys

	//initialize emitBatch with new document id
	emitBatch.setDocID(doc.id);
	//this variable is set to true, when ther were deleted rows
	bool rowsDeleted = false;
	//query database whether there is already index for this document
	if (db.get(emitBatch.dockey, emitBatch.buffer)) {
		//when true, pick list of keys
		//keys are binary serialized one follows other, so we need to walk binary string and parse
		std::string_view kstr(emitBatch.buffer);
		//until empty
		while (!kstr.empty()) {
			//we need to know, where each key starts and end. So remember first pointer
			auto c1 = kstr.data();
			//deserialize key
			json::Value key = string2jsonkey(std::move(kstr));
			//remember second pointer
			auto c2 = kstr.data();
			//put key to container of updated keys
			emitBatch.keys.push_back(key);
			//pick binary version of key (using pointers) and append it to temporary key (should be cleared)
			emitBatch.tmpkey.append(std::string_view(c1, c2-c1));
			//append document id
			emitBatch.tmpkey.append(doc.id);
			//delete record
			emitBatch.Delete(emitBatch.tmpkey);
			//clear temporary key
			emitBatch.tmpkey.clear();
		}

		//mark that some rows has been deleted
		rowsDeleted = true;
	}
	//we index only not-deleted documents
	if (!doc.deleted) {
		//call index function
		//function through the emit() should put rows to the index and generate binkeys
		indexFn(doc, emitBatch);
		//so if binkeys is not empty, some records has been stored
		if (!emitBatch.binkeys.empty()) {
			//also update dockey with new list of keys
			emitBatch.Put(emitBatch.dockey, emitBatch.binkeys);
			//some records has been added, so clear this flag
			rowsDeleted = false;
		}
	}

	//if rows has been deleted (and none added)
	if (rowsDeleted) {
		//also remove dockey - document is no longer indexed
		emitBatch.Delete(emitBatch.dockey);
	}

	//if there are keys some keys collected
	if (!emitBatch.keys.empty()) {
		//broadcast the updated keys to observers (with batch)
		observers.broadcast(emitBatch, emitBatch.keys);
	}

	//all done now, called must commit the batch

}

}

void docdb::DocStoreIndex::EmitService::operator ()(
		const json::Value &key,
		const json::Value &value)  {
	//put key to keys container
	keys.push_back(key);
	//serialize key to binary form and store it to tmpkey
	jsonkey2string(key, tmpkey);
	//also store it to binkeys, which contains list of serialized keys
	binkeys.append(tmpkey.content());
	//serialize value to buffer
	json2string(value, buffer);
	//append docid to key
	tmpkey.append(docId);
	//put key and value to batch
	Put(tmpkey, buffer);
	//clear temporary key
	tmpkey.clear();
	//clear temporary buffer
	buffer.clear();
}
