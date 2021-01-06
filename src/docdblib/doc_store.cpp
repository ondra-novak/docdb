/*
 * doc_store.cpp
 *
 *  Created on: 26. 12. 2020
 *      Author: ondra
 */

#include "classes.h"
#include "doc_store.h"

#include <thread>

#include <imtjson/value.h>
#include <imtjson/operations.h>
#include "exception.h"
#include "formats.h"

using json::Value;
namespace docdb {

DocStoreViewBase::DocStoreViewBase(const IncrementalStoreView &incview, const std::string_view &name, bool graveyard)
:incview(incview)
{
	DB db = incview.getDB();
	kid = db.allocKeyspace(KeySpaceClass::document_index, name);
	gkid = graveyard?db.allocKeyspace(KeySpaceClass::graveyard_index, name):kid;
}


const DocStoreViewBase::DocumentHeaderData *DocStoreViewBase::DocumentHeaderData::map(const std::string_view &buffer, unsigned int &revCount) {
	revCount = buffer.size() / 8 - 1;
	return reinterpret_cast<const DocumentHeaderData *>(buffer.data());
}

const DocStoreViewBase::DocumentHeaderData* DocStoreViewBase::findDoc(
		const std::string_view &docId, unsigned int &revCount) const {
	return findDoc(incview.getDB(), docId, revCount);
}


const DocStoreViewBase::DocumentHeaderData* DocStoreViewBase::findDoc(const DB &snapshot,
		const std::string_view &docId, unsigned int &revCount) const {
	const DocumentHeaderData *hdr = nullptr;
	auto val = DB::getBuffer();
	if (!snapshot.get(Key(kid, docId), val)) {
		if (kid != gkid) {
			if (snapshot.get(Key(gkid, docId), val)) {
				hdr = DocumentHeaderData::map(val, revCount);
			}
		}
	} else {
		hdr = DocumentHeaderData::map(val, revCount);
	}
	return hdr;
}

DocStoreViewBase::Iterator DocStoreViewBase::scan() const {
	return scan(false);
}

DocStoreViewBase::Iterator DocStoreViewBase::scanRange(
		const std::string_view &from, const std::string_view &to,
		bool exclude_begin, bool exclude_end) const {

	DB snapshot = incview.getDB().getSnapshot();
	Iterator iter(IncrementalStoreView(incview, snapshot),
			snapshot.createIterator(Iterator::RangeDef{Key(kid, from), Key(kid, to), exclude_begin, exclude_end}));
	initFilter(iter);
	return iter;
}

DocStoreViewBase::Iterator DocStoreViewBase::scanPrefix(
		const std::string_view &prefix, bool backward) const {
	DB snapshot = incview.getDB().getSnapshot();
	Key b(kid, prefix);
	Iterator iter(IncrementalStoreView(incview, snapshot),
			backward?snapshot.createIterator(Iterator::RangeDef{Key::upper_bound(b),b, true, false})
					:snapshot.createIterator(Iterator::RangeDef{b, Key::upper_bound(b), false, true}));
	initFilter(iter);
	return iter;

}
DocStoreViewBase::Iterator DocStoreViewBase::scanDeleted() const {
	return scanDeleted(std::string_view());
}

DocStoreViewBase::Iterator DocStoreViewBase::scanDeleted(const std::string_view &prefix) const {
	DB snapshot = incview.getDB().getSnapshot();
	Key b(gkid,prefix);
	Iterator iter(IncrementalStoreView(incview, snapshot),
					snapshot.createIterator(Iterator::RangeDef{b, Key::upper_bound(b), false, true}));
	if (kid == gkid)
		iter.addFilter([&](const KeyView &key, const std::string_view &value){
		unsigned int dummy;
		return DocumentHeaderData::map(value, dummy)->isDeleted();
	});
	return iter;

}

DocStoreViewBase::ChangesIterator DocStoreViewBase::scanChanges(SeqID from) const {
	DB snapshot = incview.getDB().getSnapshot();
	IncrementalStoreView myinc(incview, snapshot);
	return ChangesIterator(DocStoreViewBase(*this, myinc),myinc.scanFrom(from));
}

DocStoreViewBase::DocStoreViewBase(const DocStoreViewBase &src,
		const IncrementalStoreView &incview)
:incview(incview),kid(src.kid),gkid(src.gkid)
{
}

DocRevision DocStoreViewBase::getRevision(const std::string_view &docId) const {
	unsigned int revCount;
	auto hdr = findDoc(docId, revCount);
	if (!hdr || !revCount) return 0;
	else return hdr->getRev(0);
}

DocStoreViewBase::Status DocStoreViewBase::getStatus(const std::string_view &docId) const {
	unsigned int revCount;
	auto hdr = findDoc(docId, revCount);
	if (!hdr || !revCount) return not_exists;
	return hdr->isDeleted()?deleted:exists;

}

void DocStoreViewBase::initFilter(Iterator &iter) const {
	if (kid == gkid) iter.addFilter([&](const KeyView &key, const std::string_view &value){
		unsigned int dummy;
		return !DocumentHeaderData::map(value, dummy)->isDeleted();
	});
}

DocStoreViewBase::Iterator DocStoreViewBase::scan(bool backward) const {
	return scanPrefix(std::string_view(), backward);
}

json::Value DocStoreViewBase::parseRevisions(const DocumentHeaderData *hdr, unsigned int revCount) {
	auto revisions = json::ArrayValue::create(revCount);
	for (unsigned int i = 0; i < revCount; i++) {
		Value rev(hdr->getRev(i));
		revisions->push_back(rev.getHandle());
	}
	return Value(json::PValue::staticCast(revisions));
}

DocumentRepl DocStoreViewBase::replicate_get(const std::string_view &docId) const {
	DB snapshot = incview.getDB().getSnapshot();
	IncrementalStoreView myincview(incview, snapshot);
	unsigned int revCount;
	const DocumentHeaderData *hdr = findDoc(snapshot, docId, revCount);
	if (!hdr) return DocumentRepl{std::string(docId),Value(),0,true,json::array};
	auto revisions = parseRevisions(hdr, revCount);
	SeqID seqId = hdr->getSeqID();
	bool deleted = hdr->isDeleted();

	json::Value doc = myincview.get(seqId);
	json::Value timestamp = doc[index_timestamp];
	json::Value content = doc[index_content];
	return DocumentRepl{
		std::string(docId),
		content,
		timestamp.getUIntLong(),
		deleted,
		revisions
	};


}

SeqID DocStoreViewBase::DocumentHeaderData::getSeqID() const {
	return string2index(std::string_view(seqIdDel)) >> 1;
}

bool DocStoreViewBase::DocumentHeaderData::isDeleted() const {
	return (string2index(std::string_view(seqIdDel)) & 1) != 0;
}

DocRevision DocStoreViewBase::DocumentHeaderData::getRev(unsigned int idx) const {
	return string2index(std::string_view(revList[idx]));
}

Document DocStoreViewBase::get(const std::string_view &docId) const {
	DB snapshot = incview.getDB().getSnapshot();
	IncrementalStoreView myincview(incview, snapshot);
	unsigned int revCount;
	const DocumentHeaderData *hdr = findDoc(snapshot, docId, revCount);
	if (!hdr) return Document{std::string(docId),Value(),0,true,0};

	auto rev = revCount?hdr->getRev(0):DocRevision(0);
	auto seqId = hdr->getSeqID();
	auto deleted = hdr->isDeleted();


	json::Value doc = myincview.get(seqId);
	json::Value timestamp = doc[index_timestamp];
	json::Value content = doc[index_content];
	return Document{
		std::string(docId),
		content,
		timestamp.getUIntLong(),
		deleted,
		rev
	};
}

DocStoreViewBase::Iterator::Iterator(const IncrementalStoreView &incview, Super &&src)
	:Super(std::move(src)),incview(incview) {}

std::string_view DocStoreViewBase::Iterator::id() const {
	return key().content();
}

json::Value DocStoreViewBase::Iterator::content() const {
	unsigned int revCount;
	auto hdr = DocumentHeaderData::map(value(),revCount);
	return incview.get(hdr->getSeqID())[index_content];
}


bool DocStoreViewBase::Iterator::deleted() const {
	unsigned int dummy;
	return DocumentHeaderData::map(value(),dummy)->isDeleted();
}

SeqID DocStoreViewBase::Iterator::seqId() const {
	unsigned int dummy;
	return DocumentHeaderData::map(value(),dummy)->getSeqID();
}

DocRevision DocStoreViewBase::Iterator::revision() const {
	unsigned int dummy;
	auto hdr = DocumentHeaderData::map(value(),dummy);
	if (dummy) return hdr->getRev(0); else return 0;
}

Document DocStoreViewBase::Iterator::get() const {
	unsigned int revCount;
	auto hdr = DocumentHeaderData::map(value(),revCount);
	auto docData = incview.get(hdr->getSeqID());

	return Document{
		std::string(id()),
		docData[index_content],
		docData[index_timestamp].getUInt(),
		hdr->isDeleted(),
		revCount?hdr->getRev(0):0
	};

}

DocumentRepl DocStoreViewBase::Iterator::replicate_get() const {
	unsigned int revCount;
	auto hdr = DocumentHeaderData::map(value(),revCount);
	auto docData = incview.get(hdr->getSeqID());
	auto revList = parseRevisions(hdr, revCount);

	return DocumentRepl{
		std::string(id()),
		docData[index_content],
		docData[index_timestamp].getUInt(),
		hdr->isDeleted(),
		revList
	};
}

DocStoreViewBase::ChangesIterator::ChangesIterator(const DocStoreViewBase &docStore, Super &&iter)
:Super(std::move(iter))
,docStore(docStore)
{
}

std::string DocStoreViewBase::ChangesIterator::id() const {
	return Super::doc()[index_docId].getString();
}

json::Value DocStoreViewBase::ChangesIterator::content() const {
	return Super::doc()[index_content].getString();
}

bool DocStoreViewBase::ChangesIterator::deleted() const {
	return get().deleted;
}

Document DocStoreViewBase::ChangesIterator::get() const {
	Value v = Super::doc();
	auto id = v[index_docId].getString();
	unsigned int dummy;
	auto hdr = docStore.findDoc(id, dummy);
	return Document {
		std::string(id),
		v[index_content],
		v[index_timestamp].getUInt(),
		hdr->isDeleted(),
		dummy?hdr->getRev(0):0
	};
}

SeqID DocStoreViewBase::ChangesIterator::seqId() const {
	return Super::seqId();
}

DocRevision DocStoreViewBase::ChangesIterator::revision() const {
	return get().rev;
}

DocumentRepl DocStoreViewBase::ChangesIterator::replicate_get() const {
	Value v = Super::doc();
	auto id = v[index_docId].getString();
	unsigned int dummy;
	auto hdr = docStore.findDoc(id, dummy);
	auto revList = parseRevisions(hdr, dummy);

	return DocumentRepl{
		std::string(id),
		v[index_content],
		v[index_timestamp].getUInt(),
		hdr->isDeleted(),
		revList
	};

}

DocStoreView::DocStoreView(DB &db, const std::string_view &name, const DocStore_Config &cfg)
:Super(IncrementalStoreView(db, name), name, cfg.graveyard)
{

}

DocStore::DocStore(DB &db, const std::string &name, const DocStore_Config &cfg)
:Super(IncrementalStoreView(db, name), name, cfg.graveyard)
,incstore(db,name)
{

}

bool DocStore::replicate_put(const DocumentRepl &doc) {
	unsigned int revCnt = 0;
	SeqID prevSeqID = 0;
	bool wasDel = false;

	//create batch - it also locks incremental store - only one batch can be active
	IncrementalStore::Batch b = incstore.createBatch();
	//retrieve header of current document
	const DocumentHeaderData *hdr = findDoc(doc.id, revCnt);
	//in case, that documen was found
	if (hdr != nullptr && revCnt) {
		//retrieve lastest revision from the header
		auto r = hdr->getRev(0);
		//find revision in list of the revisions
		auto p = doc.revisions.findIndex([&](const Value &x) {
			return x.getUIntLong() == r;
		});
		//if revision not found, it is conflict, because we cannot connect this document
		if (p == -1) return false;
		//if revision is same, assume the content is also same
		if (p == 0) return true;
		//retrieve previous sequence id - to connect history (if enabled)
		prevSeqID = hdr->getSeqID();
		//retrieve whether document has been deleted
		wasDel = hdr->isDeleted();
	}
	//retrieve current timestamp
	auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	//store document to the incremental store (receive new seqId)
	SeqID newSeq = incstore.put(b, {doc.id,timestamp,doc.content});
	//receive a temporary buffer
	wrbuff.clear();
	//push sequence id and deleted flag to the buffer
	Index2String_Push<8>::push((newSeq<<1) | (doc.deleted?1:0), wrbuff);
	//calculate count of revisions
	auto cnt = std::min<std::size_t>(doc.revisions.size(), revHist);
	//push all revisions to the buffer
	for (unsigned int i = 0; i < cnt; i++) Index2String_Push<8>::push(doc.revisions[i].getUIntLong(), wrbuff);
	//put document header to batch
	b.Put(Key(doc.deleted?gkid:kid, doc.id), wrbuff);
	//determine, whether it is need to delete graveyard
	//there is gravyeyard and state of deletion changed
	if (gkid != kid && wasDel != doc.deleted) {
		//delete document from the other keyspace
		b.Delete(Key(wasDel?gkid:kid));
	}
	//if history is not enabled and there is prevSeqID
	if (prevSeqID) {
		//erase prevSeqID from the incremental store
		incstore.erase(b, prevSeqID);
	}
	//commit whole batch
	b.commit();
	//unlock batch
	return true;
}

bool DocStore::put(const Document &doc) {
	unsigned int revCnt = 0;
	SeqID prevSeqID = 0;
	bool wasDel = false;
	DocRevision newRev = std::hash<json::Value>()(doc.content);

	//create batch - it also locks incremental store - only one batch can be active
	IncrementalStore::Batch b = incstore.createBatch();
	//retrieve header of current document
	const DocumentHeaderData *hdr = findDoc(doc.id, revCnt);
	//in case, that documen was found
	if (hdr != nullptr && revCnt) {
		//retrieve lastest revision from the header
		auto r = hdr->getRev(0);
		if (r != doc.rev) return false;
		//retrieve previous sequence id - to connect history (if enabled)
		prevSeqID = hdr->getSeqID();
		//retrieve whether document has been deleted
		wasDel = hdr->isDeleted();
	} else {
		if (doc.rev) return false;
	}
	//retrieve current timestamp
	auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	//store document to the incremental store (receive new seqId)
	SeqID newSeq = incstore.put(b, {doc.id,timestamp,doc.content});
	//receive a temporary buffer
	wrbuff.clear();
	//push sequence id and deleted flag to the buffer
	Index2String_Push<8>::push((newSeq<<1) | (doc.deleted?1:0), wrbuff);
	//calculate count of revisions
	auto cnt = std::min<unsigned int>(revCnt, revHist-1);
	//push new revision
	Index2String_Push<8>::push(newRev, wrbuff);
	//push other revisions - we can simply copy bytes
	wrbuff.append(reinterpret_cast<const char *>(hdr->revList), 8*cnt);
	//put document header to batch
	b.Put(Key(doc.deleted?gkid:kid, doc.id), wrbuff);
	//determine, whether it is need to delete graveyard
	//there is gravyeyard and state of deletion changed
	if (gkid != kid && wasDel != doc.deleted) {
		//delete document from the other keyspace
		b.Delete(Key(wasDel?gkid:kid));
	}
	//if history is not enabled and there is prevSeqID
	if (prevSeqID) {
		//erase prevSeqID from the incremental store
		incstore.erase(b, prevSeqID);
	}
	//commit whole batch
	b.commit();
	//unlock batch
	return true;
}


bool DocStore::erase(const std::string_view &id, const DocRevision &rev) {
	return put(Document({std::string(id), Value(), 0, true, rev}));
}

bool DocStore::purge(const std::string_view &id) {
	unsigned int revCnt = 0;
	//create batch - it also locks incremental store - only one batch can be active
	IncrementalStore::Batch b = incstore.createBatch();
	//retrieve header of current document
	const DocumentHeaderData *hdr = findDoc(id, revCnt);
	//in case, that documen was found
	if (hdr != nullptr && revCnt) {

		b.Delete(Key(hdr->isDeleted()?gkid:kid, id));

		incstore.erase(b, hdr->getSeqID());

		b.commit();

		return true;
	} else {
		return false;
	}


}

bool DocStore::purge(const std::string_view &id, const DocRevision &rev) {
	unsigned int revCnt = 0;
	//create batch - it also locks incremental store - only one batch can be active
	IncrementalStore::Batch b = incstore.createBatch();
	//retrieve header of current document
	const DocumentHeaderData *hdr = findDoc(id, revCnt);
	//in case, that documen was found
	if (hdr != nullptr && revCnt && hdr->getRev(0) == rev) {

		b.Delete(Key(hdr->isDeleted()?gkid:kid, id));

		incstore.erase(b, hdr->getSeqID());

		b.commit();

		return true;
	} else {
		return false;
	}
}

void DocStore::cancelListen() {
	incstore.cancelListen();
}

} /* namespace docdb */

