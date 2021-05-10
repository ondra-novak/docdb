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

DocStoreView::DocStoreView(const DB &db, const std::string_view &name)
		:incview(&iview)
		 ,active(db, (ClassID)KeySpaceClass::document_index, name)
		 ,erased(db, (ClassID)KeySpaceClass::graveyard_index, name)
		 ,iview(db, name)
		 ,observable(active.getDB().getObservable<Obs>(active.getKID()))
{
}

DocStoreView::DocStoreView(const DB &db, IncrementalStoreView &incview, const std::string_view &name)
 	 :incview(&incview)
	 ,active(db, (ClassID)KeySpaceClass::document_index, name)
	 ,erased(db, (ClassID)KeySpaceClass::graveyard_index, name)
	 ,observable(active.getDB().getObservable<Obs>(active.getKID()))
{

}

DocStoreView::DocStoreView(const DocStoreView &other)
	:incview(&iview)
	,active(other.active)
	,erased(other.erased)
	,iview(*other.incview)
	,observable(other.observable)
	{

	}

DocStoreView::~DocStoreView() {
	if (incview == &iview) {
		iview.~IncrementalStoreView();
		incview = nullptr;
	}
}

DocStoreView::DocStoreView(const DocStoreView &source, DB snapshot)
	:incview(&iview)
	,active(source.active, snapshot)
	,erased(source.erased, snapshot)
	,iview(source.iview, snapshot)
	,observable(source.observable)

{
}

json::Value DocStoreView::getDocHeader(const DocID &docId, bool &isdel) const {
	auto dochdr = active.lookup(docId);
	isdel = false;
	if (!dochdr.defined()) {
		isdel = true;
		dochdr = erased.lookup(docId);
	}
	return dochdr;

}

DocumentRepl DocStoreView::replicate_get(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	if (!dochdr.defined()) {
		return {
			docId,
			json::undefined,
			0,
			true,
			json::undefined
		};
	}
	SeqID seq = dochdr[hdrSeq].getUIntLong();
	auto docdata = incview->get(seq);
	return {docId,
		docdata[indexContent],
		docdata[indexTimestamp].getUIntLong(),
		isdel,
		dochdr.slice(hdrRevisions)
	};
}

Document DocStoreView::get(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	if (!dochdr.defined()) {
		return {
			docId,
			json::undefined,
			0,
			true,
			0
		};
	}
	SeqID seq = dochdr[0].getUIntLong();
	auto docdata = incview->get(seq);
	return {docId,
		docdata[indexContent],
		docdata[indexTimestamp].getUIntLong(),
		isdel,
		dochdr[hdrRevisions].getUIntLong()
	};
}

DocRevision DocStoreView::getRevision(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	return dochdr[hdrRevisions].getUIntLong();
}

json::Value DocStoreView::getRevisions(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	return dochdr.slice(hdrRevisions);
}

DocStoreView::Status DocStoreView::getStatus(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	if (dochdr.defined()) {
		return isdel?deleted:exists;
	} else {
		return not_exists;
	}

}

DocStoreView DocStoreView::getSnapshot() const {
	return DocStoreView(*this, getDB().getSnapshot(SnapshotMode::writeError));
}

DocStoreView::Iterator DocStoreView::scan() const {
	return Iterator(*incview, getSnapshot().active.scan());
}

DocStoreView::Iterator DocStoreView::scan(bool backward) const {
	return Iterator(*incview, getSnapshot().active.scan(backward));
}

DocStoreView::Iterator DocStoreView::range(const DocID &from, const DocID &to, bool include_upper_bound) const {
	return Iterator(*incview, getSnapshot().active.range(from, to, include_upper_bound));
}
DocStoreView::Iterator DocStoreView::prefix(const DocID &prefix, bool backward) const {
	return Iterator(*incview, getSnapshot().active.range(prefix, backward));
}
DocStoreView::Iterator DocStoreView::scanDeleted() const {
	return Iterator(*incview, getSnapshot().erased.scan());
}
DocStoreView::Iterator DocStoreView::scanDeleted(const DocID &prefix) const {
	return Iterator(*incview, getSnapshot().erased.prefix(prefix));
}

DocStoreView::ChangesIterator DocStoreView::scanChanges(SeqID from) const {
	auto snapshot = getSnapshot();
	return ChangesIterator(snapshot,snapshot.incview->scanFrom(from));
}

DocStoreView::ChangesIterator::ChangesIterator(const DocStoreView &store, Super &&iter)
	:Super(std::move(iter)),snapshot(store) {}

json::Value DocStoreView::Iterator::id() const {
	return key();
}

bool DocStoreView::Iterator::next() {
	cache = json::undefined;
	hdr = json::undefined;
	return Super::next();
}

bool DocStoreView::Iterator::peek() {
	cache = json::undefined;
	hdr = json::undefined;
	return Super::peek();
}

const json::Value &DocStoreView::Iterator::getDocContent() const {
	if (!cache.defined()) {
		cache = incview.get(seqId());
	}
	return cache;
}

const json::Value &DocStoreView::Iterator::getDocHdr() const {
	if (!hdr.defined()) {
		hdr = value();
	}
	return hdr;
}

json::Value DocStoreView::Iterator::content() const {
	return getDocContent()[indexContent];
}

bool DocStoreView::Iterator::deleted() const {
	return getDocContent()[indexDeleted].getBool();
}

SeqID DocStoreView::Iterator::seqId() const {
	return SeqID(getDocHdr()[0].getUIntLong());
}

DocRevision DocStoreView::Iterator::revision() const {
	return SeqID(getDocHdr()[1].getUIntLong());
}

Document DocStoreView::Iterator::get() const {
	auto h = getDocHdr();
	auto d = getDocContent();
	return {
		d[indexID],d[indexContent],d[indexTimestamp].getUIntLong(),d[indexDeleted].getBool(), h[hdrRevisions].getUIntLong()
	};
}

DocumentRepl DocStoreView::Iterator::replicate_get() const {
	auto h = getDocHdr();
	auto d = getDocContent();
	return {
		d[indexID],d[indexContent],d[indexTimestamp].getUIntLong(),d[indexDeleted].getBool(), h.slice(hdrRevisions)
	};
}

DocStoreView::DocID DocStoreView::ChangesIterator::id() const {
	return getData()[indexID];
}

bool DocStoreView::ChangesIterator::next() {
	cache = json::undefined;
	return Super::next();
}

bool DocStoreView::ChangesIterator::peek() {
	cache = json::undefined;
	return Super::peek();
}

const json::Value DocStoreView::ChangesIterator::getData() const {
	if (!cache.defined()) cache = Super::value();
	return cache;
}

json::Value DocStoreView::ChangesIterator::content() const {
	return getData()[indexContent];
}

bool DocStoreView::ChangesIterator::deleted() const {
	return getData()[indexDeleted].getBool();
}

DocRevision DocStoreView::ChangesIterator::revision() const {
	return snapshot.getRevision(id());
}

Document DocStoreView::ChangesIterator::get() const {
	auto data = getData();
	return {
		data[indexID],
		data[indexContent],
		data[indexTimestamp].getUIntLong(),
		data[indexDeleted].getBool(),
		snapshot.getRevision(data[indexID])
	};
}

DocumentRepl DocStoreView::ChangesIterator::replicate_get() const {
	auto data = getData();
	return {
		data[indexID],
		data[indexContent],
		data[indexTimestamp].getUIntLong(),
		data[indexDeleted].getBool(),
		snapshot.getRevisions(data[indexID])
	};
}

DocStore::DocStore(const DB &db, const std::string &name, const DocStore_Config &cfg)
	:DocStoreView(db, istore, name)
	,revHist(cfg.rev_history_length)
	,timestampFn(cfg.timestampFn?cfg.timestampFn:defaultTimestampFn)

{
	new(&istore) IncrementalStore(db, name);
	active.getDB().keyspaceLock(active.getKID(), true);
	erased.getDB().keyspaceLock(erased.getKID(), true);
}

DocStore::~DocStore() {
	active.getDB().keyspaceLock(active.getKID(), false);
	erased.getDB().keyspaceLock(erased.getKID(), false);
	if (incview == &istore) {
		istore.~IncrementalStore();
		incview = nullptr;
	}

}



DocStoreView::Iterator::Iterator(const IncrementalStoreView &incview, Super &&src)
:JsonMapView::Iterator(std::move(src)),incview(incview)
{
}

Timestamp DocStore::defaultTimestampFn() {
	return Timestamp(
			std::chrono::duration_cast<std::chrono::milliseconds>(
					std::chrono::system_clock::now().time_since_epoch()).count());
}

void DocStore::writeHeader(Batch &b, const Value &docId, bool replace,
		bool wasdel, bool isdel, SeqID seq, Value rev) {
	Key k = active.createKey(docId);
	if (replace) {
		if (wasdel) {
			k.transfer(erased.getKID());
			if (!isdel) {
				b.erase(k);
				k.transfer(active.getKID());
			}
		} else {
			if (isdel) {
				b.erase(k);
				k.transfer(erased.getKID());
			}
		}
	} else {
		if (isdel) {
			k.transfer(erased.getKID());
		}
	}
	std::string &buff = DB::getBuffer();
	rev.unshift(seq);
	JsonMapView::createValue(rev, buff);
	b.put(k, buff);
}

bool DocStore::replicate_put(Batch &b, const DocumentRepl &doc) {
	if (!doc.id.defined()) throw DocumentIDCantBeEmpty();
	bool wasdel;
	Value hdr = getDocHeader(doc.id, wasdel);
	if (hdr.defined()) {
		SeqID seq = hdr[hdrSeq].getUIntLong();
		Value rev = hdr[hdrRevisions];
		auto pos = doc.revisions.indexOf(rev);
		if (pos == Value::npos && !wasdel) return false; //conflict
		if (pos == 0) return true; //already stored
		istore.erase(b, seq);
	}
	SeqID seq = istore.put(b, {
			doc.id,
			doc.deleted,
			doc.timestamp,
			doc.revisions,
			doc.content
	});

	writeHeader(b, doc.id, hdr.defined(), wasdel, doc.deleted, seq, doc.revisions);
	observable->broadcast(b, doc);
	return true;
}

bool DocStore::put(Batch &b, const Document &doc) {
	if (!doc.id.defined()) throw DocumentIDCantBeEmpty();
	bool wasdel;
	std::hash<Value> h;
	DocRevision newrev = h(doc.content);
	Value hdr = getDocHeader(doc.id, wasdel);
	Value revs(json::array);
	if (hdr.defined()) {
		SeqID seq = hdr[hdrSeq].getUIntLong();
		revs = hdr.slice(hdrRevisions);
		if (doc.rev != 0 || !wasdel) {
			DocRevision prevRev = revs[0].getUIntLong();
			if (doc.rev != prevRev) return false; //conflict;
			if (newrev == prevRev) return true; //no change
		}
		istore.erase(b, seq);
	} else {
		if (doc.rev != 0) return false; //conflict - revision must be 0
	}
	revs.unshift(newrev);
	if (revs.size() > revHist) {
		revs = revs.slice(0,revHist);
	}

	SeqID seq = istore.put(b, {
			doc.id,
			doc.deleted,
			timestampFn(),
			doc.content
	});

	writeHeader(b, doc.id, hdr.defined(), wasdel, doc.deleted, seq, revs);
	observable->broadcast(b, doc);
	return true;
}

bool DocStore::replicate_put(const DocumentRepl &doc) {
	Batch b(*this);
	if (!replicate_put(b, doc)) return false;
	getDB().commitBatch(b);
	return true;
}

bool DocStore::put(const Document &doc) {
	Batch b(*this);
	if (!put(b, doc)) return false;
	getDB().commitBatch(b);
	return true;
}

bool DocStore::erase(const json::Value &id, const DocRevision &rev) {
	return put({
		id, nullptr, 0, true, rev
	});
}

bool DocStore::purge(Batch &b, const json::Value &id) {
	Value hdr = erased.lookup(id);
	if (hdr.defined()) {
		SeqID id = hdr[0].getUIntLong();
		istore.erase(b, id);
		return true;
	} else {
		return false;
	}
}

bool DocStore::purge(const json::Value &id) {
	std::lock_guard _(lock);
	Batch b(*this);
	if (!purge(b, id)) return false;
	getDB().commitBatch(b);
	return true;
}

DocStore::Batch::Batch(DocStore &dst):owner(dst) {
	owner.lock.lock();
}

DocStore::Batch::~Batch() {
	owner.lock.unlock();
}


} /* namespace docdb */

