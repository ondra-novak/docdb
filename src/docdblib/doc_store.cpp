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
	SeqID seq = dochdr[0].getUIntLong();
	auto docdata = incview->get(seq);
	return {docId,
		docdata[indexContent],
		docdata[indexTimestamp].getUIntLong(),
		isdel,
		docdata[indexRevisions]
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
		docdata[indexRevisions][0].getUIntLong()
	};
}

DocRevision DocStoreView::getRevision(const DocID &docId) const {
	bool isdel;
	auto dochdr = getDocHeader(docId, isdel);
	return dochdr[1].getUIntLong();
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
	return ChangesIterator(incview->scanFrom(from));
}

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
	json::Value d = getDocContent();
	return {
		id(),d[indexContent],d[indexTimestamp].getUIntLong(),d[indexDeleted].getBool(), revision()
	};
}

DocumentRepl DocStoreView::Iterator::replicate_get() const {
	json::Value d = getDocContent();
	return {
		id(),d[indexContent],d[indexTimestamp].getUIntLong(),d[indexDeleted].getBool(), d[indexRevisions]
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
	return getData()[indexRevisions][0].getUIntLong();
}

Document DocStoreView::ChangesIterator::get() const {
	auto data = getData();
	return {
		data[indexID],
		data[indexContent],
		data[indexTimestamp].getUIntLong(),
		data[indexDeleted].getBool(),
		data[indexRevisions][0].getUIntLong()
	};
}

DocumentRepl DocStoreView::ChangesIterator::replicate_get() const {
	auto data = getData();
	return {
		data[indexID],
		data[indexContent],
		data[indexTimestamp].getUIntLong(),
		data[indexDeleted].getBool(),
		data[indexRevisions]
	};
}

DocStore::DocStore(const DB &db, const std::string &name, const DocStore_Config &cfg)
	:DocStoreView(db, istore, name)
	,revHist(cfg.rev_history_length)

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

} /* namespace docdb */

