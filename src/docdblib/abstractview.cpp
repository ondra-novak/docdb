/*
 * abstractview.cpp
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#include "abstractview.h"
#include "formats.h"
#include <imtjson/binjson.tcc>
#include "../imtjson/src/imtjson/fnv.h"
#include "changesiterator.h"

namespace docdb {

class ViewName: public std::string {
public:
	ViewName(const std::string_view &viewName) {
		reserve(viewName.length() + 2);
		push_back(0);
		push_back(0);
		append(viewName);
	}
};


ViewTools::State ViewTools::getViewState(const std::string_view &viewName) const {
	ViewName key(viewName);
	std::string val;
	if (db.mapGet_pk(std::move(key), val)) {
		std::uint64_t serialNr = string2index(std::string_view(val).substr(0,8));
		SeqID seqID = string2index(std::string_view(val).substr(8,8));
		return State{serialNr, seqID};
	} else {
		return State{0,0};
	}
}


void ViewTools::setViewState(const std::string_view &viewName, const State &state) {
	ViewName key(viewName);
	std::string val;
	Index2String_Push<8>::push(state.serialNr, val);
	Index2String_Push<8>::push(state.seq, val);
	db.mapSet_pk(std::move(key), val);
}

void ViewTools::erase(const std::string_view &name) {
	std::string tmp;
	index2string(getViewKey(name),tmp);
	db.mapErasePrefix_pk(std::move(tmp));
	ViewName key(name);
	db.mapErase_pk(std::move(key));
}

ViewTools::ViewTools(DocDB &db):db(db) {
}

ViewList docdb::ViewTools::list() {
	char zero = 0;
	MapIterator iter = db.mapScanPrefix(std::string_view(&zero,1), false);
	return ViewList(std::move(iter));
}

ViewList::ViewList(MapIterator &&iter):MapIterator(std::move(iter)) {
}

const std::string_view ViewList::name() const {
	return MapIterator::key().substr(1);
}

std::uint64_t ViewList::serialNr() const {
	return string2index(MapIterator::value().substr(0,8));
}

SeqID ViewList::seqID() const {
	return string2index(MapIterator::value().substr(8,8));
}

ViewIterator::ViewIterator(MapIterator &&iter):MapIterator(std::move(iter)) {

}

json::Value ViewIterator::key() const {
	if (need_parse) parseKey();
	return ukey;
}

bool ViewIterator::next() {
	need_parse = true;
	return MapIterator::next();
}


json::Value ViewIterator::value() const {
	auto val = MapIterator::value();
	std::size_t i = 0;
	return json::Value::parseBinary([&]{
		return val[i++];
	});
}

const std::string_view ViewIterator::id() const {
	if (need_parse) parseKey();
	return docid;
}

void ViewIterator::parseKey() const {
	auto k = MapIterator::key();
	k = k.substr(8); //skip view-id
	ukey = string2jsonkey(std::move(k));
	docid = k;
	need_parse = false;
}


std::uint64_t ViewTools::getViewKey(const std::string_view &viewName) {
	std::uint64_t h;
	FNV1a64 fnv(h);
	for (char c: viewName) fnv(c);
	//highest number 0 is reserved for view definition - so ensure, that this number is not zero
	h |= static_cast<std::uint64_t>(1) << (sizeof(h)*8-1);
	return h;
}

class AbstractView::UpdateDoc: public EmitFn {
public:

	UpdateDoc(DocDB &db):db(db) {}

	virtual void operator()(const json::Value &key, const json::Value &value) const override;
	void indexDoc(std::uint64_t viewId,const Document &doc, const IViewMap &view);
protected:
	DocDB &db;
	mutable std::string keybase;
	mutable std::string val;
	mutable std::string keypart;
	mutable json::Array keys;
	mutable WriteBatch batch;
	std::string_view id;


};

void AbstractView::UpdateDoc::operator()(const json::Value &key, const json::Value &value) const {
	keypart.clear();
	jsonkey2string(key, keypart);
	keys.push_back(keypart);
	keybase.resize(index_byte_length);
	keybase.append(keypart);
	keybase.append(id);
	val.clear();
	json2string(value, val);
	batch.Put(keybase, val);
}


void AbstractView::UpdateDoc::indexDoc(std::uint64_t viewId, const Document &doc, const IViewMap &view) {
	batch.Clear();
	keys.clear();
	keybase.clear();
	id = doc.id;

	index2string(viewId, keybase);
	{
		keybase.push_back(codepoints::doc);
		keybase.append(doc.id);
		if (db.mapGet_pk(std::move(keybase), val)) {
			batch.Delete(keybase);
			json::Value keys = string2json(val);
			for (json::Value v: keys) {
				keybase.resize(index_byte_length);
				keybase.append(v.getString());
				keybase.append(doc.id);
				batch.Delete(keybase);
			}
		}
		keybase.resize(index_byte_length);
	}
	view.map(doc, *this);

	keybase.resize(index_byte_length);
	keybase.push_back(codepoints::doc);
	keybase.append(doc.id);
	val.clear();
	json2string(keys, val);
	batch.Put(keybase, val);
	db.flushBatch(batch, false);
}

AbstractView::AbstractView(DocDB &db, std::string &&name, uint64_t serial_nr)
:db(db)
,seqId(0)
,serialNr(serial_nr)
,updateDB(&db)
,name(std::move(name))
,viewid(ViewTools::getViewKey(this->name))
{
	ViewTools tools(db);
	ViewTools::State x = tools.getViewState(this->name);
	if (x.serialNr == serial_nr) seqId = x.seq;
}


void AbstractView::rebuild() {
	seqId = 0;
	storeState();
	update();
}

void AbstractView::clear() {
	std::string key;
	index2string(viewid, key);
	db.mapErasePrefix_pk(std::move(key));
}

class EmptyMap: public IViewMap {
public:
	virtual void map(const Document &doc, const EmitFn &emit) const {
	}
};




void AbstractView::purgeDoc(std::string_view docid) {
	EmptyMap emap;
	UpdateDoc up(db);
	up.indexDoc(viewid, Document{std::string(docid)}, emap);
}

void AbstractView::update() {
	if (updateDB) {
		update(*updateDB);
	}
}

void AbstractView::update(DocDB &updateDB) {

	if (seqId < updateDB.getLastSeqID()) {
		std::lock_guard _(wrlock);
		UpdateDoc up(db);
		ChangesIterator iter = updateDB.getChanges(seqId);
		SeqID newseqid = seqId;

		while (iter.next()) {
			auto docid = iter.doc();
			auto doc = db.get(docid);
			up.indexDoc(viewid,doc,*this);
			newseqid = iter.seq();
		}

		seqId = newseqid;
		storeState();
	}
}

ViewIterator AbstractView::find(const json::Value &key, bool backward) {
	return find(key,std::string_view());
}

ViewIterator AbstractView::find(const json::Value &key, const std::string_view &from_doc, bool backward) {
	update();
	std::string skey1, skey2;
	index2string(viewid, skey1);
	jsonkey2string(key,skey1);
	skey2 = skey1;
	if (backward) {
		if (from_doc.empty()) {
			DocDB::increaseKey(skey1);
		} else {
			skey1.append(from_doc);
		}
	} else {
		skey1.append(from_doc);
		DocDB::increaseKey(skey2);
	}
	return ViewIterator(db.mapScan_pk(std::move(skey1), std::move(skey2), DocDB::excludeBegin));
}

json::Value AbstractView::lookup(const json::Value &key) {
	auto iter = find(key,false);
	if (iter.next()) return iter.value();
	else return json::Value();
}

ViewIterator AbstractView::scan() {
	update();
	std::string pfx;
	index2string(viewid,pfx);
	return ViewIterator(db.mapScanPrefix_pk(std::move(pfx),false));
}

ViewIterator AbstractView::scanRange(const json::Value &from,
		const json::Value &to, bool exclude_end) {
	return scanRange(from, to, std::string_view(), exclude_end);
}

ViewIterator AbstractView::scanRange(const json::Value &from,
		const json::Value &to, const std::string_view &from_doc,
		bool exclude_end) {

	update();
	std::string strf, strt;
	index2string(viewid, strf);
	strt = strf;
	jsonkey2string(from,strf);
	jsonkey2string(to,strt);
	strf.append(from_doc);
	return ViewIterator(
			db.mapScan_pk(std::move(strf), std::move(strt), (exclude_end?DocDB::excludeEnd:0)|DocDB::excludeBegin)
	);
}

ViewIterator AbstractView::scanPrefix(const json::Value &prefix, bool backward) {
	update();
	std::string key1;
	index2string(viewid, key1);
	jsonkey2string(prefix, key1);
	key1.pop_back();
	return ViewIterator(
			db.mapScanPrefix_pk(std::move(key1), backward)
	);

}

ViewIterator AbstractView::scanFrom(const json::Value &item, bool backward, const std::string &from_doc) {
	update();
	std::string key1;
	index2string(viewid, key1);
	std::string key2(key1);
	if (!backward) {
		DocDB::increaseKey(key2);
	}
	key1.append(from_doc);
	return ViewIterator(
			db.mapScan_pk(std::move(key1), std::move(key2), DocDB::excludeBegin|DocDB::excludeEnd)
	);
}

ViewIterator AbstractView::scanFrom(const json::Value &key, bool backward) {
	update();
	std::string key1;
	index2string(viewid, key1);
	std::string key2(key1);
	if (backward) {
		DocDB::increaseKey(key1);
	} else {
		DocDB::increaseKey(key2);
	}
	return ViewIterator(
			db.mapScan_pk(std::move(key1), std::move(key2), DocDB::excludeBegin|DocDB::excludeEnd)
	);
}

void AbstractView::storeState() {
	ViewTools tools(db);
	tools.setViewState(name,{serialNr,seqId});
}

} /* namespace docdb */

