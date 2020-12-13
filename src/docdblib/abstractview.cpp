/*
 * abstractview.cpp
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#include "abstractview.h"
#include "formats.h"
#include <imtjson/binjson.tcc>
#include <imtjson/fnv.h>
#include "changesiterator.h"
#include "view_iterator.h"

namespace docdb {

using ViewName = DocDB::ViewListIndexKey;

ViewTools::State ViewTools::getViewState(const std::string_view &viewName) const {
	ViewName key(viewName);
	std::string val;
	if (db.mapGet(key, val)) {
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
	db.mapSet(key, val);
}

void ViewTools::erase(const std::string_view &name) {
	auto id = getViewID(name);
	db.mapErasePrefix(DocDB::ViewIndexKey(id));
	db.mapErasePrefix(DocDB::ViewDocIndexKey(id));
	db.mapErase(ViewName(name));
}

ViewTools::ViewTools(DocDB &db):db(db) {
}

ViewList docdb::ViewTools::list() {
	MapIterator iter = db.mapScanPrefix(ViewName(), false);
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

std::string ViewTools::getViewID(const std::string_view &viewName) {
	std::uint64_t h;
	FNV1a64 fnv(h);
	for (char c: viewName) fnv(c);
	//highest number 0 is reserved for view definition - so ensure, that this number is not zero
	h |= static_cast<std::uint64_t>(1) << (sizeof(h)*8-1);
	std::string out;
	Index2String_Push<8>::push(h,out);
	return out;
}

class AbstractView::UpdateDoc: public EmitFn {
public:

	UpdateDoc(DocDB &db, const ViewID &viewId, const ViewDocID &viewDocID)
		:db(db)
		,viewkey(viewId)
		,viewdockey(viewDocID)
		,viewkey_size(viewId.length())
		,viewdockey_size(viewDocID.length())
		{}


	virtual void operator()(const json::Value &key, const json::Value &value) const override {
		indexKey(key, value);
	}
	virtual void operator()(const json::Value &key, const std::initializer_list<json::Value> &value) const override {
		indexKey(key, value);
	}
	virtual void operator()(const std::initializer_list<json::Value> &key, const json::Value &value) const override {
		indexKey(key, value);
	}
	virtual void operator()(const std::initializer_list<json::Value> &key, const std::initializer_list<json::Value> &value) const override {
		indexKey(key, value);
	}

	template<typename A, typename B>
	void indexKey(const A &key, const B &value) const;
	void indexDoc(const Document &doc, AbstractView &owner);
protected:
	DocDB &db;
	mutable ViewID viewkey;
	mutable ViewDocID viewdockey;
	std::size_t viewkey_size;
	std::size_t viewdockey_size;
	mutable std::string val;
	mutable std::string keypart;
	mutable json::Array keys;
	mutable WriteBatch batch;
	std::string_view id;



};

template<typename A, typename B>
void AbstractView::UpdateDoc::indexKey(const A &key, const B &value) const {
	//clear viewkey - up to length of viewid
	viewkey.resize(viewkey_size);
	//serialize key to string
	jsonkey2string(key, viewkey);
	//clear value
	val.clear();
	//serialize value to string
	json2string(value, val);
	//retrieve end of key
	std::size_t keyend = viewkey.length();
	//append document id
	viewkey.append(id);
	//put record to batch
	batch.Put(viewkey, val);
	//crop binary key from viewkey and store to array
	keys.push_back(std::string_view(viewkey.data()+viewkey_size, keyend - viewkey_size));
}



void AbstractView::UpdateDoc::indexDoc(const Document &doc, AbstractView &owner) {
	//clear batch - remove anything not-written
	batch.Clear();
	//clear list of keys
	keys.clear();
	//clear viewdockey
	viewdockey.resize(viewdockey_size);
	//append document id
	viewdockey.append(doc.id);
	//doc id
	id = doc.id;

	//try to search all current keys for the document
	if (db.mapGet(viewdockey, val)) {
		//delete keys - we will no longer needed
		batch.Delete(viewdockey);
		//deserialize list of keys
		json::Value keys = string2json(val);
		//for every key
		for (json::Value v: keys) {
			//clear viewkey
			viewkey.resize(viewkey_size);
			//append binary form of key
			viewkey.append(v.getString());
			//append document id
			viewkey.append(doc.id);

			assert(db.mapExist(viewkey));

			//delete it
			batch.Delete(viewkey);
		}
		owner.updatedKeys(batch, keys, true);
	}

	//now process the document only if not deleted
	if (!doc.deleted) {
		//map whole document
		//if exception is thrown here, all changes are reverted because batch is not flushed
		owner.map(doc, *this);
		//clear value buffer
		val.clear();
		//serialize list of keys to value
		json2string(keys, val);
		//register new keys with document
		batch.Put(viewdockey, val);

		owner.updatedKeys(batch, keys, false);
	}
	//flush any batch
	db.flushBatch(batch);
}

StaticView::StaticView(DocDB &db, const std::string_view &name)
	:db(db),viewid(DocDB::ViewIndexKey(ViewTools::getViewID(name)))
{}

StaticView::StaticView(DocDB &db, const ViewID &viewid)
:db(db),viewid(viewid)
{}

AbstractView::AbstractView(DocDB &db, std::string &&name, uint64_t serial_nr)
	:UpdatableObject(db)
	,AbstractUpdatableView(db, name)
	,name(std::move(name))
	,serialNr(serial_nr)
	,viewdocid(ViewTools::getViewID(this->name))
{
	ViewTools tools(db);
	ViewTools::State x = tools.getViewState(this->name);
	if (x.serialNr == serial_nr) seqId = x.seq;
}


void AbstractView::clear() {
	ViewTools tools(db);
	tools.erase(name);
	seqId = 0;
	storeState();
}



void AbstractView::purgeDoc(std::string_view docid) {
	std::lock_guard _(wrlock);
	UpdateDoc up(db, viewid, viewdocid);
	json::Value ck, dk;
	up.indexDoc(Document{std::string(docid), json::Value(), 0, true}, *this);
}

void AbstractView::updateDoc(const Document &doc) {
	std::lock_guard _(wrlock);
	json::Value ck, dk;
	UpdateDoc up(db, viewid, viewdocid);
	up.indexDoc(doc, *this);
}

SeqID AbstractView::scanUpdates(ChangesIterator &&iter) {
	UpdateDoc up(db, viewid, viewdocid);
	SeqID newseqid = 0;
	json::Value ck, dk;

	while (iter.next()) {
		auto docid = iter.doc();
		auto doc = db.get(docid);
		up.indexDoc(doc,*this);
		newseqid = iter.seq();
	}
	return newseqid;
}

ViewIterator StaticView::find(const json::Value &key, bool backward) {
	return find(key,std::string_view(), backward);
}

ViewIterator StaticView::find(const json::Value &key, const std::string_view &from_doc, bool backward) {
	ViewID skey1(viewid);
	jsonkey2string(key,skey1);
	ViewID skey2 (skey1);

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
	return ViewIterator(db.mapScan(skey1, skey2, from_doc.empty()?0:DocDB::excludeBegin), DefaultJSONTranslator::getInstance());
}

json::Value StaticView::lookup(const json::Value &key) {
	auto iter = find(key,false);
	if (iter.next()) return iter.value();
	else return json::Value();
}

ViewIterator StaticView::scan() {
	return ViewIterator(db.mapScanPrefix(viewid,false), DefaultJSONTranslator::getInstance());
}

ViewIterator StaticView::scanRange(const json::Value &from,
		const json::Value &to, bool exclude_end) {
	return scanRange(from, to, std::string_view(), exclude_end);
}

ViewIterator StaticView::scanRange(const json::Value &from,
		const json::Value &to, const std::string_view &from_doc,
		bool exclude_end) {

	ViewID strf(viewid), strt(viewid);
	jsonkey2string(from,strf);
	jsonkey2string(to,strt);
	strf.append(from_doc);
	return ViewIterator(
			db.mapScan(strf, strt, (exclude_end?DocDB::excludeEnd:0)|(from_doc.empty()?0:DocDB::excludeBegin)),
			DefaultJSONTranslator::getInstance()
	);
}

ViewIterator StaticView::scanPrefix(const json::Value &prefix, bool backward) {
	ViewID key1(viewid);
	jsonkey2string(prefix, key1);
	/* remove last character
	 *  because only array or string is allowed here otherwise it doesn't make sense.
	 *  For string, it is terminated by zero - so we need to search any substring
	 *  For array, it is terminated by array end - si we need to search any additional subkeys
	 */
	key1.pop_back();
	return ViewIterator(
			db.mapScanPrefix(key1, backward),
			DefaultJSONTranslator::getInstance()
	);

}

ViewIterator StaticView::scanFrom(const json::Value &item, bool backward, const std::string &from_doc) {
	ViewID key1(viewid);
	ViewID key2(viewid);
	if (!backward) {
		DocDB::increaseKey(key2);
	}
	jsonkey2string(item, key1);
	key1.append(from_doc);
	return ViewIterator(
			db.mapScan(key1, key2, (from_doc.empty()?0:DocDB::excludeBegin)|DocDB::excludeEnd),
			DefaultJSONTranslator::getInstance()
	);
}

ViewIterator StaticView::scanFrom(const json::Value &item, bool backward) {
	ViewID key1(viewid);
	ViewID key2(viewid);
	jsonkey2string(item, key1);
	if (backward) {
		DocDB::increaseKey(key1);
	} else {
		DocDB::increaseKey(key2);
	}
	return ViewIterator(
			db.mapScan(key1, key2, (backward?DocDB::excludeBegin:0)|DocDB::excludeEnd),
			DefaultJSONTranslator::getInstance()
	);
}

void AbstractView::storeState() {
	ViewTools tools(db);
	tools.setViewState(name,{serialNr,seqId});
}

View::View(DocDB &db, std::string_view name, std::uint64_t serialnr, MapFn mapFn)
:AbstractView(db, std::string(name), serialnr), mapFn(mapFn)
{
}

void View::map(const Document &doc, const EmitFn &emit) const {
	mapFn(doc,emit);
}

void AbstractView::removeObserver(IKeyUpdateObserver *obs) {
	std::unique_lock _(wrlock);
	keylistener.removeObserver(obs);
}

void AbstractView::addObserver(IKeyUpdateObserver *obs) {
	std::unique_lock _(wrlock);
	keylistener.addObserver(obs);
}

void AbstractView::update() {
	UpdatableObject::update();
}

void AbstractView::updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted) {
	if (!keylistener.empty()) {
		for (json::Value x: keys) {
			json::Value k = string2jsonkey(x.getString());
			keylistener.updatedKey(batch, k, deleted);
		}
	}
}

ViewIterator AbstractUpdatableView::find(const json::Value &key,
		bool backward) {
	update();return StaticView::find(key, backward);
}

ViewIterator AbstractUpdatableView::find(const json::Value &key,
		const std::string_view &from_doc, bool backward) {
	update();return StaticView::find(key, from_doc,backward);
}

json::Value AbstractUpdatableView::lookup(const json::Value &key) {
	update();return StaticView::lookup(key);
}

ViewIterator AbstractUpdatableView::scan() {
	update();return StaticView::scan();
}

ViewIterator AbstractUpdatableView::scanRange(const json::Value &from,
		const json::Value &to, bool exclude_end) {
	update();return StaticView::scanRange(from, to, exclude_end);

}

ViewIterator AbstractUpdatableView::scanRange(const json::Value &from,
		const json::Value &to, const std::string_view &from_doc,
		bool exclude_end) {
	update();return StaticView::scanRange(from, to, from_doc, exclude_end);
}

ViewIterator AbstractUpdatableView::scanPrefix(const json::Value &prefix,
		bool backward) {
	update();return StaticView::scanPrefix(prefix, backward);
}

ViewIterator AbstractUpdatableView::scanFrom(const json::Value &key,
		bool backward) {
	update();return StaticView::scanFrom(key, backward);
}

ViewIterator AbstractUpdatableView::scanFrom(const json::Value &key,
		bool backward, const std::string &from_doc) {
	update();return StaticView::scanFrom(key, backward, from_doc);
}

} /* namespace docdb */

