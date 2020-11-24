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


	virtual void operator()(const json::Value &key, const json::Value &value) const override;
	void indexDoc(const Document &doc, const IViewMap &view, IReduceObserver &observer);
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

void AbstractView::UpdateDoc::operator()(const json::Value &key, const json::Value &value) const {
	//clear viewkey - up to length of viewid
	viewkey.resize(viewkey_size);
	//serialize key to string
	jsonkey2string(key, viewkey);
	//retrieve end of key
	std::size_t keyend = viewkey.length();
	//append document id
	viewkey.append(id);
	//clear value
	val.clear();
	//serialize value to string
	json2string(value, val);
	//put record to batch
	batch.Put(viewkey, val);
	//crop binary key from viewkey and store to array
	keys.push_back(std::string_view(viewkey.data()+viewkey_size, keyend - viewkey_size));
}


void AbstractView::UpdateDoc::indexDoc(const Document &doc, const IViewMap &view, IReduceObserver &observer) {
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
		observer.updatedKeys(batch, keys, true);
	}

	//now process the document only if not deleted
	if (!doc.deleted) {
		//map whole document
		//if exception is thrown here, all changes are reverted because batch is not flushed
		view.map(doc, *this);
		//clear value buffer
		val.clear();
		//serialize list of keys to value
		json2string(keys, val);
		//register new keys with document
		batch.Put(viewdockey, val);

		observer.updatedKeys(batch, keys, false);
	}
	//flush any batch
	db.flushBatch(batch);
}

AbstractViewBase::AbstractViewBase(DocDB &db, ViewID &&viewid)
	:db(db),viewid(std::move(viewid))
{}

AbstractView::AbstractView(DocDB &db, std::string &&name, uint64_t serial_nr)
	:UpdatableObject(db)
	,AbstractViewBase(db, ViewTools::getViewID(name))
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

class EmptyMap: public IViewMap {
public:
	virtual void map(const Document &, const EmitFn &) const {
	}
};




void AbstractView::purgeDoc(std::string_view docid) {
	std::lock_guard _(wrlock);
	EmptyMap emap;
	UpdateDoc up(db, viewid, viewdocid);
	json::Value ck, dk;
	up.indexDoc(Document{std::string(docid), json::Value(), 0, true}, emap, *this);
}

void AbstractView::updateDoc(const Document &doc) {
	std::lock_guard _(wrlock);
	json::Value ck, dk;
	UpdateDoc up(db, viewid, viewdocid);
	up.indexDoc(doc, *this, *this);
}

SeqID AbstractView::scanUpdates(ChangesIterator &&iter) {
	UpdateDoc up(db, viewid, viewdocid);
	SeqID newseqid = 0;
	json::Value ck, dk;

	while (iter.next()) {
		auto docid = iter.doc();
		auto doc = db.get(docid);
		up.indexDoc(doc,*this, *this);
		newseqid = iter.seq();
	}
	return newseqid;
}

ViewIterator AbstractViewBase::find(const json::Value &key, bool backward) {
	return find(key,std::string_view(), backward);
}

ViewIterator AbstractViewBase::find(const json::Value &key, const std::string_view &from_doc, bool backward) {
	update();
	ViewID skey1(viewid), skey2;
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
	return ViewIterator(db.mapScan(skey1, skey2, DocDB::excludeBegin));
}

json::Value AbstractViewBase::lookup(const json::Value &key) {
	auto iter = find(key,false);
	if (iter.next()) return iter.value();
	else return json::Value();
}

ViewIterator AbstractViewBase::scan() {
	update();
	return ViewIterator(db.mapScanPrefix(viewid,false));
}

ViewIterator AbstractViewBase::scanRange(const json::Value &from,
		const json::Value &to, bool exclude_end) {
	return scanRange(from, to, std::string_view(), exclude_end);
}

ViewIterator AbstractViewBase::scanRange(const json::Value &from,
		const json::Value &to, const std::string_view &from_doc,
		bool exclude_end) {

	update();
	ViewID strf(viewid), strt(viewid);
	jsonkey2string(from,strf);
	jsonkey2string(to,strt);
	strf.append(from_doc);
	return ViewIterator(
			db.mapScan(strf, strt, (exclude_end?DocDB::excludeEnd:0)|DocDB::excludeBegin)
	);
}

ViewIterator AbstractViewBase::scanPrefix(const json::Value &prefix, bool backward) {
	update();
	ViewID key1(viewid);
	jsonkey2string(prefix, key1);
	/* remove last character
	 *  because only array or string is allowed here otherwise it doesn't make sense.
	 *  For string, it is terminated by zero - so we need to search any substring
	 *  For array, it is terminated by array end - si we need to search any additional subkeys
	 */
	key1.pop_back();
	return ViewIterator(
			db.mapScanPrefix(key1, backward)
	);

}

ViewIterator AbstractViewBase::scanFrom(const json::Value &item, bool backward, const std::string &from_doc) {
	update();
	ViewID key1(viewid);
	ViewID key2(viewid);
	if (!backward) {
		DocDB::increaseKey(key2);
	}
	jsonkey2string(item, key1);
	key1.append(from_doc);
	return ViewIterator(
			db.mapScan(key1, key2, DocDB::excludeBegin|DocDB::excludeEnd)
	);
}

ViewIterator AbstractViewBase::scanFrom(const json::Value &item, bool backward) {
	update();
	ViewID key1(viewid);
	ViewID key2(viewid);
	jsonkey2string(item, key1);
	if (backward) {
		DocDB::increaseKey(key1);
	} else {
		DocDB::increaseKey(key2);
	}
	return ViewIterator(
			db.mapScan(key1, key2, DocDB::excludeBegin|DocDB::excludeEnd)
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

void AbstractView::unregisterObserver(IReduceObserver *obs) {
	auto len = reduceObservers.size();
	if (len) {
		auto last = len - 1;
		auto idx = len;
		while (idx) {
			--idx;
			if (reduceObservers[idx] == obs) {
				if (idx != last)
					std::swap(reduceObservers[idx], reduceObservers[last]);
				reduceObservers.resize(last);
				break;
			}
		}
	}
}

void AbstractView::registerObserver(IReduceObserver *obs) {
	reduceObservers.push_back(obs);
}

void AbstractView::update() {
	UpdatableObject::update();
}

void AbstractView::updatedKeys(WriteBatch &batch, const json::Value &keys, bool deleted) {
	for (auto &&x : reduceObservers) x->updatedKeys(batch, keys, deleted);
}

AbstractReduceView::AbstractReduceView(AbstractView &viewMap, const std::string &name, unsigned int maxGroupLevel , bool reduceAll )
	:AbstractViewBase(viewMap.getDB(), ViewTools::getViewID(name))
	,viewMap(viewMap)
	,name(name)
	,maxGroupLevel(std::max<unsigned int>(maxGroupLevel,1))
	,reduceAll(reduceAll)
{
	viewMap.registerObserver(this);
}

AbstractReduceView::~AbstractReduceView() {
	viewMap.unregisterObserver(this);
}

void AbstractReduceView::updatedKeys(WriteBatch &batch, const json::Value &keys, bool) {
	DocDB::ReduceIndexKey k(viewid.content());
	auto ksz = k.length();

	for (json::Value v : keys) {
		json::Value pv = string2jsonkey(v.getString());
		json2string(pv, k);
		batch.Put(k, "");
		updated = false;
		k.resize(ksz);
	}
}

void AbstractReduceView::update() {
	if (updated) return;

	DocDB::ReduceIndexKey k(viewid.content());
	WriteBatch b;
	std::vector<KeyValue> items;
	std::unordered_map<json::Value, std::vector<json::Value> > rereduce_items, wrk;
	std::vector<json::Value> all_reduce;

	auto iter = db.mapScanPrefix(k, false);
	ViewID itmk(viewid);
	auto itmksz = itmk.length();
	std::string value;
	bool rep = false;

	while (iter.next()) {
		auto orig_key = iter.orig_key();
		auto str = iter.key();
		str = str.substr(k.content().length());
		json::Value v = string2json(str);
		if (v.type() == json::array) {
			v = v.slice(0,maxGroupLevel);
		}
		ViewIterator iter = viewMap.scanPrefix(v, false);
		while (iter.next()) {
			items.push_back(KeyValue{std::string(iter.id()), iter.key(), iter.value()});
		}
		json::Value rval = reduce(items, false);
		items.clear();
		auto asz =v.size();
		if (asz) {
			json::Value rr = v.slice(0,asz-1);
			rereduce_items[rr].push_back(rval);
			rep = true;
		} else if (reduceAll) {
			all_reduce.push_back(rval);
		}
		jsonkey2string(v,itmk);
		if (rval.defined()) {
			json2string(rval, value);
			b.Put(itmk, value);
		} else {
			b.Delete(itmk);
		}
		itmk.resize(itmksz);
		value.clear();
		b.Delete(orig_key);
	}

	while (rep) {
		rep = false;
		db.flushBatch(b);
		b.Clear();
		std::swap(wrk, rereduce_items);
		rereduce_items.clear();
		for (const auto &itm: wrk) {
			auto v = itm.first;
			for (const auto &x: itm.second) {
				items.push_back({"",v, x});
			}
			json::Value rval = reduce(items, true);
			items.clear();
			auto asz =v.size();
			if (asz) {
				json::Value rr = v.slice(0,asz-1);
				rereduce_items[rr].push_back(rval);
				rep = true;
			} else if (reduceAll) {
				all_reduce.push_back(rval);
			}
			jsonkey2string(v,itmk);
			if (rval.defined()) {
				json2string(rval, value);
				b.Put(itmk, value);
			} else {
				b.Delete(itmk);
			}
			itmk.resize(itmksz);
			value.clear();
		}
	}
	if (reduceAll) {
		for (const auto &x: all_reduce) {
			items.push_back({"",nullptr, x});
		}
		json::Value rval = reduce(items, true);
		items.clear();
		jsonkey2string(nullptr,itmk);
		if (rval.defined()) {
			json2string(rval, value);
			b.Put(itmk, value);
		} else {
			b.Delete(itmk);
		}
		itmk.resize(itmksz);
		value.clear();
	}
	db.flushBatch(b);
	updated = true;
}

ReduceView::ReduceView(AbstractView &viewMap, const std::string &name, ReduceFn reduceFn,
				unsigned int maxGroupLevel , bool reduceAll )
	:AbstractReduceView(viewMap, name, maxGroupLevel, reduceAll)
	,reduceFn(reduceFn) {}

json::Value ReduceView::reduce(const std::vector<KeyValue> &items, bool rereduce) const {
	return reduceFn(items, rereduce);
}

ReadOnlyView::ReadOnlyView(DocDB &db, const std::string_view &name)
:AbstractViewBase(db, ViewTools::getViewID(name))
{

}

} /* namespace docdb */

