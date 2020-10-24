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

	void operator()(json::Value key, json::Value value) const {
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

	void processDoc(const Document &doc, const AbstractView &view);
protected:
	DocDB &db;
	mutable std::string keybase;
	mutable std::string val;
	mutable std::string keypart;
	mutable json::Array keys;
	mutable WriteBatch batch;
	std::string_view id;


};

void AbstractView::UpdateDoc::processDoc(const Document &doc, const AbstractView &view) {
	batch.Clear();
	keys.clear();
	keybase.clear();
	id = doc.id;

	index2string(view.viewid, keybase);
	{
		keybase.push_back(codepoints::doc);
		keybase.append(doc.id);
		MapIterator iter = db.mapScanPrefix_pk(std::move(keybase), false);
		if (iter.next()) {
			batch.Delete(keybase);
			json::Value keys = string2json(iter.value());
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





} /* namespace docdb */

