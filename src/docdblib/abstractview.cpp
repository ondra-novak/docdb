/*
 * abstractview.cpp
 *
 *  Created on: 24. 10. 2020
 *      Author: ondra
 */

#include "abstractview.h"
#include "formats.h"
#include <imtjson/binjson.tcc>

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
	ViewName key(name);
	db.mapErasePrefix(name);
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
}

json::Value ViewIterator::value() const {
}

const std::string_view ViewIterator::id() const {
}

} /* namespace docdb */

