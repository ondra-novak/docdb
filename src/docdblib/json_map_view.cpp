/*
 * json_map_view.cpp
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#include "json_map_view.h"

#include "formats.h"
namespace docdb {

JsonMapView::JsonMapView(DB db, const std::string_view &name)
:db(db),kid(this->db.allocKeyspace(KeySpaceClass::jsonmap_view, name))
{
}

JsonMapView::JsonMapView(DB db, ClassID classId, const std::string_view &name)
:db(db),kid(this->db.allocKeyspace(classId, name)) {

}

JsonMapView::JsonMapView(const JsonMapView &other, const DB &snapshot)
:db(snapshot),kid(other.kid)
{

}

json::Value JsonMapView::lookup(const json::Value &key) const {
	auto &buffer = DB::getBuffer();
	if (db.get(createKey(key), buffer)) {
		return string2json(std::string_view(buffer));
	} else {
		return json::undefined;
	}
}

JsonMapView::Iterator JsonMapView::find(json::Value key) const {
	Key f(createKey(key));
	Key t(f);
	t.upper_bound();
	return Iterator(db.createIterator({f,t,false,true}));
}

JsonMapView::Iterator JsonMapView::range(json::Value fromKey,
		json::Value toKey) const {
	return range(fromKey, toKey, false);
}

JsonMapView::Iterator JsonMapView::range(json::Value fromKey, json::Value toKey,
		bool include_upper_bound) const {
	Key f(createKey(fromKey));
	Key t(createKey(toKey));
	bool bkw = f > t;
	bool b1 = include_upper_bound || bkw;
	bool b2 = include_upper_bound || !bkw;
	return Iterator(db.createIterator({f,t,b1,b2}));
}

JsonMapView::Iterator JsonMapView::prefix(json::Value key) const {
	Key f(createKey(key));
	f.pop();
	Key t(f);
	t.upper_bound();
	return Iterator(db.createIterator({f,t,false,true}));
}

JsonMapView::Iterator JsonMapView::prefix(json::Value key,
		bool backward) const {
	Key f(createKey(key));
	f.pop();
	Key t(f);
	if (backward) f.upper_bound(); else t.upper_bound();
	return Iterator(db.createIterator({f,t,backward,!backward}));
}

JsonMapView::Iterator JsonMapView::scan() const {
	return Iterator(db.createIterator({Key(kid),Key(kid+1),false,true}));}

JsonMapView::Iterator JsonMapView::scan(bool backward) const {
	return Iterator(db.createIterator({Key(kid+(backward?1:0)),Key(kid+(backward?0:1)),backward,!backward}));
}

JsonMapView::Iterator JsonMapView::scan(json::Value fromKey, bool backward) const {
	return Iterator(db.createIterator({createKey(fromKey),Key(kid+(backward?0:1)),true,!backward}));
}

json::Value JsonMapView::parseKey( const KeyView &key) {
	return string2jsonkey(key.content());
}

json::Value JsonMapView::parseValue(const std::string_view &value) {
	return string2json(std::string_view(value));
}

json::Value JsonMapView::parseValue(std::string_view &&value) {
	if (value.empty() || value[0]<0) return json::Value(); //to see inside agregator and not crash the system
	else return string2json(std::move(value));
}

json::Value JsonMapView::extractSubKey(unsigned int index, const KeyView &key) {
	return extract_subkey(index, key.content());
}


json::Value JsonMapView::extractSubValue(unsigned int index,
		const std::string_view &key) {
	return extract_subvalue(index, std::string_view(key));
}

Key JsonMapView::createKey(const json::Value &val) const {
	Key ret(kid,guessKeySize(val)+8);
	ret.append(val);
	return ret;
}

Key JsonMapView::createKey(
		const std::initializer_list<json::Value> &val) const {
	Key ret(kid,guessKeySize(val)+8);
	ret.append(val);
	return ret;
}


JsonMapView::Iterator::Iterator(Super &&src):Super(std::move(src)) {

}

KeyView JsonMapView::Iterator::global_key() {
	return Super::key();
}

json::Value JsonMapView::Iterator::key() const {
	return JsonMapView::parseKey(Super::key());
}

json::Value JsonMapView::Iterator::key(unsigned int index) const {
	return JsonMapView::extractSubKey(index, Super::key());
}

json::Value JsonMapView::Iterator::value() const {
	return JsonMapView::parseValue(Super::value());
}

json::Value JsonMapView::Iterator::value(unsigned int index) const {
	return JsonMapView::extractSubValue(index, Super::value());
}

void JsonMapView::createValue(const std::initializer_list<json::Value> &val, std::string &out) {
	json2string(val,out);
}

void JsonMapView::createValue(const json::Value &val, std::string &out) {
	json2string(val,out);
}

}
