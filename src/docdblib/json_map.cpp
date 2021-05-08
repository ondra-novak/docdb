/*
 * json_map.cpp
 *
 *  Created on: 9. 1. 2021
 *      Author: ondra
 */


#include "json_map.h"

#include "formats.h"
namespace docdb {

void JsonMapBase::set(Batch &b, const json::Value &key, const json::Value &value) {
	auto &buff = DB::getBuffer();
	json2string(value, buff);
	b.Put(createKey(key), buff);
}

void JsonMap::set(const json::Value &key, const json::Value &value) {
	Batch b;
	set(b, key, value);
	db.commitBatch(b);
}

void JsonMapBase::erase(Batch &b, const json::Value &key) {
	b.Delete(createKey(key));
}

void JsonMap::erase(const json::Value &key) {
	Batch b;
	erase(b,key);
	db.commitBatch(b);
}

void JsonMapBase::clear() {
	db.clearKeyspace(kid);
}

void JsonMap::set(Batch &b, const json::Value &key, const json::Value &value) {
	JsonMapBase::set(b,key,value);
	observers->broadcast(b, key, value);
}
void JsonMap::erase(Batch &b, const json::Value &key) {
	JsonMapBase::erase(b,key);
	observers->broadcast(b, key, json::undefined);

}

JsonMap::JsonMap(DB db, const std::string_view &name)
	:JsonMapBase(db, name)
	,observers(db.getObservable<Obs>(kid))
{
	db.keyspaceLock(kid, true);
}

JsonMap::~JsonMap() {
	db.keyspaceLock(kid, false);
}

KeySpaceID JsonMapBase::getKID() const {
	return kid;
}

}
