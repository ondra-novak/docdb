/*
 * json_map.cpp
 *
 *  Created on: 9. 1. 2021
 *      Author: ondra
 */


#include "json_map.h"

#include "formats.h"
namespace docdb {

void JsonMap::set(Batch &b, const json::Value &key, const json::Value &value) {
	auto &buff = DB::getBuffer();
	json2string(value, buff);
	b.Put(createKey(key), buff);
	observers.broadcast(b, key);
}

void JsonMap::set(const json::Value &key, const json::Value &value) {
	Batch b;
	set(b, key, value);
	db.commitBatch(b);
}

void JsonMap::erase(Batch &b, const json::Value &key) {
	b.Delete(createKey(key));
	observers.broadcast(b, key);
}

void JsonMap::erase(const json::Value &key) {
	Batch b;
	erase(b,key);
	db.commitBatch(b);
}

void JsonMap::clear() {
	db.clearKeyspace(kid);
}

}
