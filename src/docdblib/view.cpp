/*
 * view.cpp
 *
 *  Created on: 3. 1. 2021
 *      Author: ondra
 */


#include "view.h"
#include "formats.h"

using namespace json;


namespace docdb {


std::pair<json::Value, std::string_view> View::parseKey(const KeyView &key) {
	auto content = key.content();
	auto v = string2jsonkey(std::move(content));
	return {
		v, content
	};

}

json::Value View::parseValue(const std::string_view &value) {
	return string2json(value);
}

json::Value View::extractSubKey(unsigned int index, const KeyView &key) {
	return extract_subkey(index, key.content());
}

json::Value View::extractSubValue(unsigned int index, const std::string_view &key) {
	return extract_subvalue(index, std::string_view(key));
}

View::Iterator::Iterator(Super &&src):Super(std::move(src)) {

}

bool View::Iterator::next() {
	cache.reset();
	return next();
}

bool View::Iterator::peek() {
	cache.reset();
	return peek();

}
KeyView View::Iterator::global_key() {
	return Super::key();
}


json::Value View::Iterator::key() const {
	if (!cache.has_value()) cache = parseKey(Super::key());
	return cache->first;
}

std::string_view View::Iterator::id() const {
	if (!cache.has_value()) cache = parseKey(Super::key());
	return cache->second;
}

json::Value View::Iterator::key(unsigned int index) const {
	return extractSubKey(index, Super::key());
}

json::Value View::Iterator::value() const {
	return parseValue(Super::value());
}

json::Value View::Iterator::value(unsigned int index) const {
	return extractSubValue(index, Super::value());
}


View::View(const DB &db, const std::string_view &name)
:db(db),kid(this->db.allocKeyspace(KeySpaceClass::view, name))
{

}

json::Value View::lookup(const json::Value &key, bool set_docid) {
	Iterator iter = find(key);
	if (iter.next()) {
		Value v = iter.value();
		if (set_docid) return v.setKey(iter.id());
		else return v;
	} else {
		return undefined;
	}
}

Key View::createKey(const std::initializer_list<json::Value> &val, const std::string_view &doc) const {
	Key ret(kid,guessKeySize(val)+doc.size()+8);
	ret.append(val);
	ret.append(doc);
	return ret;
}

Key View::createKey(const json::Value &val, const std::string_view &doc) const {
	Key ret(kid,guessKeySize(val)+doc.size()+8);
	ret.append(val);
	ret.append(doc);
	return ret;
}

Key View::createDocKey(const std::string_view &doc) const {
	Key ret(kid, doc.size()+1);
	ret.push(0);
	ret.append(doc);
	return ret;
}

View::Iterator View::find(json::Value key) const {
	Key b1(createKey(key,std::string_view()));
	Key b2(b1);
	b2.upper_bound();
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::find(json::Value key, bool backward) const {
	Key b1(createKey(key,std::string_view()));
	Key b2(b1);
	b2.upper_bound();
	return Iterator(backward
			?db.createIterator({b2,b1,true,false})
			:db.createIterator({b1,b2,false,true}));
}

View::Iterator View::find(json::Value key, const std::string_view &fromDoc, bool backward) const {
	Key b1(createKey(key,fromDoc));
	Key b2(createKey(key,std::string_view()));
	if (!backward) b2.upper_bound();
	return Iterator(db.createIterator({b1,b2,true,!backward}));
}

View::Iterator View::range(json::Value fromKey, json::Value toKey) const {

}

}


