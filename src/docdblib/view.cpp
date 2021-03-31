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
	return Super::next();
}

bool View::Iterator::peek() {
	cache.reset();
	return Super::peek();

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

json::Value View::lookup(const json::Value &key, bool set_docid) const {
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
	Key b1(createKey(fromKey,std::string_view()));
	Key b2(createKey(toKey,std::string_view()));
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::range(json::Value fromKey, json::Value toKey,
		bool include_upper_bound) const {
	Key b1(createKey(fromKey,std::string_view()));
	Key b2(createKey(toKey,std::string_view()));
	if (include_upper_bound) {
		if (b1 > b2) b1.upper_bound(); else b2.upper_bound();
	}
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::range(json::Value fromKey, json::Value toKey,
		const std::string_view &fromDoc, bool include_upper_bound) const {
	Key b1(createKey(fromKey,fromDoc));
	Key b2(createKey(toKey,std::string_view()));
	if (include_upper_bound) {
		if (b1 > b2) {
			if (fromDoc.empty()) b1.upper_bound();
		} else {
			b2.upper_bound();
		}
	}
	return Iterator(db.createIterator({b1,b2,true,true}));

}

View::Iterator View::prefix(json::Value key) const {
	Key b1(createKey(key, std::string_view()));
	b1.pop();
	Key b2(b1);
	b2.upper_bound();
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::prefix(json::Value key, bool backward) const {
	Key b1(createKey(key, std::string_view()));
	b1.pop();
	Key b2(b1);
	if (backward) {
		b1.upper_bound();
	} else {
		b2.upper_bound();
	}
	return Iterator(db.createIterator({b1,b2,backward,!backward}));
}

View::Iterator View::prefix(json::Value key, json::Value fromKey,
		const std::string_view &fromDoc, bool backward) const {
	Key b1(createKey(fromKey, fromDoc));
	Key b2(createKey(key, std::string_view()));
	if (!backward) b2.upper_bound();
	return Iterator(db.createIterator({b1,b2,true,!backward}));

}

View::Iterator View::scan() const {
	Key b1(kid,1);
	b1.push(1);
	Key b2(kid+1);
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::scan(bool backward) const {
	Key b1(kid,1);
	b1.push(1);
	Key b2(kid+1);
	if (backward) {
		return Iterator(db.createIterator({b2,b1,true,false}));
	} else {
		return Iterator(db.createIterator({b1,b2,false,true}));
	}
}

View::Iterator View::scan(json::Value fromKey, const std::string_view &fromDoc, bool backward) const {
	Key b1(createKey(fromKey, fromDoc));
	Key b2(backward?kid:kid+1);
	return Iterator(db.createIterator({b1,b2,true,!backward}));
}

View::DocKeyIterator View::getDocKeys(const std::string_view &docid) const {
	return DocKeyIterator(db, Key(createDocKey(docid)));
}

View::DocKeyIterator::DocKeyIterator(const DB db, const Key &key):rdidx(0) {
	if (!db.get(key, buffer)) buffer.clear();
}

bool View::DocKeyIterator::next() {
	if (rdidx >= buffer.size()) return false;
	std::string_view tmp(buffer.data()+rdidx, buffer.size()-rdidx);
	rdkey = string2jsonkey(std::move(tmp));
	rdidx = buffer.size() - tmp.size();
	return true;
}

bool View::DocKeyIterator::peek() {
	if (rdidx >= buffer.size()) return false;
	std::string_view tmp(buffer.data()+rdidx, buffer.size()-rdidx);
	rdkey = string2jsonkey(std::move(tmp));
	return true;
}

json::Value View::DocKeyIterator::key() const {
	return rdkey;
}

bool View::DocKeyIterator::empty() const {
	return rdidx >= buffer.size();
}

}
