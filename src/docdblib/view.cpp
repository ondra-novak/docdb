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

json::Value View::Iterator::id() const {
	if (!cache.has_value()) cache = parseKey(Super::key());
	return string2jsonkey(std::string_view(cache->second));
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

json::Value View::lookup(const json::Value &key) const {
	Iterator iter = find(key);
	if (iter.next()) {
		return iter.value();
	} else {
		return undefined;
	}
}

json::Value View::lookup(const json::Value &key, json::Value &docId) const {
	Iterator iter = find(key);
	if (iter.next()) {
		docId = iter.id();
		return iter.value();
	} else {
		return undefined;
	}
}


Key View::createKey(const std::initializer_list<json::Value> &val, const json::Value &docId) const {
	Key ret(kid,guessKeySize(val)+guessKeySize(docId)+8);
	ret.append(val);
	ret.append(docId);
	return ret;
}

Key View::createKey(const json::Value &val, const json::Value &docId) const {
	Key ret(kid,guessKeySize(val)+guessKeySize(docId)+8);
	ret.append(val);
	ret.append(docId);
	return ret;
}

Key View::createDocKey(const json::Value &docId) const {
	Key ret(kid, guessKeySize(docId)+1);
	ret.push(0);
	ret.append(docId);
	return ret;
}

View::Iterator View::find(const json::Value &key) const {
	return range(key, nullptr, key, MAX_KEY_VALUE);
}

View::Iterator View::find(const json::Value &key, bool backward) const {
	return backward?range(key, MAX_KEY_VALUE, key, nullptr):range(key, nullptr, key, MAX_KEY_VALUE);
}

View::Iterator View::find(const json::Value &key, const json::Value &fromDoc, bool backward) const {
	return backward?range(key, fromDoc, key, nullptr):range(key, fromDoc, key, MAX_KEY_VALUE);}

View::Iterator View::range(const json::Value &fromKey, const json::Value &toKey) const {
	return range(fromKey, nullptr, toKey, nullptr);
}

View::Iterator View::range(const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound) const {
	Key b1(createKey(fromKey,nullptr));
	Key b2(createKey(toKey,nullptr));
	bool backward = b1 > b2;
	if (include_upper_bound) {
		if (backward) b1.upper_bound(); else b2.upper_bound();
	}
	return Iterator(db.createIterator({b1,b2,backward,!backward}));
}

View::Iterator View::range(const json::Value &fromKey, const json::Value &fromDocId, const json::Value &toKey, const json::Value &toDocId) const {
	Key b1(createKey(fromKey,fromDocId));
	Key b2(createKey(toKey,toDocId));
	return Iterator(db.createIterator({b1,b2,true,true}));

}

View::Iterator View::prefix(const json::Value &key) const {
	Key b1(createKey(key, std::string_view()));
	b1.pop();
	Key b2(b1);
	b2.upper_bound();
	return Iterator(db.createIterator({b1,b2,false,true}));
}

View::Iterator View::prefix(const json::Value &key, bool backward) const {
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

View::Iterator View::prefix(const json::Value &key, const json::Value &fromKey, const json::Value &fromDoc, bool backward) const {
	Key b1(createKey(fromKey, fromDoc));
	Key b2(createKey(key, nullptr));
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

View::Iterator View::scan(const json::Value &fromKey, const json::Value &fromDoc, bool backward) const {
	Key b1(createKey(fromKey, fromDoc));
	Key b2(backward?kid:kid+1);
	return Iterator(db.createIterator({b1,b2,true,!backward}));
}

View::DocKeyIterator View::getDocKeys(const json::Value &docid) const {
	return DocKeyIterator(db, Key(createDocKey(docid)));
}

View::DocKeyIterator::DocKeyIterator(const DB db, const Key &key):rdidx(0) {
	if (!db.get(key, buffer)) buffer.clear();
}

bool View::DocKeyIterator::next() {
	if (rdidx >= buffer.size()) return false;
	std::string_view tmp(buffer.data()+rdidx, buffer.size()-rdidx);
	rdkey = string2json(std::move(tmp));
	rdidx = buffer.size() - tmp.size();
	return true;
}

bool View::DocKeyIterator::peek() {
	if (rdidx >= buffer.size()) return false;
	std::string_view tmp(buffer.data()+rdidx, buffer.size()-rdidx);
	rdkey = string2json(std::move(tmp));
	return true;
}

json::Value View::DocKeyIterator::key() const {
	return rdkey;
}

bool View::DocKeyIterator::empty() const {
	return rdidx >= buffer.size();
}

void View::serializeDocKeys(const std::vector<json::Value> &keys, std::string &buffer) {
	buffer.clear();
	for (const auto &k: keys) {
		json2string(k, buffer);
	}
}

void IndexBatch::emit(const json::Value &k, const json::Value &v) {
	//key already initialized with current keyspace
	//append k
	key.append(k);
	//append docId
	key.append(dockey);
	//create value (serialize to buffer)
	json2string(v, buffer);
	//push current key to list of keys
	keys.push_back(k);
	//store key-value pair to batch
	Put(key, buffer);
	//clear key buffer
	key.clear();
	//clear value buffer
	buffer.clear();
}

void IndexBatch::commit() {

	//after key-value pairs has been created (emit), we continue hear
	//retrieve count of created keys
	auto klen = key.size();
	//if no keys has been created
	if (klen == 0) {
		//are there some previous keys (must be erased)
		if (!prev_keys.empty()) {
			//yes - so first erase record about allocated keys
			//reuse key field
			key.clear();
			key.push_back(0);
			key.append(dockey);
			Delete(key);
			//now process all previous keys and erase them
			//erase keys in reverse order (it is simple)
			while (!prev_keys.empty()) {
				json::Value k (std::move(prev_keys.back()));
				key.clear();
				key.append(k);
				key.append(dockey);
				Delete(key);
				prev_keys.pop_back();
			}
			//erased everything
		}
		//all done
	} else {
		//some keys has been created
		//serialize list of keys to buffer, to be stored
		View::serializeDocKeys(keys, buffer);
		//update doc->keys record with newly generated keys
		key.clear();
		key.push_back(0);
		key.append(dockey);
		Put(key, buffer);
		//process previous keys to check and erase them
		while (!prev_keys.empty()) {
			json::Value k ( std::move(prev_keys.back()));
			auto kend = keys.begin()+klen;
			//try find this key in list of generated keys
			//if not found it must be erased
			if (std::find(keys.begin(),kend,k) == kend) {
				key.clear();
				key.append(k);
				key.append(dockey);
				//erase this key
				Delete(key);
				keys.push_back(k);
			}
			prev_keys.pop_back();
		}
		//all done
	}
}

}
