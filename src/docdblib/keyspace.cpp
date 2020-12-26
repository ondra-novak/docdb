/*
 * keyspace.cpp
 *
 *  Created on: 16. 12. 2020
 *      Author: ondra
 */

#include "keyspace.h"

#include "formats.h"
#include <imtjson/binjson.h>

namespace docdb {

static std::size_t guessSize(const json::Value &v) {
	switch (v.type()) {
	case json::undefined:
	case json::null:
	case json::boolean: return 1;
	case json::number: return 10;
	case json::string: return v.getString().length+2;
	case json::array: {
		std::size_t x = 0;
		for (json::Value item: v) {
			x = x + guessSize(item);
		}
		x++;
		return x;
	}
	default: {
		std::size_t x = 1;
		v.serializeBinary([&](char){++x;});
		return x;
	}}
}

static std::size_t guessSize(const std::initializer_list<json::Value> &v) {
	std::size_t x = 0;
	for (json::Value item: v) {
		x = x + guessSize(item);
	}
	x++;
	return x;
}

Key::Key(KeySpaceID keySpaceId, const std::initializer_list<json::Value> &key)
:Key(keySpaceId,guessSize(key))
{
	append(key);
}


Key::Key(KeySpaceID keySpaceId, std::size_t reserve) {
	keydata.reserve(keyspaceid_size+reserve);
	keydata.append(std::string_view(reinterpret_cast<char *>(&keySpaceId), keyspaceid_size));
}


Key::Key(KeySpaceID keySpaceId, const std::string_view &key)
:Key(keySpaceId, key.size())
{
	append(key);
}


Key::Key(KeySpaceID keySpaceId, const json::Value &key)
:Key(keySpaceId, guessSize(key))
{
	append(key);
}

Key::Key(KeySpaceID keySpaceId)
:Key(keySpaceId, 100)
{
}

void Key::append(const std::string_view &key) {
	keydata.append(key);
}


void Key::append(const json::Value &v) {
	jsonkey2string(v, keydata);
}

void Key::append(const std::initializer_list<json::Value> &v) {
	jsonkey2string(v, keydata);
}

void Key::clear() {
	keydata.resize(keyspaceid_size);
}


void Key::push(unsigned char byte) {
	keydata.push_back(static_cast<char>(byte));
}

unsigned char Key::pop() {
	if (empty()) return 0;
	else {
		unsigned char c = keydata.back();
		keydata.pop_back();
		return c;
	}
}

void Key::truncate(std::size_t sz) {
	keydata.resize(std::max(sz, keyspaceid_size));
}

template<typename T>
std::string_view KeyViewT<T>::extract_string(iterator &iter, std::size_t bytes) {
	std::size_t ofs = std::distance(begin(), iter);
	std::size_t remain = std::min(keydata.size() - ofs, bytes);
	iter+=remain;
	return std::string_view(keydata.data()+ofs, remain);
}

static bool calcUpperBound(std::string &x) {
	if (x.empty()) return false;
	unsigned char c = x.back();
	x.pop_back();
	if (c == 0xFF) {
		if (!calcUpperBound(x)) {
			x.push_back(c);
			return false;
		}
		x.push_back(0);
	} else{
		x.push_back(c+1);
	}
	return true;
}

bool Key::upper_bound() {
	return calcUpperBound(keydata);

}

template<typename T>
json::Value KeyViewT<T>::extract_json(iterator &iter) {
	auto tmpiter = iter;
	auto content = extract_string(tmpiter);
	auto res = string2jsonkey(std::move(content));
	auto ofs = keydata.size() - content.size();
	iter = begin() + ofs;
	return res;
}

template class KeyViewT<std::string_view>;
template class KeyViewT<std::string>;

}


