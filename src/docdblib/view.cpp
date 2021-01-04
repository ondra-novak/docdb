/*
 * view.cpp
 *
 *  Created on: 3. 1. 2021
 *      Author: ondra
 */


#include "view.h"
#include "formats.h"

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


}

