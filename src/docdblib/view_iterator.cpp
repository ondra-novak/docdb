/*
 * view_iterator.cpp
 *
 *  Created on: 13. 12. 2020
 *      Author: ondra
 */


#include "view_iterator.h"
#include "formats.h"
#include <imtjson/binjson.h>

namespace docdb {


ViewIterator::ViewIterator(MapIterator &&iter, IValueTranslator &translator)
		:MapIterator(std::move(iter)),translator(translator) {

}
ViewIterator::ViewIterator(ViewIterator &&iter, IValueTranslator &translator)
		:MapIterator(std::move(iter)),translator(translator) {
}

json::Value ViewIterator::key() const {
	if (need_parse) parseKey();
	return ukey;
}

bool ViewIterator::next() {
	need_parse = true;
	while (MapIterator::next()) {
		auto key = MapIterator::orig_key();
		val = translator.translate(std::string_view(key.data(), key.size()), MapIterator::value());
		if (val.defined()) return true;
	}
	return false;
}


json::Value ViewIterator::value() const {
	return val;
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

json::Value DefaultJSONTranslator::translate(const std::string_view &, const std::string_view &value) const {
	return string2json(std::string_view(value));
}

DefaultJSONTranslator& DefaultJSONTranslator::getInstance() {
	static DefaultJSONTranslator translator;
	return translator;
}

}

