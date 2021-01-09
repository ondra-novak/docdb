/*
 * filterview.cpp
 *
 *  Created on: 6. 1. 2021
 *      Author: ondra
 */

#include "filterview.h"

#include "formats.h"
namespace docdb {

FilterView::FilterView(const DB &db, const std::string_view &name)
:db(db),kid(this->db.allocKeyspace(KeySpaceClass::filterView, name))
{

}

json::Value FilterView::lookup(const std::string_view &docId) const {
	auto &buffer = DB::getBuffer();
	if (db.get(createKey(docId), buffer)) return string2json(buffer);
	else return json::undefined;
}

json::Value FilterView::find(const std::string_view &docid) const {
	return lookup(docid);
}

FilterView::Iterator FilterView::range(const std::string_view &fromKey,
		const std::string_view &toKey) const {
	return range(fromKey, toKey, false);
}

FilterView::Iterator FilterView::range(const std::string_view &fromKey,
		const std::string_view &toKey, bool include_upper_bound) const {
	bool bkw = fromKey > toKey;
	return Iterator(db.createIterator({createKey(fromKey), createKey(toKey), !bkw || include_upper_bound, bkw || include_upper_bound}));
}

FilterView::Iterator FilterView::prefix(const std::string_view &prefix) const {
	Key k1(createKey(prefix));
	Key k2(k1);
	k2.upper_bound();
	return Iterator(db.createIterator({k1, k2, false, true}));
}
FilterView::Iterator FilterView::prefix(const std::string_view &prefix, bool backward) const {
	Key k1(createKey(prefix));
	Key k2(k1);
	if (backward) k1.upper_bound(); else k2.upper_bound();
	return Iterator(db.createIterator({k1, k2, backward, !backward}));
}

FilterView::Iterator FilterView::scan() const {
	return Iterator(db.createIterator({Key(kid), Key(kid+1), false, true}));
}

FilterView::Iterator FilterView::scan(bool backward) const {
	return Iterator(db.createIterator({Key(kid+(backward?1:0)), Key(kid+(backward?0:1)), backward, !backward}));
}

FilterView::Iterator FilterView::scan(const std::string_view &fromDoc, bool backward) const {
	Key k1(createKey(fromDoc));
	Key k2(kid+(backward?0:1));
	return Iterator(db.createIterator({Key(kid+(backward?1:0)), Key(kid+(backward?0:1)), true, !backward}));
}

json::Value FilterView::parseValue(const std::string_view &value) {
	return string2json(value);
}

json::Value FilterView::extractSubValue(unsigned int index, const std::string_view &value) {
	return extract_subvalue(index, 	std::string_view(value));
}

Key FilterView::createKey(const std::string_view &doc) const {
	return Key(kid,doc);
}

bool FilterView::isDocumentInView(const std::string_view &docId) const {
	return db.get(createKey(docId),DB::getBuffer());
}



FilterView::Iterator::Iterator(Super &&src):Super(std::move(src)) {
}

KeyView FilterView::Iterator::global_key() {
	return Super::key();
}

json::Value FilterView::Iterator::key() const {
	return Super::key().content();
}

std::string_view FilterView::Iterator::id() const {
	return Super::key().content();
}

json::Value FilterView::Iterator::value() const {
	return string2json(std::string_view(Super::value()));
}

json::Value FilterView::Iterator::value(unsigned int index) const {
	return extract_subvalue(index, std::string_view(Super::value()));
}

const std::string& FilterView::createValue(const json::Value v) {
	std::string &buffer = DB::getBuffer();
	json2string(v, buffer);
	return buffer;

}

}
