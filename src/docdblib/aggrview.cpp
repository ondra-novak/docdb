/*
 * reduceview.cpp
 *
 *  Created on: 10. 12. 2020
 *      Author: ondra
 */
#include "aggrview.h"

#include "formats.h"
#include <imtjson/binjson.tcc>
#include "view_iterator.h"

namespace docdb {

AbstractAggregateView::AbstractAggregateView(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel  )
	:AbstractUpdatableView(viewMap.getDB(),name)
	,viewMap(viewMap)
	,name(name)
	,groupLevel(groupLevel)
	,keytmp(viewid.content())
	,translator(*this)
{
	viewMap.addObserver(this);
}

AbstractAggregateView::~AbstractAggregateView() {
	viewMap.removeObserver(this);
}

class AbstractAggregateView::KeyMapImpl: public KeyMapping {
public:
	KeyMapImpl(WriteBatch &b, DocDB::ViewIndexKey &rkey, std::string &val)
		:b(b),rkey(rkey),val(val) {rkeysz = rkey.size();}

	virtual void scan(const json::Value &result_key, const json::Value &value) const override {
		jsonkey2string(result_key, rkey);
		json2string({json::Value(),json::Value(static_cast<int>(ScanCommand::scan)),value}, val);
		putRecord();
	};
	virtual void scanPrefix(const json::Value &result_key, const json::Value &prefix, const json::Value &value) const override {
		jsonkey2string(result_key, rkey);
		json2string({json::Value(),json::Value(static_cast<int>(ScanCommand::scanPrefix)),prefix,value}, val);
		putRecord();
	}
	virtual void find(const json::Value &result_key ,const json::Value &key, const json::Value &value) const override {
		jsonkey2string(result_key, rkey);
		json2string({json::Value(),json::Value(static_cast<int>(ScanCommand::find)),key,value}, val);
		putRecord();
	}
	virtual void scanRange(const json::Value &result_key, const json::Value &lower_bound, const json::Value &upper_bound, Exclude exclude, const json::Value &value) const override {
		jsonkey2string(result_key, rkey);
		json2string({json::Value(),json::Value(static_cast<int>(ScanCommand::scanRange)),lower_bound, upper_bound, json::Value(exclude == exclude_end), value}, val);
		putRecord();
	}


protected:
	WriteBatch &b;
	DocDB::ViewIndexKey &rkey;
	std::string &val;
	std::size_t rkeysz;

	void putRecord() const {
		b.Put(rkey, val);
		rkey.resize(rkeysz);
		val.clear();
	}
};

void AbstractAggregateView::mapKey(const json::Value &key, const KeyMapping &mapping) const {
	if (groupLevel == 0) {
		mapping.scan(nullptr);
	} else if (key.type() == json::array) {
		mapping.scanPrefix(key.slice(0,groupLevel));
	} else {
		mapping.find(key);
	}
}

void AbstractAggregateView::updatedKey(WriteBatch &batch, const json::Value &key, bool) {
	KeyMapImpl mp(batch, keytmp, tmpval);
	mapKey(key, mp);
}


void AbstractAggregateView::addObserver(IKeyUpdateObserver *obs)  {
	std::unique_lock _(lock);
	keylistener.addObserver(obs);
}
void AbstractAggregateView::removeObserver(IKeyUpdateObserver *obs) {
	std::unique_lock _(lock);
	keylistener.removeObserver(obs);
}

void AbstractAggregateView::rebuild() {
	ViewIterator iter = viewMap.scan();
	WriteBatch b;
	std::string val;
	DocDB::ViewIndexKey rkey(viewid.content());
	KeyMapImpl mp(b, rkey, val);
	while (iter.next()) {
		mapKey(iter.key(), mp);
		if (b.ApproximateSize()>256000) {
			db.flushBatch(b);
			b.Clear();
		}
	}
	db.flushBatch(b);
}


void AbstractAggregateView::update() {

}

ViewIterator AbstractAggregateView::find(const json::Value &key, bool backward) {
	return ViewIterator(StaticView::find(key, backward),translator);
}

json::Value AbstractAggregateView::lookup(const json::Value &key) {
	auto iter = find(key);
	if (iter.next()) return iter.value();
	else return json::undefined;
}

ViewIterator AbstractAggregateView::scan() {
	return ViewIterator(StaticView::scan(), translator);
}

ViewIterator AbstractAggregateView::scanRange(const json::Value &from, const json::Value &to, bool exclude_end) {
	return ViewIterator(StaticView::scanRange(from, to, exclude_end),translator);

}

ViewIterator AbstractAggregateView::scanPrefix(const json::Value &prefix, bool backward) {
	return ViewIterator(StaticView::scanPrefix(prefix, backward),translator);

}

ViewIterator AbstractAggregateView::scanSource(const json::Value &args, const std::string_view &result_key) const {
	ScanCommand cmd = static_cast<ScanCommand>(args[1].getInt());
	json::Value param = args[2];
	if (!param.defined()) param = string2jsonkey(result_key.substr(viewid.length()));
	switch (cmd) {
		case ScanCommand::scan: return viewMap.scan();
		case ScanCommand::scanPrefix: return viewMap.scanPrefix(param, false);
		case ScanCommand::scanRange: return viewMap.scanRange(args[2], args[3], args[4].getBool());
		default: return viewMap.find(param, false);
	}
}


AggregateView::AggregateView(AbstractView &viewMap, const std::string &name, unsigned int groupLevel, AggrFn aggrFn)
	:AbstractAggregateView(viewMap, name, groupLevel)
	,aggrFn(aggrFn) {}

json::Value AggregateView::calc(ViewIterator &iter,const json::Value &custom) const {
	return aggrFn(iter, custom);
}
AggregateViewWithKeyMap::AggregateViewWithKeyMap(AbstractView &viewMap, const std::string &name, MapKeyFn mapKeyFn, AggrFn aggrFn)
	:AggregateView(viewMap,name,1,aggrFn),mapKeyFn(mapKeyFn) {}

void AggregateViewWithKeyMap::mapKey(const json::Value &key, const KeyMapping &keyMapEmit) const {
	return mapKeyFn(key, keyMapEmit);
}

json::Value AggregateCount::calc(ViewIterator &iter, const json::Value &) const {
	std::size_t cnt = 0;
	while (iter.next()) cnt++;
	return cnt;
}

AggregateReduce::AggregateReduce(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel, Operation op)
	:AbstractAggregateView(viewMap,name,groupLevel),op(op)
{}

json::Value AggregateReduce::calc(ViewIterator &iter, const json::Value &) const {
	json::Value res;
	if (iter.next()) {
		res = iter.value();
		if (res.type() == json::array) {
			std::vector<json::Value> data(res.begin(), res.end());
			while (iter.next()) {
				auto v = iter.value();
				std::size_t sz = data.size();
				if (sz < v.size()) data.resize(sz = v.size());
				for (std::size_t i = 0; i < sz; i++) {
					data[i] = op(data[i], v[i], i);
				}
			}
			res = json::Value(json::array, data.begin(), data.end(), [](const json::Value &v){return v;});
		} else {
			while (iter.next()) {
				res = op(res, iter.value(), 0);
			}
		}
	}
	return res;
}

json::Value AggregateReduce::sum(const json::Value &a, json::Value &b, unsigned int ){
	return a.getNumber()+b.getNumber();
}
json::Value AggregateReduce::max(const json::Value &a, json::Value &b, unsigned int ){
	return json::Value::compare(b,a) > 0?b:a;
}
json::Value AggregateReduce::min(const json::Value &a, json::Value &b, unsigned int ){
	return json::Value::compare(b,a) < 0?b:a;
}

json::Value AggregateSum::calc(ViewIterator &iter, const json::Value &) const {
	json::Value res;
	if (iter.next()) {
		res = iter.value();
		if (res.type() == json::array) {
			std::vector<double> data;
			data.reserve(res.size());
			for (json::Value v:res) data.push_back(v.getNumber());
			while (iter.next()) {
				auto v = iter.value();
				std::size_t sz = data.size();
				if (sz < v.size()) data.resize(sz = v.size(),0.0);
				for (std::size_t i = 0; i < sz; i++) {
					data[i] = data[i] + v[i].getNumber();
				}
			}
			res = json::Value(json::array, data.begin(), data.end(), [](double v){return json::Value(v);});
		} else {
			double sum = res.getNumber();
			while (iter.next()) {
				sum = sum + iter.value().getNumber();
			}
			res = sum;
		}
	}
	return res;

}

json::Value AggregateIntegerSum::calc(ViewIterator &iter, const json::Value &) const {
	json::Value res;
	if (iter.next()) {
		res = iter.value();
		if (res.type() == json::array) {
			std::vector<std::intptr_t> data;
			data.reserve(res.size());
			for (json::Value v:res) data.push_back(v.getInt());
			while (iter.next()) {
				auto v = iter.value();
				std::size_t sz = data.size();
				if (sz < v.size()) data.resize(sz = v.size(), 0);
				for (std::size_t i = 0; i < sz; i++) {
					data[i] = data[i] + v[i].getInt();
				}
			}
			res = json::Value(json::array, data.begin(), data.end(), [](double v){return json::Value(v);});
		} else {
			std::intptr_t sum = res.getInt();
			while (iter.next()) {
				sum = sum + iter.value().getInt();
			}
			res = sum;
		}
	}
	return res;

}

json::Value AbstractAggregateView::Translator::translate(const std::string_view &key, const std::string_view &value) const {
	json::Value args = string2json(value);
	if (args.type() != json::array || args.empty() || args[0].defined()) return args;

	std::unique_lock<std::mutex> _(owner.lock);
	ViewIterator iter = owner.scanSource(args, key);
	json::Value custom = args[args.size()-1];
	json::Value result;
	if (!iter.empty()) result = owner.calc(iter, custom);

	bool deleted;
	if (result.defined()) {
		json2string(result, tmp);
		b.Put(leveldb::Slice(key.data(), key.length()), tmp);
		tmp.clear();
		deleted = false;
	} else {
		b.Delete(leveldb::Slice(key.data(), key.length()));
		deleted = true;
	}

	if (!owner.keylistener.empty()) {
		auto k = string2jsonkey(key.substr(owner.viewid.length()));
		owner.keylistener.updatedKey(b, k, deleted);
	}
	owner.db.flushBatch(b);
	return result;
}


}

