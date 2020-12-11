/*
 * reduceview.cpp
 *
 *  Created on: 10. 12. 2020
 *      Author: ondra
 */
#include "aggrview.h"

#include "formats.h"
#include <imtjson/binjson.tcc>

namespace docdb {

AbstractAggregateView::AbstractAggregateView(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel  )
	:AbstractUpdatableView(viewMap.getDB(), ViewTools::getViewID(name))
	,viewMap(viewMap)
	,name(name)
	,groupLevel(groupLevel)
	,updated_rev(1)
	,last_update_rev(0)
	,keytmp(viewid.content())
{
	viewMap.addObserver(this);
}

AbstractAggregateView::~AbstractAggregateView() {
	viewMap.removeObserver(this);
}

AbstractAggregateView::KeyTransform AbstractAggregateView::mapKey(const json::Value &key) const {
	if (groupLevel == 0) {
		return {
			KeyTransform::scan, json::Value(), nullptr
		};
	} else if (key.type() == json::array) {
		return {
			KeyTransform::scanPrefix, json::Value(), key.slice(0, groupLevel)
		};
	} else {
		return {
			KeyTransform::find, json::Value(), key
		};
	}
}

void AbstractAggregateView::updatedKey(WriteBatch &batch, const json::Value &key, bool) {
	KeyTransform trns = mapKey(key);
	json2string(trns.keyResult, keytmp);
	tmpval.push_back(static_cast<char>(trns.cmd));
	json2string(trns.keyScan, tmpval);
	batch.Put(keytmp, tmpval);
	keytmp.resize(viewid.size());
	tmpval.clear();
	++updated_rev;
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
	while (iter.next()) {
		updatedKey(b, iter.key(), false);
		if (b.ApproximateSize()>256000) {
			db.flushBatch(b);
			b.Clear();
		}
	}
	db.flushBatch(b);
	update();
}

void AbstractAggregateView::update() {
	if (last_update_rev.load() != updated_rev.load()) {
		std::unique_lock _(lock);
		last_update_rev.store(updated_rev.load());
		WriteBatch b;
		ViewID itmk(viewid);
		std::string value;

		DocDB::ReduceIndexKey k(viewid.content());
		auto iter = db.mapScanPrefix(k, false);

		KeyTransform trns;


		while (iter.next()) {
			auto orig_key = iter.orig_key();
			auto str = iter.key();
			str = str.substr(k.content().length());
			trns.keyResult = string2json(str);
			auto val = iter.value();
			trns.cmd = static_cast<KeyTransform::Command>(val[0]);
			trns.keyScan = string2json(val.substr(1));
			updateRow(trns ,itmk, value, b);

			b.Delete(orig_key);
			db.flushBatch(b);
			b.Clear();

		}
	}

}

ViewIterator AbstractAggregateView::scanSource(const KeyTransform &trns) const {
	json::Value param = trns.keyScan;
	if (!param.defined()) param = trns.keyResult;
	switch (trns.cmd) {
		case KeyTransform::scan: return viewMap.scan();
		case KeyTransform::scanPrefix: return viewMap.scanPrefix(param, false);
		default:
		case KeyTransform::find: return viewMap.find(param, false);
	}
}

void AbstractAggregateView::updateRow(const KeyTransform &trns, ViewID &itmk, std::string &value, WriteBatch &b) {
	ViewIterator iter = scanSource(trns);

	auto itmksz = itmk.length();
	jsonkey2string(trns.keyResult, itmk);
	json::Value rval;
	if (!iter.empty()) rval = calc(iter);

	bool deleted;
	if (rval.defined()) {
		json2string(rval, value);
		b.Put(itmk, value);
		deleted = false;
	} else {
		b.Delete(itmk);
		deleted = true;
	}
	if (!keylistener.empty()) {
		keylistener.updatedKey(b, trns.keyResult, deleted);
	}
	itmk.resize(itmksz);
	value.clear();

}

AggregateView::AggregateView(AbstractView &viewMap, const std::string &name, unsigned int groupLevel, AggrFn aggrFn)
	:AbstractAggregateView(viewMap, name, groupLevel)
	,aggrFn(aggrFn) {}

json::Value AggregateView::calc(ViewIterator &iter) const {
	return aggrFn(iter);
}
AggregateViewWithKeyMap::AggregateViewWithKeyMap(AbstractView &viewMap, const std::string &name, MapKeyFn mapKeyFn, AggrFn aggrFn)
	:AggregateView(viewMap,name,1,aggrFn),mapKeyFn(mapKeyFn) {}

AggregateViewWithKeyMap::KeyTransform AggregateViewWithKeyMap::mapKey(const json::Value &key) const {
	return mapKeyFn(key);
}

json::Value AggregateCount::calc(ViewIterator &iter) const {
	std::size_t cnt = 0;
	while (iter.next()) cnt++;
	return cnt;
}

AggregateReduce::AggregateReduce(AbstractUpdatableView &viewMap, const std::string &name, unsigned int groupLevel, Operation op)
	:AbstractAggregateView(viewMap,name,groupLevel),op(op)
{}

json::Value AggregateReduce::calc(ViewIterator &iter) const {
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

json::Value AggregateSum::calc(ViewIterator &iter) const {
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

json::Value AggregateIntegerSum::calc(ViewIterator &iter) const {
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

}


