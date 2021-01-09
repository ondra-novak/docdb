/*
 * aggregator_view.h
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_AGGREGATOR_VIEW_H_
#define SRC_DOCDBLIB_AGGREGATOR_VIEW_H_
#include "json_map_view.h"

namespace docdb {

///Interface passed into KeyMapFn for store key mapping
class IMapKey {
public:
	///For given key, perform aggregation using find() command
	/**
	 * @param resultKey result key - under which key is aggregated result stored
	 * @param srchKey search key - what to search in the source view
	 * @param value - value associated with aggregation, passed to aggregator
	 */
	virtual void find(const json::Value &resultKey, const json::Value &srchKey, const json::Value &value = json::undefined) = 0;
	///For given prefix, perform aggregation using prefix() command
	/**
	 * @param resultKey result key - under which key is aggregated result stored
	 * @param srchKey search key - what to search in the source view - prefix is searched
	 * @param value - value associated with aggregation, passed to aggregator
	 */
	virtual void prefix(const json::Value &resultKey, const json::Value &srchKey, const json::Value &value = json::undefined) = 0;
	///Maps a range for given key
	/**
	 * @param resultKey result key - under which key is aggregated result stored
	 * @param fromKey starting key
	 * @param toKey ending key
	 * @param include_upper_bound set true to include upper bound
	 * @param value - value associated with aggregation, passed to aggregator
	 */
	virtual void range(const json::Value &resultKey, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound = false, const json::Value &value = json::undefined) = 0;
};

template<typename Adapter>
class AggregatorView: public JsonMapView {
public:

	using Super = JsonMapView;
	using SrcIterator = typename Adapter::IteratorType;
	using Source = typename Adapter::SourceType;
	///Mapping function
	using KeyMapFn = std::function<void(json::Value, IMapKey &)>;
	using AggregatorFn = std::function<json::Value(SrcIterator &, const json::Value &)>;

	class Iterator: public Super::Iterator {
	public:
		Iterator(JsonMapView::Iterator &&iter, AggregatorView &owner);

		bool next();
		bool peek();

		///Retrieves value
		json::Value value() const;
		///Retrieves single value of multicolumn value
		json::Value value(unsigned int index) const;

	protected:
		AggregatorView &owner;
		json::Value valueCache;
		bool prepareValue();
	};

	AggregatorView(Source &src, const std::string_view &name, KeyMapFn &&keyMapFn, AggregatorFn &&aggr);
	~AggregatorView();
	json::Value lookup(const json::Value &key, bool set_docid = false) const;
	json::Value find(json::Value key) const;
	Iterator range(json::Value fromKey, json::Value toKey) const;
	Iterator range(json::Value fromKey, json::Value toKey, bool include_upper_bound) const;
	Iterator prefix(json::Value key) const;
	Iterator prefix(json::Value key, bool backward) const;
	Iterator scan() const;
	Iterator scan(bool backward) const;
	Iterator scan(json::Value fromKey, bool backward) const;

	void rebuild();

protected:
	Source &src;
	std::size_t regHandle;
	KeyMapFn keyMapFn;
	AggregatorFn aggrFn;

	static const char srchFind = -1;
	static const char srchPrefix = -2;
	static const char srchRange = -3;

	template<typename Cont>
	void onKeyChange(Batch &b, const Cont &keys);
	json::Value runAggregator(const KeyView &key, const std::string_view &value);
	json::Value runAggrFn(SrcIterator &&iter, const json::Value &custom);

	class MapKeySvc: public IMapKey {
	public:
		MapKeySvc(Batch &b,Key &&key):b(b),key(std::move(key)) {}

		virtual void find(const json::Value &resultKey, const json::Value &srchKey, const json::Value &value = json::undefined) override {
			invalidateKey(resultKey, srchFind, srchKey, value);
		}
		virtual void prefix(const json::Value &resultKey, const json::Value &srchKey, const json::Value &value = json::undefined) override {
			invalidateKey(resultKey, srchPrefix, srchKey, value);
		}
		virtual void range(const json::Value &resultKey, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound = false, const json::Value &value = json::undefined) override {
			invalidateKey(resultKey, srchRange, {fromKey, toKey, include_upper_bound}, value);
		}


	protected:
		Batch &b;
		Key &&key;

		void invalidateKey(const json::Value &key, char op, json::Value args, json::Value custom);
	};

};



template<typename Adapter>
inline AggregatorView<Adapter>::AggregatorView(Source &source,
		const std::string_view &name, KeyMapFn &&keyMapFn,
		AggregatorFn &&aggr)
:JsonMapView(Adapter::getDB(source), name)
,src(source)
,keyMapFn(std::move(keyMapFn))
,aggrFn(std::move(aggr))
{
	regHandle = Adapter::observe(src, [&](Batch &b, const auto &keyCont){
		this->onKeyChange(b, keyCont);return true;
	});
}


template<typename Adapter>
inline AggregatorView<Adapter>::~AggregatorView() {
	Adapter::stopObserving(src, regHandle);
}

template<typename Adapter>
template<typename Cont>
inline void AggregatorView<Adapter>::onKeyChange(Batch &b, const Cont &cont) {
	MapKeySvc mapsvc(b, Key(kid));
	for (json::Value key: cont) {
		keyMapFn(key, mapsvc);
	}
}


template<typename Adapter>
inline void AggregatorView<Adapter>::MapKeySvc::invalidateKey(
		const json::Value &key, char op, json::Value args, json::Value custom) {
	auto &buffer = DB::getBuffer();
	buffer.push_back(op);
	createValue(args, buffer);
	createValue(custom, buffer);
	this->key.append(key);
	b.Put(this->key, buffer);
	this->key.clear();
}


template<typename Adapter>
inline AggregatorView<Adapter>::Iterator::Iterator(
		JsonMapView::Iterator &&iter, AggregatorView &owner)
:JsonMapView::Iterator(std::move(iter)), owner(owner) {}



template<typename Adapter>
inline bool AggregatorView<Adapter>::Iterator::next() {
	while (next()) {
		if (prepareValue()) {
			return true;
		}
	}
	return false;
}


template<typename Adapter>
inline bool AggregatorView<Adapter>::Iterator::peek() {
	while (peek()) {
		if (prepareValue()) {
			return true;
		} else {
			next();
		}
	}
	return false;
}

template<typename Adapter>
inline json::Value AggregatorView<Adapter>::Iterator::value() const {
	return valueCache;
}


template<typename Adapter>
inline json::Value docdb::AggregatorView<Adapter>::Iterator::value(
		unsigned int index) const {
	return valueCache[index];
}


template<typename Adapter>
inline bool AggregatorView<Adapter>::Iterator::prepareValue() {
	auto val = ::docdb::Iterator::value();
	if (val.empty()) return false;
	if (val[0]<0) {
		valueCache = owner.runAggregator(this->global_key(), val);
	} else {
		valueCache = JsonMapView::Iterator::value();
	}
	return valueCache.defined();
}

template<typename Adapter>
inline json::Value AggregatorView<Adapter>::runAggregator(const KeyView &key, const std::string_view &value) {
	Batch b;
	std::string_view pval(value);
	char cmd = pval[0];pval = pval.substr(1);
	json::Value args = parseValue(std::move(pval));
	json::Value custom = parseValue(std::move(pval));
	json::Value res;
	switch (cmd) {
		case srchFind: res =runAggrFn(Adapter::find(src, args), custom);break;
		case srchPrefix: res =runAggrFn(Adapter::prefix(src, args), custom);break;
		case srchRange: res =runAggrFn(Adapter::range(src, args[0], args[1], args[2].getBool()), custom);break;
	}
	if (res.defined()) {
		auto &buff = DB::getBuffer();
		createValue(res, buff);
		b.Put(key, buff);
	} else {
		b.Delete(key);
	}
	db.commitBatch(b);
	return res;
}

template<typename Adapter>
inline json::Value docdb::AggregatorView<Adapter>::runAggrFn(SrcIterator &&iter,
		const json::Value &custom) {
	if (iter.empty()) return json::undefined;
	else return aggrFn(iter, custom);
}


}


#endif /* SRC_DOCDBLIB_AGGREGATOR_VIEW_H_ */
