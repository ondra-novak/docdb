/*
 * json_map.h
 *
 *  Created on: 9. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_JSON_MAP_H_
#define SRC_DOCDBLIB_JSON_MAP_H_

#include "json_map_view.h"
#include "observable.h"

namespace docdb {

///JsonMapBase is universal lowlevel map which maps json value to json value
/**
 * JsonMapBase is intended to be used by other objects when they implementing their internals
 * As top level standalone object you should use JsonMap,
 */
class JsonMapBase: public JsonMapView {
public:

	using JsonMapView::JsonMapView;

	void set(Batch &b, const json::Value &key, const json::Value &value);
	void erase(Batch &b, const json::Value &key);
	void clear();

	KeySpaceID getKID() const;
};

///JsonMap - maps json value to a json value
/**
 * It is general purpose map. It also supports observers
 */
class JsonMap: public JsonMapBase {
public:

	JsonMap(DB db, const std::string_view &name);
	~JsonMap();

	void set(Batch &b, const json::Value &key, const json::Value &value);
	void set(const json::Value &key, const json::Value &value);
	void erase(Batch &b, const json::Value &key);
	void erase(const json::Value &key);


	template<typename Fn>
	auto addObserver(Fn &&fn) {
		return observers->addObserver(std::forward<Fn>(fn));
	}
	auto removeObserver(std::size_t h) {
		return observers->removeObserver(h);
	}

	struct AggregatorAdapter {
			using IteratorType = Iterator;
			using SourceType = JsonMap;

			static const DB &getDB(const SourceType &src) {return src.db;}
			template<typename Fn>
			static auto observe(SourceType &src, Fn &&fn) {
				return src.addObserver([fn = std::move(fn)](Batch &b, const json::Value &k, const json::Value &){
					return fn(b, std::initializer_list<json::Value>({k}));
				});
			}
			static void stopObserving(SourceType &src, typename Observable<Batch &, json::Value>::Handle h) {
				src.removeObserver(h);
			}
			static IteratorType find(const SourceType &src, const json::Value &key) {
				return src.find(key);
			}
			static IteratorType prefix(const SourceType &src, const json::Value &key) {
				return src.prefix(key);
			}
			static IteratorType range(const SourceType &src, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound ) {
				return src.range(fromKey, toKey, include_upper_bound);
			}
			static json::Value getKey(IteratorType &iter) {
				return json::Value(iter.key());
			}

		};



	using Obs = Observable<Batch &, json::Value, json::Value>;

protected:

	json::RefCntPtr<Obs> observers;



};


}



#endif /* SRC_DOCDBLIB_JSON_MAP_H_ */
