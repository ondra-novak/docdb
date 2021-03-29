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

class JsonMap: public JsonMapView {
public:

	using JsonMapView::JsonMapView;

	void set(Batch &b, const json::Value &key, const json::Value &value);
	void set(const json::Value &key, const json::Value &value);
	void erase(Batch &b, const json::Value &key);
	void erase(const json::Value &key);
	void clear();


	template<typename Fn>
	auto addObserver(Fn &&fn) {
		return observers.addObserver(std::forward<Fn>(fn));
	}
	auto removeObserver(std::size_t h) {
		return observers.removeObserver(h);
	}

	struct AggregatorAdapter {
			using IteratorType = Iterator;
			using SourceType = JsonMap;

			static const DB &getDB(const SourceType &src) {return src.db;}
			template<typename Fn>
			static auto observe(SourceType &src, Fn &&fn) {
				return src.addObserver([fn = std::move(fn)](Batch &b, const json::Value &k){
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


protected:

	Observable<Batch &, json::Value> observers;



};


}



#endif /* SRC_DOCDBLIB_JSON_MAP_H_ */
