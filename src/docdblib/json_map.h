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

protected:

	Observable<Batch &, json::Value> observers;



};


}



#endif /* SRC_DOCDBLIB_JSON_MAP_H_ */
