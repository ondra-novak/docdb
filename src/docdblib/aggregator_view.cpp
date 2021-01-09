/*
 * aggregator_view.cpp
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#include "aggregator_view.h"
#include "doc_store_index.h"


namespace docdb {

template class AggregatorView<DocStoreIndex::AggregatorAdapter>;
template class AggregatorView<DocStore::AggregatorAdapter>;

static DocStore *idx;
static AggregatorView<DocStore::AggregatorAdapter> test1(*idx,
"aggr1",
[](const json::Value &key, IMapKey &mk){},
[](DocStore::Iterator &iter, json::Value v) {return v;});

}
