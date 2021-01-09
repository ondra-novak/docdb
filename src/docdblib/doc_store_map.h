/*
 * doc_store_valuemap.h
 *
 *  Created on: 8. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_DOC_STORE_MAP_H_
#define SRC_DOCDBLIB_DOC_STORE_MAP_H_
#include "doc_store.h"
#include "document.h"
#include "filterview.h"


namespace docdb {

class DocStoreMap: public UpdatableFilterView<DocStoreMap> {
public:
	using MapFn = std::function<json::Value(const Document &)>;

	DocStoreMap(const DB &db, const std::string_view &name, std::size_t revision, MapFn &&mapFn);
	DocStoreMap(const DocStore &store, const std::string_view &name, std::size_t revision, MapFn &&mapFn);
	void setSource(const DocStore &store);

	void update();

	void mapDoc(const Document &doc);
	///clears the index
	void clear();
	void purgeDoc(std::string_view docid);
protected:
	static const int index_revision = 0;
	static const int index_sequence = 1;

	std::size_t revision;
	MapFn mapFn;
	const DocStore *source = nullptr;
	SeqID lastSeqID = 0;

	void indexDoc(Batch &emitBatch, const Document &doc);


};

}


#endif /* SRC_DOCDBLIB_DOC_STORE_MAP_H_ */
