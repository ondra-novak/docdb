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
#include "json_map_view.h"
#include "emitfn.h"

namespace docdb {

///Maps document do a key-value pair. Key must be unique
/**
 * Object must have a MapFn (map function) which maps document do key-value pair. Function is called
 * also for deleted documents so map function can delete generated keys
 *
 * In compare with DocStoreIndex, this map is not directly connected to documents. Map function
 * is responsible to clear records of deleted documents. Map takes less space then Index, but cannot be used
 * for general purpose.
 *
 * Index function is called for every document (even if the document is marked as deleted). Function can generate
 * one or multiple keys. However function must somehow track which keys has been created, to be
 * able delete keys when document is deleted (so it must be somehow deterministic).
 *
 * Recomended way is to map document ID to a key and document content to a value. When document is deleted,
 * it is easy to recreate key from the document ID.
 *
 * To delete key, set the value to json::undefined
 *
 *
 */
class DocStoreMap: public UpdatableMap<DocStoreMap> {
public:

	using Super = UpdatableMap<DocStoreMap>;

	DocStoreMap(const DB &db, const std::string_view &name, std::size_t revision, IndexFn &&mapFn);
	DocStoreMap(const DocStore &store, const std::string_view &name, std::size_t revision, IndexFn &&mapFn);
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
	IndexFn mapFn;
	const DocStore *source = nullptr;
	SeqID lastSeqID = 0;
	std::mutex lock;

	void indexDoc(IndexBatch &emitBatch, const Document &doc);


};

}


#endif /* SRC_DOCDBLIB_DOC_STORE_MAP_H_ */
