/*
 * doc_store_index.h
 *
 *  Created on: 5. 1. 2021
 *      Author: ondra
 */

#ifndef SRC_DOCDBLIB_DOC_STORE_INDEX_H_
#define SRC_DOCDBLIB_DOC_STORE_INDEX_H_
#include "doc_store.h"
#include "view.h"
#include "emitfn.h"

namespace docdb {



class DocStoreIndex: public UpdatableView<DocStoreIndex> {
public:

	///Initialize doc store index
	/**
	 * @param db database object
	 * @param name name of the index
	 * @param revision content revision. Revision is stored and checked. If revision doesn't
	 *  match, content of the view is destroyed and reindexed
	 * @param indexFn indexing function
	 *
	 * @note object is not connected with document store. You need to call setSource to connect
	 */
	DocStoreIndex(const DB &db, const std::string_view &name, std::size_t revision, IndexFn &&indexFn);
	///Initialize index and connect it to document store
	/**
	 * @param store source document store
	 * @param name name of the index
	 * @param revision content revision. Revision is stored and checked. If revision doesn't
	 *  match, content of the view is destroyed and reindexed
	 * @param indexFn indexing function
	 *
	 * @note object is automatically connected.
	 */
	DocStoreIndex(const DocStore &store, const std::string_view &name, std::size_t revision, IndexFn &&indexFn);

	///Specifies document source
	/**
	 * @param store reference to document source. Reference must remain valid during updates
	 *
	 * @note it is allowed to specify source from different database.
	 */
	void setSource(const DocStore &store);

	void update();

	void indexDoc(const Document &doc);
	///clears the index
	void clear();
	void purgeDoc(std::string_view docid);


protected:

	static const int index_revision = 0;
	static const int index_sequence = 1;

	std::size_t revision;
	IndexFn indexFn;
	const DocStore *source = nullptr;
	SeqID lastSeqID = 0;


	void indexDoc(IndexBatch &emitBatch, const Document &doc);
};


}



#endif /* SRC_DOCDBLIB_DOC_STORE_INDEX_H_ */
