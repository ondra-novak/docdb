/*
 * doc_store.h
 *
 *  Created on: 26. 12. 2020
 *      Author: ondra
 */
#include <docdblib/json_map.h>
#include <condition_variable>

#include "incremental_store.h"
#include "observable.h"
#ifndef SRC_DOCDBLIB_DOC_STORE_H_
#define SRC_DOCDBLIB_DOC_STORE_H_
#include "db.h"
#include "iterator.h"
#include "document.h"
#include "changesiterator.h"

namespace docdb {

struct DocStore_Config {


	///Specifies maximum count of entries in revisition history
	/**
	 * Revision history is required to support replication. The number specifies maximum
	 * updates after the document cannot be replicated back to the
	 * source document store. Note that each entry takes at least 10 bytes.
	 */
	std::size_t rev_history_length = 100;

	///Defines timestamp function.
	/**
	 * If set to nullptr, it uses time in milliseconds as timestamp
	 */
	Timestamp (*timestampFn)() = nullptr;
};


class DocStoreView {
public:

	using DocID=json::Value;

	DocStoreView(const DB &db, const std::string_view &name);

	DocStoreView(const DB &db, IncrementalStoreView &incview, const std::string_view &name);

	///allows to change snapshot
	DocStoreView(const DocStoreView &source, DB snapshot);

	DocStoreView(const DocStoreView &other);

	virtual ~DocStoreView();

	class Iterator;
	class ChangesIterator;

	///Retrieve document ready for replication
	/**
	 * Retrieve document in form which can be used to replicate
	 * @param docId document id
	 * @return replication form of the document
	 *
	 * @note if document doesn't exists, it is generated empty result with deleted flag and
	 * zero revision history, which can be used to indicate, that document doesn't exist
	 */
	DocumentRepl replicate_get(const DocID &docId) const;
	///Retrieve document ready for the modify
	/**
	 *
	 * @param docId document id
	 * @return document. If document is deleted and has field "rev" equal to zero, it probably
	 * doesn't exist
	 */
	Document get(const DocID &docId) const;

	///Receives document latest revision
	/**
	 * @param docId document id
	 * @return document revision. If document doesn't exist, returns 0;
	 * @note works even if document is deleted. Function is much faster, then receiving whole
	 * document and retrieve the latest revision id
	 */
	DocRevision getRevision(const DocID &docId) const;

	json::Value getRevisions(const DocID &docId) const;

	///Retrieves document header
	/**
	 * Header contains minimal set of informations. It is array, where the first field is sequence ID and second field is top revision
	 * @param docId
	 * @param isdel
	 * @return
	 */
	json::Value getDocHeader(const DocID &docId, bool &isdel) const;



	enum Status {
		///document doesn't exists
		not_exists,
		///document exists
		exists,
		///document existed, but has been deleted
		deleted
	};

	///Retrieve document status
	/**
	 * @param docId document id
	 * @return status of the document
	 *
	 * @note function is faster than receive whole document and explore its state.
	 */
	Status getStatus(const DocID &docId) const;

	///Scans whole store
	/**
	 * @return iterator
	 * @note only live documents are included (not deleted)
	 */
	Iterator scan() const;

	///Scans whole store
	/**
	 * @param backward scan backward
	 * @return iterator
	 * @node only live documents are included (not deleted)
	 */
	Iterator scan(bool backward) const;

	///Scans range of documents
	/**
	 * @param from from document
	 * @param to to document
	 * @param include_upper_bound set true to include upper bound of the range
	 * @return iterator - only live documents are included (not deleted)
	 */
	Iterator range(const DocID &from, const DocID &to, bool include_upper_bound = false) const;

	///Scans for prefix
	/**
	 * Returns iterator of live documents taht match specified prefix
	 * @param prefix prefix
	 * @param backward iterate backward
	 * @return iterator - only live documents are included (not deleted)
	 */
	Iterator prefix(const DocID &prefix, bool backward = false) const;

	///Scans all deleted documents
	/**
	 * @return iterator - only deleted documents are included
	 */
	Iterator scanDeleted() const;

	///Scans deleted documents that match specified prefix
	/**
	 * @param prefix prefix
	 * @return iterator - only deleted documents are included
	 */
	Iterator scanDeleted(const DocID &prefix) const;

	///Scans changes from given sequence number
	/**
	 * @param from starting sequence number, it is excluded from the result. For the first
	 * call, use 0. For futhrer calls, you have to specify seqID of the last seen result
	 *
	 * @return iterator - includeds all documents, live and deleted. Result is ordered in chronologic order
	 */
	ChangesIterator scanChanges(SeqID from) const;


	DB getDB() const {return incview->getDB();}


	DocStoreView getSnapshot() const;

	template<typename Fn>
	auto addObserver(Fn &&fn) {
		return observable->addObserver(std::forward<Fn>(fn));
	}
	void removeObserver(std::size_t h) {
		return observable->removeObserver(h);
	}

protected:
	//keyspace id, graveyard key space
	IncrementalStoreView *incview;
	JsonMapBase active;
	JsonMapBase erased;
	union {
		IncrementalStoreView iview;
		IncrementalStore istore;
	};

	using Obs = Observable<Batch &, const DocumentBase &>;
	json::RefCntPtr<Obs> observable;

	static const int indexID = 0;
	static const int indexDeleted = 1;
	static const int indexTimestamp = 2;
	static const int indexContent = 3;

	static const int hdrSeq = 0;
	static const int hdrRevisions = 1;
};

class DocStoreView::Iterator: public JsonMapView::Iterator {
public:
	using Super = JsonMapView::Iterator;

	Iterator(const IncrementalStoreView& incview, Super &&src);

	///Retrieve current document id
	/**@return current document id. Returned string_view is valid until next() is called */
	json::Value id() const;
	///Retrieve document data (content of document)
	/**
	 * @return document content (user data)
	 * @note If you need to read multiple document attributes, it is much faster to use get()
	 */
	json::Value content() const;
	///Retrieve deleted flag (whether document is deleted)
	/**
	 * @retval true document is deleted
	 * @retval false document is not deleted
	 * @note If you need to read multiple document attributes, it is much faster to use get()
	 */
	bool deleted() const;
	///Retrieve sequence ID from the incremental store
	/**
	 * @return document's sequence id
	 */
	SeqID seqId() const;
	///Retrieve document revision
	/**
	 * @return document's last revision id
	 */
	DocRevision revision() const;
	///Retrieve whole document
	/**
	 * @return whole document object
	 */
	Document get() const;
	///Retrieve whole document prepared for replication
	/**
	 *
	 * @return whole document object prepared for replication
	 */
	DocumentRepl replicate_get() const;

	bool next();
	bool peek();

protected:
	IncrementalStoreView incview;
	mutable json::Value cache;
	mutable json::Value hdr;
	const json::Value &getDocContent() const;
	const json::Value &getDocHdr() const;


};


class DocStoreView::ChangesIterator: public IncrementalStoreView::Iterator {
public:
	using Super = IncrementalStoreView::Iterator;

	ChangesIterator(const DocStoreView &store, Super &&iter);

	///Retrieve current document id
	/**
	 * @return document id
	 *
	 * @note if you need more attributes, it is much faster to use get()
	 */
	DocID id() const;
	///Retrieve content of document (user data)
	/**
	 * @return content of the document
	 * @note if you need more attributes, it is much faster to use get()
	 *
	 */
	json::Value content() const;
	///Retrieve deleted flag (whether document is deleted)
	/**
	 * @retval true document is deleted
	 * @retval false document is not deleted
	 * @note function actually calls get(), retrieves deleted flag and throws rest of the object
	 */
	bool deleted() const;
	///Retrieve document revision
	/**
	 * @return document revision
	 * @note function actually calls get(), retrieves revision and throws rest of the object
	 */
	DocRevision revision() const;
	///Retrieve whole document
	Document get() const;
	///Retrieve whole document prepared for replication
	DocumentRepl replicate_get() const;


	bool next();
	bool peek();

protected:
	mutable json::Value cache;
	const json::Value getData() const;
	DocStoreView snapshot;
};



///Document store - ideal to store documents
/**
 * Document store stores documents. Each document is stored under a name which is ordinary
 * string. Document itself is JSON value which is put to the envelope, which carries
 * several attributes, such a revision id, timestamp and whether document is deleted or not.
 *
 * You can read history if updates, and replicate content of store to a different store (which
 * can be carried through various media, such a network).
 */
class DocStore: public DocStoreView {
public:
	using Super = DocStoreView;

	///Inicialize document store
	/**
	 * @param db database object. It should not be snapshot, unless you only need to read.
	 * However, there is much lighter object DocStoreView which can be used to read
	 * the stire
	 * @param name name of the store
	 * @param cfg configuration
	 */
	DocStore(const DB &db, const std::string &name, const DocStore_Config &cfg);


	virtual ~DocStore();

	bool replicate_put(Batch &b, const DocumentRepl &doc);
	bool put(Batch &b, const Document &doc);

	///Replicate document to this storage
	/**
	 * @param doc document ready to be replicated. You need use replicate_get to receive
	 * this document
	 * @retval true successfully replicated
	 * @retval false failed, conflict
	 */
	bool replicate_put(const DocumentRepl &doc);

	///Put document to the database
	/**
	 * @param doc document to put. The field "rev" must contain the revision id of the current
	 * stored revision to indicate, that new update was created from the currently stored source.
	 * To put brand new document, the field must be zero. If this expectation is not met,
	 * the function fails indicating conflict
	 * @retval true successfully put or updated
	 * @retval false conflict
	 */
	bool put(const Document &doc);

	///Erase document
	/** Function is equivalent to put document with deleted flag. Deleted documents are
	 * tombstoned in graveyard, with status "deleted". This allows to replicate the update,
	 * however, the deleted document is still occupies a space in the db.
	 *
	 * @param id id of document
	 * @param rev current revision
	 * @retval true success
	 * @retval false conflict
	 */
	bool erase(const std::string_view &id, const DocRevision &rev);

	///Purges document from the database
	/** Purge perform actual delete, reclaiming all occupied space. However because
	 * the operation cannot be replicated, it is recommended to purge documents, that
	 * has been delete before a long time, when there is zero possibility, that its
	 * deleted status was not replicated somewhere.
	 *
	 * @note Support of replication is necesery to update indexes and views. When document
	 * is purged, there is no way to propagate this action to all indexes. You can only
	 * purge documents manually.
	 *
	 * @param id document to purge
	 * @retval true success
	 * @retval false document doesn't exist
	 */
	bool purge(const std::string_view &id);


	///Purges document
	/**
	 * Function works same way asi purge/1, only checks for revision
	 * @param id document id
	 * @param rev revision
	 * @retval true purged
	 * @retval false document doesn't exist on specified revision
	 */
	bool purge(const std::string_view &id, const DocRevision &rev);

	SeqID getSeq() const {return istore.getSeq();}

	struct AggregatorAdapter {
		using IteratorType = Iterator;
		using SourceType = DocStore;

		static DB getDB(const SourceType &src) {return src.getDB();}
		template<typename Fn>
		static auto observe(SourceType &src, Fn &&fn) {
			return src.addObserver([fn = std::forward<Fn>(fn)](Batch &b, const DocumentBase &doc){
				return fn(b, std::initializer_list<json::Value>({doc.id}));
			});
		}
		static void stopObserving(SourceType &src, std::size_t h) {
			src.removeObserver(h);
		}
		static json::Value getKey(IteratorType &iter) {
			return json::Value(iter.id());
		}

		static IteratorType find(const SourceType &src, const json::Value &key) {
			return src.range(key, key, true);
		}
		static IteratorType prefix(const SourceType &src, const json::Value &key) {
			return src.prefix(key);
		}
		static IteratorType range(const SourceType &src, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound ) {
			return src.range(fromKey, toKey, include_upper_bound);
		}

	};

	static Timestamp defaultTimestampFn();

protected:
	unsigned int revHist;
	std::string wrbuff;
	Timestamp (*timestampFn)();
	void writeHeader(Batch &b, const json::Value &docId, bool replace,
			bool wasdel, bool isdel, SeqID seq, json::Value rev);
};



} /* namespace docdb */

#endif /* SRC_DOCDBLIB_DOC_STORE_H_ */
;

