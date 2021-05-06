/*
 * doc_store.h
 *
 *  Created on: 26. 12. 2020
 *      Author: ondra
 */
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

};

class DocStoreViewBase {
public:

	///Inicialize document store
	/**
	 * @param db the database object
	 * @param name name of the document store
	 * @param cfg document store configuration
	 */
	DocStoreViewBase(const IncrementalStoreView& incview, const std::string_view &name);

	///Allows change incremental store view (for different snapshot);
	DocStoreViewBase(const DocStoreViewBase &src, const IncrementalStoreView& incview);

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
	DocumentRepl replicate_get(const std::string_view &docId) const;

	///Retrieve document ready for the modify
	/**
	 *
	 * @param docId document id
	 * @return document. If document is deleted and has field "rev" equal to zero, it probably
	 * doesn't exist
	 */
	Document get(const std::string_view &docId) const;

	///Receives document latest revision
	/**
	 * @param docId document id
	 * @return document revision. If document doesn't exist, returns 0;
	 * @note works even if document is deleted. Function is much faster, then receiving whole
	 * document and retrieve the latest revision id
	 */
	DocRevision getRevision(const std::string_view &docId) const;

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
	Status getStatus(const std::string_view &docId) const;

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
	Iterator range(const std::string_view &from, const std::string_view &to, bool include_upper_bound = false) const;

	///Scans range of documents
	/**
	 * @param from from document
	 * @param to to document
	 * @param exclude_begin exclude first document if equals to "from"
	 * @param exclude_end exclude last document if equals to "to"
	 * @return iterator - only live documents are included (not deleted)
	 */
	Iterator range_ex(const std::string_view &from, const std::string_view &to, bool exclude_begin = false, bool exclude_end = false) const;

	///Scans for prefix
	/**
	 * Returns iterator of live documents taht match specified prefix
	 * @param prefix prefix
	 * @param backward iterate backward
	 * @return iterator - only live documents are included (not deleted)
	 */
	Iterator prefix(const std::string_view &prefix, bool backward = false) const;

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
	Iterator scanDeleted(const std::string_view &prefix) const;

	///Scans changes from given sequence number
	/**
	 * @param from starting sequence number, it is excluded from the result. For the first
	 * call, use 0. For futhrer calls, you have to specify seqID of the last seen result
	 *
	 * @return iterator - includeds all documents, live and deleted. Result is ordered in chronologic order
	 */
	ChangesIterator scanChanges(SeqID from) const;


	DB getDB() const {return incview.getDB();}

	Key createKey(const json::Value &docId, bool deleted) const;


protected:
	struct DocumentHeaderData{
		char seqIdDel[8];
		char revList[][8];
		SeqID getSeqID() const;
		bool isDeleted() const;
		DocRevision getRev(unsigned int idx) const;

		static const DocumentHeaderData *map(const std::string_view &buffer, unsigned int &revCount);
	};

	const DocumentHeaderData* findDoc(const DB &snapshot,
						const json::Value &docId, unsigned int &revCount) const;

	const DocumentHeaderData* findDoc(const json::Value &docId, unsigned int &revCount) const;


	static json::Value parseRevisions(const DocumentHeaderData *hdr, unsigned int revCount);


protected:
	//keyspace id, graveyard key space
	const IncrementalStoreView& incview;
	KeySpaceID kid, gkid;


	static const int index_docId = 0;
	static const int index_timestamp = 1;
	static const int index_content = 2;
};

class DocStoreViewBase::Iterator: public ::docdb::Iterator {
public:
	using Super = ::docdb::Iterator;
	Iterator(const IncrementalStoreView& incview, Super &&src);

	///Retrieve current document id
	/**@return current document id. Returned string_view is valid until next() is called */
	std::string_view id() const;
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

protected:
	IncrementalStoreView incview;
};


class DocStoreViewBase::ChangesIterator: public IncrementalStoreView::Iterator {
public:
	using Super = IncrementalStoreView::Iterator;

	ChangesIterator(const DocStoreViewBase &docStore, Super &&iter);

	///Retrieve current document id
	/**
	 * @return document id
	 *
	 * @note if you need more attributes, it is much faster to use get()
	 */
	std::string id() const;
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
	///Retrieve sequence ID from the incremental store
	/**
	 * @return sequence id
	 */
	SeqID seqId() const;
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


	json::Value doc() const = delete;
protected:
	DocStoreViewBase docStore;

};

///Read only view for document store
/**
 * The view can only read documents, not write. You can supply database object or snapshot.
 */
class DocStoreView: public DocStoreViewBase {
public:
	using Super = DocStoreViewBase;
	///Initialize document store view
	/**
	 *
	 * @param db database object or snapshot.
	 * @param name name of the document store
	 * @param cfg configuration
	 */
	DocStoreView(DB &db, const std::string_view &name);
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
class DocStore: public DocStoreViewBase {
public:
	using Super = DocStoreViewBase;

	///Inicialize document store
	/**
	 * @param db database object. It should not be snapshot, unless you only need to read.
	 * However, there is much lighter object DocStoreView which can be used to read
	 * the stire
	 * @param name name of the store
	 * @param cfg configuration
	 */
	DocStore(DB &db, const std::string &name, const DocStore_Config &cfg);

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

	///Listen for changes
	/** Called from separated thread listens for changes. The function returns when
	 * change is detected or timeout ellapsed whichever comes first
	 *
	 * @param id last seen sequence id. For the first time, use seqID returned by last item received by iterator
	 * created by function scanChanges(). Subsequent calls should use value returned by previous call
	 * @param period period to wait
	 * @return Function returns last sequence ID. This value should be above the first argument if
	 * there is a change. It returns same sequence id, when time ellapsed. It returns 0, when
	 * no waiting is longer possible - because the object is being destroyed. In this case, the
	 * caller must not call any function of this object.
	 *
	 * @note because function cancelListen can be used anytime, the function can return earlier then
	 * specified timeout. In this case returned value will be equal to specified sequenced ID
	 */
	template<typename _Rep, typename _Period>
	SeqID listenFor(SeqID id, const std::chrono::duration<_Rep, _Period> &period);

	///Listen for changes
	/** Called from separated thread listens for changes. The function returns when
	 * change is detected or timeout ellapsed whichever comes first
	 *
	 * @param id last seen sequence id.  For the first time, use seqID returned by last item received by iterator
	 * created by function scanChanges(). Subsequent calls should use value returned by previous call
	 * @param __atime absolute time of timeout
	 * @return Function returns last sequence ID. This value should be above the first argument if
	 * there is a change. It returns same sequence id, when time ellapsed. It returns 0, when
	 * no waiting is longer possible - because the object is being destroyed. In this case, the
	 * caller must not call any function of this object.
	 *
	 * @note because function cancelListen can be used anytime, the function can return earlier then
	 * specified timeout. In this case returned value will be equal to specified sequenced ID
	 *
	 */
	template<typename _Clock, typename _Duration>
	SeqID listenUntil(SeqID id, const std::chrono::time_point<_Clock, _Duration>& __atime);

	///cancels all pending listens
	void cancelListen();


	SeqID getSeq() const {return incstore.getSeq();}

	struct AggregatorAdapter {
		using IteratorType = Iterator;
		using SourceType = DocStore;

		static DB getDB(const SourceType &src) {return src.getDB();}
		template<typename Fn>
		static auto observe(SourceType &src, Fn &&fn) {
			return src.addObserver([fn = std::forward<Fn>(fn)](Batch &b, const std::string_view &str){
				return fn(b, std::initializer_list<std::string_view>({str}));
			});
		}
		static void stopObserving(SourceType &src, std::size_t h) {
			src.removeObserver(h);
		}
		static json::Value getKey(IteratorType &iter) {
			return json::Value(iter.id());
		}

		static IteratorType find(const SourceType &src, const json::Value &key) {
			return src.range(key.getString(), key.getString(), true);
		}
		static IteratorType prefix(const SourceType &src, const json::Value &key) {
			return src.prefix(key.getString());
		}
		static IteratorType range(const SourceType &src, const json::Value &fromKey, const json::Value &toKey, bool include_upper_bound ) {
			return src.range(fromKey.getString(), toKey.getString(), include_upper_bound);
		}

	};

private :
	static std::size_t testObserverFn(bool ret);
public:
	template<typename Fn>
	auto addObserver(Fn &&fn) {
		return observable.addObserver(std::forward<Fn>(fn));
	}
	void removeObserver(std::size_t h) {
		return observable.removeObserver(h);
	}

protected:
	IncrementalStore incstore;
	unsigned int revHist;
	std::string wrbuff;
	Observable<Batch &, const std::string_view &> observable;


};

template<typename _Rep, typename _Period>
inline SeqID DocStore::listenFor(SeqID id,const std::chrono::duration<_Rep, _Period> &period) {
	return incstore.listenFor(id, period);
}

template<typename _Clock, typename _Duration>
inline SeqID DocStore::listenUntil(SeqID id,const std::chrono::time_point<_Clock, _Duration> &__atime) {
	return incstore.listenUntil(id, __atime);
}



} /* namespace docdb */

#endif /* SRC_DOCDBLIB_DOC_STORE_H_ */
;

