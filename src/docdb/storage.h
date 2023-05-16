/*
 * storage.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_STORAGE_H_
#define SRC_DOCDB_STORAGE_H_
#include "view.h"

#include <leveldb/write_batch.h>
#include <functional>
#include <vector>
#include <mutex>
namespace docdb {


class StorageView {
public:

    StorageView(PDatabase db, std::string_view name, PSnapshot snap = {})
        :_db(std::move(db)),_snap(std::move(snap)),_kid(_db->open_table(name)) {}

    ///Construct the view
    /**
     * @param db reference to database
     * @param kid keyspace id
     * @param dir default direction to iterate this view (default is forward)
     * @param snap reference to snapshot (optional)
     */
    StorageView(PDatabase db, KeyspaceID kid, PSnapshot snap = {})
        :_db(std::move(db)), _snap(std::move(snap)),_kid(kid) {}



    StorageView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return StorageView(_db, _kid, _db->make_snapshot());
    }

    StorageView open_snapshot(const PSnapshot &snap) const {
        return StorageView(_db, _kid, snap);
    }


    using DocID = std::uint64_t;

    struct DocData {
        DocID old_rev;
        std::string_view doc_data;
    };

    using RawIterator = Iterator;
    class Iterator: public RawIterator {
    public:
        using RawIterator::RawIterator;

        ///Retrieve document id
        DocID id() const {
            KeyspaceID kid;
            DocID id;
            Value::parse(key(), kid, id);
            return id;
        }

        ///Retrieve document and previous id
        DocData doc() const {
            std::string_view s = value();
            DocID prevId;
            RemainingData remain;
            Value::parse(s, prevId, remain);
            return {prevId, remain};
        }

    };


    Iterator scan(Direction dir = Direction::forward) {
        Key from(_kid);
        Key to(_kid+1);
        if (isForward(dir))  {
            return scan_internal(from, to, LastRecord::excluded);
        } else {
            auto iter = scan_internal(to, from, LastRecord::included);
            iter.next();
            return iter;
        }
    }

    Iterator scan_from(DocID docId, Direction dir = Direction::forward) {
        return scan_internal(Key(_kid,docId),
                             isForward(dir)?Key(_kid+1):Key(_kid),
                             isForward(dir)?LastRecord::excluded:LastRecord::included);
    }

    ///Get document data for given document id
    /**
     * @param docId document id to retrieve
     * @param buffer temporary buffer to store the data.
     * @return DocData, if found, otherwise no value
     */
    std::optional<DocData> get(DocID docId, std::string &buffer) {
        if (!_db->get(Key(_kid,docId), buffer)) return {};
        DocID id;
        RemainingData docdata;
        Value::parse(buffer, id, docdata);
        return DocData{id, docdata};
    }

    Iterator scan(DocID from, DocID to, LastRecord last_record = LastRecord::excluded) {
        return scan_internal(Key(_kid,from), Key(_kid,to) , last_record);
    }


    DocID find_revision_id() {
        Iterator iter = scan(Direction::backward);
        if (iter.next()) {
            return iter.id()+1;
        } else {
            return 1;
        }
    }

    PDatabase get_db() const {return _db;}
    PSnapshot get_snapshot() const {return _snap;}
    KeyspaceID get_kid() const {return _kid;}


protected:
    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;


    Iterator scan_internal(std::string_view from, std::string_view to, LastRecord last_record) {
        auto iter = _db->make_iterator(false, _snap);
        iter->Seek(to_slice(from));
        if (from <= to) {
            return Iterator(std::move(iter), to, Direction::forward, last_record);
        } else {
            return Iterator(std::move(iter), to, Direction::backward, last_record);
        }
    }

};

///Unordered storage for documents
/**
 * The document is an arbitrary string. This object can store any count of documents,
 * where each document obtains automatic number starting from 1 (to reserve value 0
 * as no-value);
 *
 * Documents are stored in following form:
 * <previous_id><document>, use extract_document() to retrieve these data
 *
 *
 */
class Storage: public StorageView {
public:

    Storage(PDatabase db, std::string_view name):StorageView(db, name)
        ,_next_id(StorageView::find_revision_id()) {}
    Storage(PDatabase db, KeyspaceID kid):StorageView(db, kid)
        ,_next_id(StorageView::find_revision_id()) {}


    using UpdateObserver = std::function<bool(const PSnapshot &)>;


    ///This object allows to write multiple documents
    class WriteBatch {
    public:
        WriteBatch(Storage &storage)
            :_storage(&storage) {
            _storage->_mx.lock();
            _alloc_ids = _storage->_next_id;
        }

        WriteBatch() = default;

        WriteBatch(const WriteBatch &) = delete;
        WriteBatch &operator=(const WriteBatch &) = delete;

        WriteBatch(WriteBatch &&x)
            :_batch(std::move(x._batch))
            ,_storage(x._storage)
            ,_alloc_ids(x._alloc_ids){
                x._storage = nullptr;
        }
        WriteBatch &operator=(WriteBatch &&x) {
            if (this != &x) {
                if (_storage) _storage->_mx.unlock();
                _batch = (std::move(x._batch));
                _storage = x._storage;
                x._storage = nullptr;
                _alloc_ids = x._alloc_ids;
            }
            return *this;
        }
        auto size() {
            return _batch.ApproximateSize();
        }

        ///Put document to the storage
        /**
         * @param doc document to write
         * @param replace_id document id which is being replaced
         * @return document id
         *
         * @note Document id can't be used until the batch is commited. If
         * the batch is rollbacked or left, the returned document id
         * is invalid and released, and can be assigned to another document
         */
        DocID put_doc(const std::string_view &doc, DocID replace_id = 0);

        ///Commit the batch to the database
        void commit() {
            _storage->_db->commit_batch(*this);
            _storage->_next_id = _alloc_ids;
            _storage->notify();
        }

        ///rollback the batch
        void rollback() {
            _batch.Clear();
            _alloc_ids = _storage->_next_id;
        }


        ~WriteBatch() {
            if (_storage) _storage->_mx.unlock();
        }

        operator Batch &() {return _batch;}
    protected:
        Batch _batch;
        Storage *_storage = nullptr;
        DocID _alloc_ids = 0;
        Value _buffer;


    };


    ///Write multiple documents (including documents to other tables)
    /**
     * @return object used to write multiple documents
     *
     * @note return value is based on Batch, so you can use it to
     * write any unrelated data to the database. While this
     * object is held, the table is locked.
     */
    WriteBatch bulk_put() {
        return WriteBatch(*this);
    }

    DocID put(const std::string_view &doc, DocID replaced_id = 0);

    ///Erase document
    /**
     * @param b write batch
     * @param id document id
     *
     * @note Document erasion is not notified. You can erase updated documents,
     * when the update is implemented as inserting a new revision of the
     * document and removing old revision.b
     *
     */
    void erase(Batch &b, DocID id);

    ///Erase document
    /**
     * @param id document id
     *
     * @note Document erasion is not notified.
     */
    void erase(DocID id);


    ///Retrieve revision id (which is recent document ID + 1)
    DocID get_rev() const {
        std::lock_guard _(_mx);
        return _next_id;
    }

    void register_observer(UpdateObserver &&cb);

    void register_observer(UpdateObserver &&cb, DocID id);

    ///Compact the storage removing old revisions
    /**
     * When document is replaced, it is always stored as new record. Old version
     * (old revision) of the document is still stored and can be accessed under
     * its original ID. New revision receives new ID but it also tracks ID of
     * document it replaces.
     *
     * This function removes all documents referenced as replaced, so documents
     * referenced as old revision will no longer available
     */
    void compact();

protected:



    DocID _next_id = 0;
    mutable std::mutex _mx;
    std::vector<UpdateObserver> _cblist;

    void notify() noexcept;


};



}



#endif /* SRC_DOCDB_STORAGE_H_ */
