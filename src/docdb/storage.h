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
class Storage: public View {
public:

    using View::View;

    using DocID = std::uint64_t;

    ///Calculate key
    /**
     * @param id document id
     * @return a key which can be used to lookup and search
     */
    Key key(DocID id) const {
        return View::key(id);
    }

    using _Batch = Batch;
    using NotifyCallback = std::function<void(DocID)>;
    using NotifyVector = std::vector<NotifyCallback>;


    ///This object allows to write multiple documents
    class WriteBatch: public Batch {
    public:
        WriteBatch(Storage &storage)
            :_storage(&storage) {
            _storage->_mx.lock();
            if (!_storage->_next_id) {
                _storage->find_revision_id();
            }
            _alloc_ids = _storage->_next_id;
        }

        WriteBatch() = default;

        WriteBatch(const WriteBatch &) = delete;
        WriteBatch &operator=(const WriteBatch &) = delete;

        WriteBatch(WriteBatch &&x)
            : Batch(std::move(x))
            ,_storage(x._storage)
            ,_alloc_ids(x._alloc_ids){
                x._storage = nullptr;
        }
        WriteBatch &operator=(WriteBatch &&x) {
            if (this != &x) {
                if (_storage) _storage->_mx.unlock();
                Batch::operator =(std::move(x));
                _storage = x._storage;
                x._storage = nullptr;
                _alloc_ids = x._alloc_ids;
            }
            return *this;
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

        void commit() {
            _storage->_db->commit_batch(*this);
            _storage->_next_id = _alloc_ids;
            if (!_storage->_notify.empty()) {
                _storage->notify();
            }
        }

        void rollback() {
            Clear();
            _alloc_ids = _storage->_next_id;
        }


        ~WriteBatch() {
            if (_storage) _storage->_mx.unlock();
        }
    protected:
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

    DocID put(const std::string_view &doc);

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
        return _next_id-1;
    }

    ///Register callback function which is called when new data are inserted
    /**
     * @param id of next document (recent document ID +1)
     * @param fn function which is called when new document(s) are written
     * @return function returns value > id when documents are available. In this
     * case, the function is not registered and called. If the returned value is equal
     * or less than id, the function was registered and will be called
     */
    template<typename Fn>
    DocID register_callback(DocID id, Fn &&fn) {
        std::lock_guard _(_mx);
        if (id >= _next_id) {
            _notify.push_back(std::forward<Fn>(fn));
        }
        return _next_id;
    }


    ///converts key retrieved by iterator to document ID
    static DocID key2docid(std::string_view key) {
        KeyspaceID kid;
        DocID docId = 0;
        Value::deserialize(key, kid, docId);
        return docId;
    }

    struct DocData {
        DocID old_rev;
        std::string_view doc;
    };

    DocData extract_document(const std::string_view &value) {
        RemainingData dd;
        DocID id;
        Value::deserialize(value, id, dd);
        return {id, dd};
    }


protected:

    DocID _next_id = 0;
    mutable std::mutex _mx;
    NotifyVector _notify;
    NotifyVector _reserved_notify;

    void notify() noexcept {
        auto id = _next_id-1;
        auto ntf = std::move(_reserved_notify);
        std::swap(ntf, _notify);
        _mx.unlock();
        for (auto &f: ntf) {
            f(id);
        }
        ntf.clear();
        _mx.lock();
        std::swap(ntf, _reserved_notify);
    }

    DocID find_revision_id();

};



}



#endif /* SRC_DOCDB_STORAGE_H_ */
