#pragma once
#ifndef SRC_DOCDB_DOC_STORAGE_H_
#define SRC_DOCDB_DOC_STORAGE_H_
#include "database.h"

#include <memory>
#include <queue>
namespace docdb {


///Document storage - Readonly view
/**
 * @tparam _DocDef Document type definition (and traits)
 */
template<DocumentDef _DocDef>
class DocumentStorageView {
public:

    DocumentStorageView(const PDatabase &db, std::string_view name, const PSnapshot &snap = {})
        :_db(db)
        ,_snap(snap)
        ,_kid(db->open_table(name))
        {}
    DocumentStorageView(const PDatabase &db, KeyspaceID kid, const PSnapshot &snap = {})
        :_db(db)
        ,_snap(snap)
        ,_kid(kid)
        {}

    DocumentStorageView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return DocumentStorageView(_db, _kid, _db->make_snapshot());
    }

    ///Get database
    const PDatabase& get_db() const {return _db;}
    ///Get Keyspace id
    KeyspaceID get_kid() const {return _kid;}
    ///Get current snapshot
    const PSnapshot& get_snapshot() const {return _snap;}

    ///Type of stored document
    using DocType = typename _DocDef::Type;
    ///Type of document ID
    using DocID = std::uint64_t;

    ///Information retrieved from database
    class DocInfo {
    public:
        ///document id
        DocID id;
        ///id of previous document, which was replaced by this document (or zero)
        DocID prev_id;
        ///document's binary data
        std::string bin_data;
        ///contains true, if document exists, or false if not found (bin_data are empty)
        bool exists;
        ///document has been deleted (previous document contans latests version)
        bool deleted;
        ///Parse binary data and returns parsed document
        DocType doc() const {
            return _DocDef::from_binary(bin_data.data(), bin_data.data()+bin_data.size());
        }
    protected:
        DocInfo(const PDatabase &db, const PSnapshot &snap, const std::string_view &key, DocID id)
            :id(id),prev_id(0),deleted(false) {
            exists = db->get(key, bin_data, snap);
            Value::parse(bin_data, prev_id);
            deleted = bin_data.size() <= sizeof(DocID);
        }

        friend class DocumentStorageView;

    };

    ///Retrieve document under given id
    DocInfo get(DocID id) const {
        return DocInfo(_db,_snap, RawKey(_kid,id), id);
    }

    ///Operator[]
    DocInfo operator[](DocID id) const {
        return get(id);
    }

    ///Iterator
    class Iterator: public GenIterator{
    public:
        using GenIterator::GenIterator;

        ///Retrieve document id
        DocID id() const {
            KeyspaceID kid;
            DocID id;
            Value::parse(key(), kid, id);
            return id;
        }

        ///retrieve binary representation of the document
        std::string_view bin_data() const {
            return value().substr(sizeof(DocID));
        }

        ///retrieve id of replaced document
        DocID prev_id() const {
            DocID id;
            Value::parse(value(),id);
            return id;
        }

        ///retrieve the document itself
        DocType doc() const {
            std::string_view bin = bin_data();
            return _DocDef::from_binary(bin.data(), bin.data()+bin.size());
        }

        ///Document has been deleter
        /**
         * @retval true this revision is about deletion of previous revision
         * @retval false document exists (was not deleted)
         */
        bool deleted() const {
            return bin_data().empty();
        }
    };

    ///Scan whole storage
    Iterator scan(Direction dir=Direction::forward) const {
        if (isForward(dir)) {
            return _db->init_iterator<Iterator>(_snap,
                    RawKey(_kid), FirstRecord::included,
                    RawKey(_kid+1), Direction::forward,
                    LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(_snap,
                    RawKey(_kid+1), FirstRecord::excluded,
                    RawKey(_kid), Direction::backward,
                    LastRecord::included);
        }
    }

    ///Scan from given document for given direction
    Iterator scan_from(DocID start_pt, Direction dir = Direction::forward) const {
        if (isForward(dir)) {
            return _db->init_iterator<Iterator>(_snap,
                    RawKey(_kid, start_pt), FirstRecord::included,
                    RawKey(_kid+1), Direction::forward,
                    LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(_snap,
                    RawKey(_kid, start_pt), FirstRecord::included,
                    RawKey(_kid), Direction::backward,
                    LastRecord::excluded);
        }
    }

    ///Scan for range
    Iterator scan_range(DocID start_id, DocID end_id, LastRecord last_record = LastRecord::excluded) const {
        return _db->init_iterator<Iterator>(_snap, RawKey(_kid, start_id), FirstRecord::included,
                RawKey(_kid, end_id), start_id<=end_id?Direction::forward:Direction::backward, last_record);
    }

    ///Retrieve ID of last document
    /**
     * @return id of last document, or zero if database is empty
     */
    DocID get_last_document_id() const {
        auto iter = scan(Direction::backward);
        if (iter.next()) return iter.id();
        else return 0;
    }

protected:

    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
};


///Document storage (writable)
/**
 * @tparam _DocDef Document type definition (and traits)
 */
template<DocumentDef _DocDef>
class DocumentStorage: public DocumentStorageView<_DocDef> {
public:

    using DocType = typename DocumentStorageView<_DocDef>::DocType;
    using DocID = typename DocumentStorageView<_DocDef>::DocID;
    using DocInfo = typename DocumentStorageView<_DocDef>::DocInfo;

    DocumentStorage(const PDatabase &db, std::string_view name)
        :DocumentStorageView<_DocDef>(db, name) {}
    DocumentStorage(const PDatabase &db, KeyspaceID kid)
        :DocumentStorageView<_DocDef>(db, kid) {}


    DocID put(const DocType &doc, DocID prev_id = 0) {
        PendingBatch *batch = new_batch();
        DocID id = batch->_id;
        try {
            batch->_buffer.add(prev_id);
            _DocDef::to_binary(doc, std::back_inserter(batch->_buffer));
            update_observers(batch->_b, id, &doc, prev_id);
            batch->_commit = true;
            finalize_batch();
        } catch (...) {
            batch->_rollback = true;
            finalize_batch();
            throw;
        }
        return id;
    }

    ///Delete specified ID
    /**
     * Deletion is special update with empty document data
     *
     * @param id id of document to delete
     * @return id of update
     */
    DocID erase(DocID del_id) {
        PendingBatch *batch = new_batch();
        DocID id = batch->_id;
        try {
            batch->_buffer.add(del_id);
            update_observers(batch->_b, id, nullptr, del_id);
            batch->_commit = true;
            finalize_batch();
        } catch (...) {
            batch->_rollback = true;
            finalize_batch();
            throw;
        }
        return id;
    }

    ///Definition of an update
    struct Update {
        ///old document
        /** This pointer can be nullptr, when there is no old document */
        const DocType *old_doc;
        ///new document
        /** This pointer can be nullptr, when the update just deleted the document */
        const DocType *old_new;
        ///id of old document (or zero)
        DocID old_doc_id;
        ///id of new document
        DocID new_doc_id;
    };

    ///The observer receives batch and update structure
    /** The obserer can update anything in the database by writting to the batch
     *
     * @param Batch batch to write a update
     * @param Update update on storage
     *
     * @exception any sobserver can throw an exception, which automatically rollbacks
     * the whole update
     */
    using UpdateObserver = std::function<void(Batch &, const Update &)>;


    ///Register new observer
    /**
     * To achieve maximum performance, this function IS NOT MT SAFE. Additionaly
     * it is also forbidden to write documents and register new observers (as the
     * put, erase and register_observer are NOT MT SAFE) - because modification
     * in observer's container is not protected by a mutex
     *
     * @param observer new observer to register (index)
     */
    void register_observer(UpdateObserver &&observer);

protected:

    DocID _next_id;
    std::mutex _mx;

    struct PendingBatch {
        Batch _b;
        DocID _id = 0;
        Value _buffer;
        bool _commit = false;
        bool _rollback = false;
    };
    using PBatch = std::unique_ptr<PendingBatch>;

    std::queue<PBatch> _pending, _dropped;
    std::vector<UpdateObserver> _observers;

    PendingBatch *new_batch() {
        std::lock_guard _(_mx);
        PBatch b;
        if (!_dropped.empty()) {
            b = std::move(_dropped.front());
            _dropped.pop();
        } else {
            b = std::make_unique<PendingBatch>();
        }
        b->_id = _next_id;
        b->_buffer.clear();
        b->_rollback = false;
        ++_next_id;
        PendingBatch *ret = b.get();
        _pending.push(std::move(b));
        return ret;
    }

    void finalize_batch() {
        std::lock_guard _(_mx);
        while (!_pending.empty()) {
            const PBatch &b = _pending.front();
            if (b->_commit) {
                this->_db->commit_batch(b->_b);
                b->_commit = false;
                _dropped.push(std::move(_pending.front()));
                _pending.pop();
            } else if (b->_rollback){
                b->_b.Clear();
                b->_rollback = false;
                _dropped.push(std::move(_pending.front()));
                _pending.pop();
            } else {
                break;
            }
        }
    }

    void update_observers(Batch &b, const Update &up) {
        for (auto &c: _observers) {
            c(b,up);
        }
    }

    void update_observers(Batch &b, DocID id, const DocType *doc, DocID prev_id) {
        if (prev_id) {
            DocInfo d = this->get(prev_id);
            if (d.exists && !d.deleted) {
                DocType old_doc = d.doc();
                update_observers(b, Update{
                    &old_doc,
                    doc,
                    prev_id,
                    id
                });
                return;
            }
        }
        update_observers(b, Update{nullptr, doc, prev_id, id});
    }




};

}




#endif /* SRC_DOCDB_DOC_STORAGE_H_ */
