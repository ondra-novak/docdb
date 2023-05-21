#pragma once
#ifndef SRC_DOCDB_DOC_STORAGE_H_
#define SRC_DOCDB_DOC_STORAGE_H_
#include "database.h"
#include "observer.h"

#include <atomic>
#include <memory>
#include <queue>
namespace docdb {



///Type of document ID
using DocID = std::uint64_t;
///Document storage - Readonly view
/**
 * @tparam _DocDef Document type definition (and traits)
 */
template<DocumentDef _DocDef>
class DocumentStorageView: public ViewBase {
public:

    using ViewBase::ViewBase;

    DocumentStorageView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return DocumentStorageView(_db, _kid, _dir, _db->make_snapshot());
    }

    ///Get database
    const PDatabase& get_db() const {return _db;}
    ///Get Keyspace id
    KeyspaceID get_kid() const {return _kid;}
    ///Get current snapshot
    const PSnapshot& get_snapshot() const {return _snap;}

    ///Type of stored document
    using DocType = typename _DocDef::Type;
    using DocID = ::docdb::DocID;;

    ///Information retrieved from database
    class DocInfo {
    public:
        ///document id
        DocID id;
        ///id of previous document, which was replaced by this document (or zero)
        DocID prev_id;
        ///contains true, if document exists, or false if not found (bin_data are empty)
        bool exists;
        ///document has been deleted (previous document contans latests version)
        bool deleted;
        ///document is available and can be parsed (!exists && !deleted)
        bool available;
        ///Parse binary data and returns parsed document
        DocType doc() const {
            std::string_view part(bin_data);
            part = part.substr(sizeof(DocID));
            return _DocDef::from_binary(part.begin(),part.end());
        }

        ///Converts to document
        DocType operator *() const {return doc();}

        ///Returns true if document is available
        operator bool() const {return available;}

        ///Returns true if document is available
        bool operator==(std::nullptr_t) const {return available;}

        ///Returns true if document exists (but can be deleted)
        bool has_value() const {return exists;}

    protected:
        ///document's binary data
        std::string bin_data;

        DocInfo(const PDatabase &db, const PSnapshot &snap, const std::string_view &key, DocID id)
            :id(id),prev_id(0),deleted(false) {
            exists = db->get(key, bin_data, snap);
            auto [docid, remain] = BasicRow::extract<DocID, Blob>(bin_data);
            prev_id = docid;
            deleted = remain.empty();
            available = !exists && !deleted;
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

    struct _IterHelper {
        using Type = std::optional<DocType>;
        template<typename Iter>
        static std::optional<DocType> from_binary(Iter beg, Iter end) {
            std::advance(beg, sizeof(DocID));
            if (beg == end) return {};
            return _DocDef::from_binary(beg, end);
        }
        template<typename Iter>
        static void to_binary(const Type &, Iter );
    };

    ///Iterator
    class Iterator: public GenIterator<_IterHelper>{
    public:
        using GenIterator<_IterHelper>::GenIterator;

        ///Retrieve document id
        DocID id() const {
            auto [id] = this->key().template get<DocID>();
            return id;
        }

        ///retrieve id of replaced document
        DocID prev_id() const {
            auto [id] = BasicRow::extract<DocID>(this->raw_value());
            return id;
        }

        auto doc() const {return this->value();}

    };

    ///Scan whole storage
    Iterator scan(Direction dir=Direction::normal) const {
        if (isForward(changeDirection(_dir, dir))) {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }

    ///Scan from given document for given direction
    Iterator scan_from(DocID start_pt, Direction dir = Direction::normal) const {
        if (isForward(changeDirection(_dir, dir))) {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid, start_pt),RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid, start_pt),RawKey(_kid),
                    FirstRecord::included, LastRecord::excluded
            });
        }
    }

    ///Scan for range
    Iterator scan_range(DocID start_id, DocID end_id, LastRecord last_record = LastRecord::excluded) const {
        return Iterator(_db->make_iterator(false,_snap),{
                RawKey(_kid, start_id),RawKey(_kid, end_id),
                FirstRecord::included, LastRecord::excluded
        });
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
    using Iterator = typename DocumentStorageView<_DocDef>::Iterator;

    DocumentStorage(const PDatabase &db, std::string_view name)
        :DocumentStorageView<_DocDef>(db, name)
        ,_next_id(DocumentStorageView<_DocDef>::get_last_document_id()+1)
         {}
    DocumentStorage(const PDatabase &db, KeyspaceID kid)
        :DocumentStorageView<_DocDef>(db, kid)
         ,_next_id(DocumentStorageView<_DocDef>::get_last_document_id()+1)
         {}


    DocID put(const DocType &doc, DocID prev_id = 0) {
        PendingBatch *batch = new_batch();
        DocID id = batch->_id;
        try {
            auto &buffer = batch->_b.get_buffer();
            auto iter = std::back_inserter(buffer);
            BasicRow::serialize_items(iter,prev_id);
            _DocDef::to_binary(doc, iter);
            batch->_b.Put(RawKey(this->_kid, id), buffer);
            update_observers(batch->_b, id, &doc, prev_id);
            batch->_state.store(BatchState::commit, std::memory_order_release);
            finalize_batch();
        } catch (...) {
            batch->_state.store(BatchState::rollback, std::memory_order_release);
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
            auto &buffer = batch->_b.get_buffer();
            auto iter = std::back_inserter(buffer);
            BasicRow::serialize_items(iter,del_id);
            batch->_b.Put(RawKey(this->_kid,id), {});
            update_observers(batch->_b, id, nullptr, del_id);
            batch->_state.store(BatchState::commit, std::memory_order_release);
            finalize_batch();
        } catch (...) {
            batch->_state.store(BatchState::rollback, std::memory_order_release);
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
        const DocType *new_doc;
        ///old id of old document (or zero)
        DocID old_old_doc_id;
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
    using UpdateObserver = std::function<bool(Batch &, const Update &)>;


    ///Register new observer
    /**
     * @param observer new observer to register (index)
     * @return id id of observer
     */
    std::size_t register_observer(UpdateObserver &&observer) {
        return _observers.register_observer(std::move(observer));
    }
    ///Unregister observer (by id)
    void unregister_observer(std::size_t id) {
        _observers.unregister_observer(id);
    }

    void compact() {
        Batch b;
        Iterator iter = this->scan();
        while (iter.next()) {
            DocID p = iter.prev_id();
            if (p) b.Delete(RawKey(this->_kid,p));
        }
        this->_db->commit_batch(b);
    }

    auto get_rev() const {
        std::lock_guard _(_mx);
        return _next_id;
    }

    ///Replay all documents to a observer
    void replay_for(const UpdateObserver &observer) {
        Batch b;
        Iterator iter = this->scan();
        bool rep = true;
        while (rep && iter.next()) {
            auto vdoc = iter.value();
            auto doc = *vdoc;
            rep = update_for(observer, b, iter.id(), &doc, iter.prev_id());
            this->_db->commit_batch(b);
        }

    }

protected:



    DocID _next_id;
    mutable std::mutex _mx;

    enum class BatchState {
            pending = 0,
            commit,
            rollback
    };

    struct PendingBatch {
        Batch _b;
        DocID _id = 0;
        std::atomic<BatchState> _state = BatchState::pending ;
    };


    using PBatch = std::unique_ptr<PendingBatch>;


    std::queue<PBatch> _pending, _dropped;
    ObserverList<UpdateObserver> _observers;

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
        ++_next_id;
        PendingBatch *ret = b.get();
        _pending.push(std::move(b));
        return ret;
    }

    void finalize_batch() {
        std::lock_guard _(_mx);
        bool cont = true;
        while (!_pending.empty() && cont) {
            const PBatch &b = _pending.front();
            BatchState state = b->_state.exchange(BatchState::pending, std::memory_order_acquire);
            switch (state) {
                case BatchState::commit:
                    this->_db->commit_batch(b->_b);
                    _dropped.push(std::move(_pending.front()));
                    _pending.pop();
                    break;
                case BatchState::rollback:
                    b->_b.Clear();
                    _dropped.push(std::move(_pending.front()));
                    _pending.pop();
                    break;
                default:
                    cont = false;
                    break;
            }
        }
    }

    template<typename Fn>
    bool update_for(Fn &&fn, Batch &b, DocID id, const DocType *doc, DocID prev_id) {
        if (prev_id) {
            DocInfo d = this->get(prev_id);
            if (d.exists && !d.deleted) {
                DocType old_doc = d.doc();
                return fn(b, Update{
                    &old_doc,
                    doc,
                    d.prev_id,
                    prev_id,
                    id
                });
            }
        }
        return fn(b, Update{nullptr, doc, 0, prev_id, id});

    }


    void update_observers(Batch &b, DocID id, const DocType *doc, DocID prev_id) {
        update_for([&](Batch &b, const Update &update){
            _observers.call(b, update);
            return true;
        }, b, id, doc, prev_id);
    }




};

}




#endif /* SRC_DOCDB_DOC_STORAGE_H_ */
