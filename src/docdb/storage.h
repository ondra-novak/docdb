#pragma once
#ifndef SRC_DOCDB_STORAGE_H_
#define SRC_DOCDB_STORAGE_H_

#include "exceptions.h"
#include "storage_view.h"

namespace docdb {

template<DocumentDef _DocDef = RowDocument>
class Storage: public StorageView<_DocDef> {
public:

    using DocType = typename _DocDef::Type;
    using DocRecord = typename StorageView<_DocDef>::DocRecord;

    Storage(const PDatabase &db, const std::string_view &name)
        :Storage(db,db->open_table(name, Purpose::storage)) {}

    Storage(const PDatabase &db, KeyspaceID kid)
        :StorageView<_DocDef>(db,kid, Direction::forward, {},false)
        ,_cmt_obs(*this) {
        init_id();
    }
    Storage(const Storage &) = delete;
    Storage &operator=(const Storage &) = delete;


    using Update = IndexUpdate<DocType>;


    ///Notifies about ongoing update transaction
    /**
     * This observer can perform additional writes to the transaction, however
     * the ongoing update is not yet visible in the storage
     */
    using TransactionObserver = std::function<void(Batch &b, const Update &up)>;

    ///Notifies about change storage revision
    /**
     * When this observer is notified, changes to the storage are visible
     *
     * @param rev current storage revision (which is always top_document + 1)
     * @param cur_id document id commited to the DB. Because writting can be done in parallel,
     * it can happen, that commited document is much lower than rev, as there can be pending
     * writes
     * @retval true continue observing
     * @retval false stop observing
      */
    using CommitObserver = std::function<bool(DocID rev, DocID commited_id)>;


    ///Register observer for newly created transactions
    /**
     * This call is intended to be used during initialization of storage and its indexers.
     * It is not protected by any lock, so if you need to add new observer, you need
     * to stop inserting new records.
     *
     * Currently there is no function to remove transaction observer.
     *
     * @param obs observer
     *
     * If you need to receive notification about update in the storage, use
     * register_commit_observer()
     */
    void register_transaction_observer(TransactionObserver obs) {
        _transaction_observers.push_back(std::move(obs));
    }

    ///Registers observer which is notified right after the transaction is writen
    /**
     * @param obs observer. The observer returns true to contine observing or false to
     * stop observing
     *
     * @note When the observer is notified,the referenced document is visible.
     *
     * @note Any commit observer can drastically decrease troughput, as the calling the
     * observers is serialized
     *
     * @note Currently there is no function to unregister observer. However the observer
     * can remove self by returning false.
     *
     * @note When batch of documents is written, all commit observers are notified after
     * the batch is complete.
     *
     */
    void register_commit_observer(CommitObserver obs) {
        _cmt_obs.reg_observer(std::move(obs));
    }


    ///Put document to the storage
    /**
     * @param doc document to store
     * @param id_of_updated_document identifier of document which is being replaced. This
     * document will be considered as deleted (the new document is considered as update).
     * If this argument is zero, the document is brang new document.
     * @return id of stored document
     */
    DocID put(const DocType &doc, DocID id_of_updated_document = 0) {
        auto b = this->_db->begin_batch();
        DocID id = write(b, &doc, id_of_updated_document);
        b.commit();
        _cmt_obs.notify_doc(id);
        return id;
    }
    ///Put document to the storage - batch update
    /**
     * @param b reference to batch
     * @param doc document to store
     * @param id_of_updated_document identifier of document which is being replaced. This
     * document will be considered as deleted (the new document is considered as update).
     * If this argument is zero, the document is brang new document.
     * @return id of stored document
     *
     * @note you need manually commit the batch through Database::commit_batch()
     */
    DocID put(Batch &b, const DocType &doc, DocID id_of_updated_document = 0) {
        DocID id = write(b, &doc, id_of_updated_document);
        _cmt_obs.reg_doc(b, id);
        return id;
    }

    ///Erase the document
    /**
     * Erases a document. Note that erase of the document is operation, which creates
     * new empty document which refers to document being erased. This document is not
     * send to the indexer.
     *
     * To erase document physically, you need to call purge()
     *
     * @param del_id document to erase
     * @return revision of the deleted document
     *
     * @note It is possible to define deleted document in a different form. This
     * is handled by DocumentDef. To erase such document by this way, you need just
     * call a put() function with document in form, which is considered as deleted
     * document
     */
    DocID erase(DocID del_id) {
        auto b = this->_db->begin_batch();
        DocID id = write(b, nullptr, del_id);
        b.commit();
        _cmt_obs.notify_doc(id);
        return id;
    }

    ///Erase the document - batch update
    /**
     * Erases a document. Note that erase of the document is operation, which creates
     * new empty document which refers to document being erased. This document is not
     * send to the indexer.
     *
     * To erase document physically, you need to call purge()
     *
     * @param b batch
     * @param del_id document to erase
     * @return revision of the deleted document
     *
     * @note It is possible to define deleted document in a different form. This
     * is handled by DocumentDef. To erase such document by this way, you need just
     * call a put() function with document in form, which is considered as deleted
     * document
     */
    DocID erase(Batch &b, DocID del_id) {
        DocID id = write(b, nullptr, del_id);
        _cmt_obs.reg_doc(b, id);
        return id;
    }

    ///Purges single document
    /**
     * Purged document is psychycaly deleted from the storage. It is also removed
     * from indexes, however commit observers are not called.
     *
     * Purpose of purge command is to remove sensitive informations from the storage. You can
     * also use purge() to remove conflicting document
     *
     * @param document to purge
     * @retval true success
     * @retval false document was not found
     */
    bool purge(DocID del_id) {
        auto b = this->_db->begin_batch();
        RawKey kk(this->_kid, del_id);
        auto v = this->_db->get(kk);
        if (v.has_value()) {
            auto [old_doc_id, bin] = Row::extract<DocID, Blob>(*v);
            auto beg = bin.begin();
            auto end = bin.end();
            if (beg != end) {
                auto old_doc = _DocDef::from_binary(beg, end);
                if (!document_is_deleted<_DocDef>(old_doc)) {
                    notify_observers(b, Update {nullptr,&old_doc,del_id,del_id,old_doc_id});
                }
            }
            b.Delete(kk);
            b.commit();
            return true;
        }
        return false;

    }

    ///Import document which has been exported by function export_documents
    /**
     * This feature is intended to support backup and restore and also incremental
     * backup and restore. You can export subset of documents and store them elsewhere.
     * If you need to restore documents, just load documents through the ExportedDocument
     * structure and import them using this function. Index functions are called as well
     *
     * @param b
     * @param docrow
     */
    void import_document(Batch &b, const ExportedDocument &docrow) {
        auto cur = _next_id.load(std::memory_order_relaxed);
        auto next = docrow.id+1;
        while (cur < next && !_next_id.compare_exchange_weak(cur, next, std::memory_order_relaxed));
        DocRecord d = DocRecordDef<_DocDef>::from_binary(docrow.data.data(), docrow.data.data()+docrow.data.size());
        const DocType *doc = d.has_value?&d.document:nullptr;
        write(docrow.id, b, doc, d.previous_id);
    }

    ///Replay all documents to a observer
      void rescan_for(const TransactionObserver &observer, DocID start_doc = 0) {
          auto b = this->_db->begin_batch();
          for (const auto &vdoc: this->select_from(start_doc, Direction::forward)) {
              b.reset();
              const DocType *doc =  vdoc.deleted?nullptr:&vdoc.document;
              if constexpr(std::is_void_v<decltype(update_for(observer, b, vdoc.id, doc, vdoc.previous_id))>) {
                  update_for(observer, b, vdoc.id, doc, vdoc.previous_id, true);
              } else {
                  bool rep = update_for(observer, b, vdoc.id, doc, vdoc.previous_id);
                  if (!rep) break;
              }
              b.commit();
          }

      }

    DocID get_rev() const {
        return _next_id.load(std::memory_order_relaxed);
    }

    ///Remove old revisions and deleted documents
    /**
     * @param count of updates to keep in history. Default value 0 removes
     * any historical document. Set 1 to keep one historical document,
     * 2 to two, etc.
     *
     * @param deleted set true to remove all deleted documents including history
     * Documents must be deleted by function erase().
     *
     * No indexes are affected, no notification is generated
     *
     * @note the function never removes the very recent document
     */
    void compact(std::size_t n=0, bool deleted = false) {
        auto b = this->_db->begin_batch();
        std::unordered_map<DocID, std::size_t> refs;
        RecordsetBase rs(this->_db->make_iterator({},true), {
                RawKey(this->_kid+1),
                RawKey(this->_kid),
                FirstRecord::excluded,
                LastRecord::excluded
        });
        bool first_processed = false;
        bool changes = false;
        if (!rs.empty()) {
            do {
                auto [cur_doc] = Key::extract<DocID>(rs.raw_key());
                auto [prev_doc, doc] = Row::extract<DocID, Blob>(rs.raw_value());

                auto iter = refs.find(cur_doc);
                std::size_t level = -1;
                if (iter != refs.end()) {
                    level = iter->second;
                    if (level >= n) {
                        b.Delete(to_slice(rs.raw_key()));
                        changes = true;
                    }
                    refs.erase(iter);
                } else if (deleted && doc.empty() && first_processed) {
                        b.Delete(to_slice(rs.raw_key()));
                        level = n;
                        changes = true;

                }
                if (prev_doc) {
                    refs.emplace(prev_doc, level+1);
                }
            } while (rs.next());
        }
        if (changes) {
            b.commit();
            this->_db->compact_range(RawKey(this->_kid), RawKey(this->_kid+1));
        }
    }


    ///Restore from backup
    /**
     * @param backup_storage rvalue reference to Storage object which contains backup
     * documents. This storage can be filled by external tool during restoration process,
     * It is also possible to use manage_db to fill this storage.
     *
     * @note function is not MT safe. Do not put any document until the restore is complete
     *
     * @note function also erases content of backup collection
     */
    void restore_backup(Storage<_DocDef> &&backup_storage) {
        Batch b;
        for (const auto &row: backup_storage.select_all()) {
            auto tdoc = this->find(row.id);
            if (tdoc) continue;
            RawKey key(this->_kid, row.id);
            const DocType *doc = row.has_value?&row.document:nullptr;
            write(row.id, b, doc, row.previous_id);
            b.Delete(RawKey(backup_storage.get_kid(), row.id));
            this->_db->commit_batch(b);
        }
    }

protected:


    class CommitObservers: public AbstractBatchNotificationListener {
    public:

        CommitObservers(Storage &owner):_owner(owner) {}
        virtual void after_commit(std::size_t rev) noexcept override {
            std::lock_guard lk(_mx);
            DocID db_rev = _owner.get_rev();
            for (const auto &i: _pending_writes) {
                if (i.first == rev) {
                    notify_observers_lk(i.second, db_rev);
                }
            }
        }
        CommitObservers(const CommitObservers &) = delete;
        CommitObservers &operator=(const CommitObservers &) = delete;

        virtual void before_commit(docdb::Batch &) override {}
        virtual void on_rollback(std::size_t ) noexcept override {}

        void reg_observer(CommitObserver &&obs) {
            std::lock_guard lk(_mx);
            _observers.emplace_back(std::move(obs));
            _any_observers = true;
        }

        void reg_doc(Batch &b, DocID id) {
            if (_any_observers) {
                std::lock_guard lk(_mx);
                b.add_listener(this);
                _pending_writes.emplace_back(b.get_revision(), id);
            }
        }
        void notify_doc(DocID id) {
            if (_any_observers) {
                std::lock_guard lk(_mx);
                notify_observers_lk(id, _owner.get_rev());
            }
        }

        void notify_observers_lk(DocID id, std::size_t db_rev) {
            auto new_end = std::remove_if(_observers.begin(), _observers.end(), [&](const auto &c){
                  return !c(db_rev, id);
            });
            _observers.erase(new_end, _observers.end());
            _any_observers = !_observers.empty();
        }

        Storage &_owner;
        std::vector<CommitObserver> _observers;
        std::vector<std::pair<std::size_t, DocID> > _pending_writes;
        std::mutex _mx;
        bool _any_observers = false;
    };

    std::vector<TransactionObserver> _transaction_observers;
    std::atomic<DocID> _next_id = 1;
    CommitObservers _cmt_obs;

    void notify_observers(Batch &b, const Update &up) {
        for (const auto &x: _transaction_observers) {
            x(b, up);
        }
    }

    void init_id() {
        _next_id = this->get_last_document_id()+1;
    }

    DocID write(Batch &b, const DocType *doc, DocID update_id) {
        DocID id = _next_id.fetch_add(1, std::memory_order_relaxed);
        write(id, b, doc, update_id);
        return id;
    }

    void write(DocID id, Batch &b, const DocType *doc, DocID prev_id) {
        auto buff = b.get_buffer();
        auto iter = std::back_inserter(buff);
        Row::serialize_items(iter, prev_id);
        if (doc) {
            _DocDef::to_binary(*doc, iter);
            if (document_is_deleted<_DocDef>(*doc)) {
                doc = nullptr;
            }
        }
        b.Put(RawKey(this->_kid, id), buff);
        update_for([&](auto &b, const auto &update){
            notify_observers(b, update);
        }, b, id, doc, prev_id);
    }

    template<typename Fn>
    auto update_for(Fn &&fn, Batch &b, DocID id, const DocType *doc, DocID prev_id, bool missing_previous_is_ok = false) {
        if (prev_id) {
            auto val = this->_db->get(RawKey(this->_kid, prev_id));
            if (val.has_value()) {
                auto [old_old_doc_id, bin] = Row::extract<DocID, Blob>(*val);
                auto beg = bin.begin();
                auto end = bin.end();
                if (beg != end) {
                    auto old_doc = _DocDef::from_binary(beg, end);
                    if (!document_is_deleted<_DocDef>(old_doc)) {
                        fn(b, Update {doc,&old_doc,id,prev_id,old_old_doc_id});
                        return;
                    }
                }
                fn(b, Update{doc,nullptr,id,prev_id,old_old_doc_id});
                return;
            } else {
                if (missing_previous_is_ok) {
                    fn(b, Update{doc,nullptr,id,prev_id,0});
                    return;
                }
                throw ReferencedDocumentNotFoundException(prev_id);
            }
        }
        fn( b, Update{doc,nullptr,id,prev_id,0});
    }
};

}


#endif /* SRC_DOCDB_STORAGE_H_ */
