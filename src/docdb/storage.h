#pragma once
#ifndef SRC_DOCDB_STORAGE_H_
#define SRC_DOCDB_STORAGE_H_

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
        :StorageView<_DocDef>(db,kid, Direction::forward, {}) {
        init_id();
    }



    ///Definition of an update
    struct Update {
        ///new document
        /** This pointer can be nullptr, when the update just deleted the document */
        const DocType *new_doc;
        ///old document
        /** This pointer can be nullptr, when there is no old document */
        const DocType *old_doc;
        ///id of new document
        DocID new_doc_id;
        ///id of old document (or zero)
        DocID old_doc_id;
        ///old id of old document (or zero)
        DocID old_old_doc_id;
    };

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
        std::lock_guard _(_commit_observers_mx);
        _commit_observers.push_back(std::move(obs));
        _any_commit_observers = true;
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
        Batch b;
        DocID id = write(b, &doc, id_of_updated_document);
        this->_db->commit_batch(b);
        notify_commit_observers(id);
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
        attach_notify_commit_to_batch(b, id);
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
        Batch b;
        DocID id = write(b, nullptr, del_id);
        this->_db->commit_batch(b);
        notify_commit_observers(id);
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
        attach_notify_commit_to_batch(b, id);
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
        Batch b;
        std::string tmp;
        RawKey kk(this->_kid, del_id);
        if (this->_db->get(kk, tmp)) {
            Row rw((RowView(tmp)));
            auto [old_doc_id, bin] = rw.get<DocID, Blob>();
            auto beg = bin.begin();
            auto end = bin.end();
            if (!StorageView<_DocDef>::is_deleted(beg, end)) {
                auto old_doc = _DocDef::from_binary(beg, end);
                notify_observers(b, Update {nullptr,&old_doc,del_id,del_id,old_doc_id});
            }
            b.Delete(kk);
            this->_db->commit_batch(b);
            return true;
        }
        return false;

    }

    ///Replay all documents to a observer
      void rescan_for(const TransactionObserver &observer) {
          Batch b;
          auto iter = this->scan();
          bool rep = true;
          while (rep && iter.next()) {
              auto vdoc = iter.value();
              DocType *doc =  vdoc.document.has_value()?&(*vdoc.document):nullptr;
              if constexpr(std::is_void_v<decltype(update_for(observer, b, iter.id(), doc, vdoc.previous_id))>) {
                  update_for(observer, b, iter.id(), doc, vdoc.previous_id);
              } else {
                  rep = update_for(observer, b, iter.id(), doc, vdoc.previous_id);
              }
              this->_db->commit_batch(b);
          }

      }

    DocID get_rev() const {
        return _next_id.load(std::memory_order_relaxed);
    }



protected:

    std::vector<TransactionObserver> _transaction_observers;
    std::vector<CommitObserver> _commit_observers;
    std::mutex _commit_observers_mx;
    std::atomic<DocID> _next_id = 1;
    bool _any_commit_observers = false;

    void notify_observers(Batch &b, const Update &up) {
        for (const auto &x: _transaction_observers) {
            x(b, up);
        }
    }
    void notify_commit_observers(DocID docId) {
        if (_any_commit_observers) {
            std::lock_guard _(_commit_observers_mx);
            DocID rev = _next_id.load(std::memory_order_relaxed);
            auto new_end = std::remove_if(_commit_observers.begin(), _commit_observers.end(), [&](const auto &c){
               return !c(rev, docId);
            });
            _commit_observers.erase(new_end, _commit_observers.end());
            _any_commit_observers = !_commit_observers.empty();
        }
    }

    class BatchCommitInfo {
    public:
        void run(bool commit) {
            if (commit) _owner.notify_commit_observers(_id);
            delete this;
        }
        BatchCommitInfo(Storage &owner, DocID id):_owner(owner),_id(id) {}

        static Batch::Hook createHook(Storage &owner, DocID id)  {
            auto p = new BatchCommitInfo(owner, id);
            return Batch::Hook::member_fn<&BatchCommitInfo::run>(p);
        }

    protected:
        Storage &_owner;
        DocID _id;
    };

    void attach_notify_commit_to_batch(Batch &b, DocID id) {
        if (_any_commit_observers) {
            b.add_hook(BatchCommitInfo::createHook(*this, id));
        }
    }

    void init_id() {
        _next_id = this->get_last_document_id()+1;
    }

    DocID write(Batch &b, const DocType *doc, DocID update_id) {
        DocID id = _next_id.fetch_add(1, std::memory_order_relaxed);
        auto buff = b.get_buffer();
        auto iter = std::back_inserter(buff);
        Row::serialize_items(iter, update_id);
        if (doc) {
            _DocDef::to_binary(*doc, iter);
        }
        b.Put(RawKey(this->_kid, id), buff);
        if (update_id) {
            std::string tmp;
            if (this->_db->get(RawKey(this->_kid, update_id), tmp)) {
                Row rw((RowView(tmp)));
                auto [old_old_doc_id, bin] = rw.get<DocID, Blob>();
                auto beg = bin.begin();
                auto end = bin.end();
                if (!StorageView<_DocDef>::is_deleted(beg, end)) {
                    auto old_doc = _DocDef::from_binary(beg, end);
                    notify_observers(b, Update {doc,&old_doc,id,update_id,old_old_doc_id});
                    return id;
                }
                notify_observers(b, Update{doc,nullptr,id,update_id,old_old_doc_id});
                return id;
            }
        }
        notify_observers( b, Update{doc,nullptr,id,update_id,0});
        return id;
    }

    template<typename Fn>
    auto update_for(Fn &&fn, Batch &b, DocID id, const DocType *doc, DocID prev_id) {
          if (prev_id) {
              std::string tmp;
              if (this->_db->get(RawKey(this->_kid, prev_id), tmp)) {
                  Row rw((RowView(tmp)));
                  auto [old_old_doc_id, bin] = rw.get<DocID, Blob>();
                  auto beg = bin.begin();
                  auto end = bin.end();
                  if (!StorageView<_DocDef>::is_deleted(beg, end)) {
                      auto old_doc = _DocDef::from_binary(beg, end);
                      return fn(b, Update {doc,&old_doc,id,prev_id,old_old_doc_id});
                  }
                  return fn(b, Update{doc,nullptr,id,prev_id,old_old_doc_id});
              }
          }
          return fn(b, Update{nullptr, doc, 0, prev_id, id});
    }
};

}




#endif /* SRC_DOCDB_STORAGE_H_ */
