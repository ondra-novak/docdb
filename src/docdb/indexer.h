#pragma once
#ifndef SRC_DOCDB_INDEXER_H_
#define SRC_DOCDB_INDEXER_H_

#include "concepts.h"
#include "database.h"
#include "keylock.h"
#include "exceptions.h"

#include "index_view.h"
namespace docdb {

using IndexerRevision = std::size_t;
struct IndexerState  {
        IndexerRevision indexer_revision;
        DocID storage_revision;
};



template<DocumentDef _ValueDef>
struct IndexerEmitTemplate {
    void operator()(Key,typename _ValueDef::Type);
    void operator()(Key);
    static constexpr bool erase = false;
    DocID id() const;
    DocID prev_id() const;
};

template<typename T, typename Storage, typename _ValueDef>
DOCDB_CXX20_CONCEPT(IndexFn, requires{
   std::invocable<T, IndexerEmitTemplate<_ValueDef>, typename Storage::DocType>;
   {T::revision} -> std::convertible_to<IndexerRevision>;
});


#if 0
class DuplicateKeyException: public std::exception {
public:
    DuplicateKeyException(Key key, const PDatabase &db, DocID incoming, DocID stored)
        :_key(key), _message("Duplicate key in an index: ") {
            auto name = db->name_from_id(key.get_kid());
            if (name.has_value()) _message.append(*name);
            else _message.append("Unknown table KID ").append(std::to_string(static_cast<int>(key.get_kid())));
            _message.append(". Conflicting document: ").append(std::to_string(stored));
    }
    const Key &get_key() const {return _key;}
    const char *what() const noexcept override {return _message.c_str();}
protected:
    Key _key;
    std::string _message;

};
#endif

template<DocumentStorageType Storage, typename _IndexFn, IndexType index_type = IndexType::multi, DocumentDef _ValueDef = RowDocument>
DOCDB_CXX20_REQUIRES(IndexFn<_IndexFn, Storage, _ValueDef>)
class Indexer: public IndexView<Storage, _ValueDef, index_type> {
public:

    static constexpr _IndexFn indexFn = {};
    static constexpr IndexerRevision revision = _IndexFn::revision;
    static constexpr Purpose _purpose = index_type == IndexType::multi || index_type == IndexType::unique_hide_dup?Purpose::index:Purpose::unique_index;

    using DocType = typename Storage::DocType;
    using ValueType = typename _ValueDef::Type;


    Indexer(Storage &storage, std::string_view name)
        :Indexer(storage, storage.get_db()->open_table(name, _purpose)) {}

    Indexer(Storage &storage, KeyspaceID kid)
        :IndexView<Storage, _ValueDef, index_type>(storage.get_db(), kid, Direction::forward, {}, false,storage)
         ,_listener(this)
    {
        auto stored_state = get_stored_state();
        _last_written_rev = stored_state.storage_revision;
        if (stored_state.indexer_revision != revision) {
            reindex();
        } else {
            reindex_from(stored_state.storage_revision);
        }
        this->_storage.register_transaction_observer(make_observer());
    }


    IndexerState get_stored_state() const {
        auto k = this->_db->get_private_area_key(this->_kid);
        auto doc = this->_db->template get_as_document<FoundRecord<RowDocument> >(k);
        if (doc.has_value()) {
            auto [cur_rev, stor_rev] = doc->template get<IndexerRevision, DocID>();
            return {cur_rev,stor_rev};
        }
        else return {0,0};
    }

    using TransactionObserver = std::function<void(Batch &b, const Key& key, const ValueType &value, DocID docId, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    void rescan_for(TransactionObserver obs) {
        Batch b;
        for (const auto &x: this->select_all()) {
            if (b.is_big()) {
                this->_db->commit_batch(b);
            }
            obs(b, x.key, x.value, x.id, false);
        }
        this->_db->commit_batch(b);
    }


    struct IndexedDoc {
        DocID cur_doc;
        DocID prev_doc;
    };

    template<bool deleting>
    class Emit {
    public:
        static constexpr bool erase = deleting;

        Emit(Indexer &owner, Batch &b, const IndexedDoc &docinfo)
            :_owner(owner), _b(b), _docinfo(docinfo) {}

        void operator()(Key &&key, const ValueType &value) {put(key, value);}
        void operator()(Key &key, const ValueType &value) {put(key, value);}
        void operator()(Key &&key) {put(key, ValueType());}
        void operator()(Key &key) {put(key, ValueType());}

        DocID id() const {return _docinfo.cur_doc;}
        DocID prev_id() const {return _docinfo.prev_doc;}

    protected:
        Indexer &_owner;
        Batch &_b;
        const IndexedDoc &_docinfo;

        void put(Key &key, const ValueType &value) {
            key.change_kid(_owner._kid);
            auto &buffer = _b.get_buffer();
            auto buff_iter = std::back_inserter(buffer);
            if constexpr(index_type == IndexType::unique && !deleting) {
               auto st =  _owner._locker.lock_key(_b.get_revision(), key, _docinfo.cur_doc, _docinfo.prev_doc);
               if (!st.locked) {
                   throw make_exception(key, _owner._db, _docinfo.cur_doc, st.locked_for);
               }
               if (!st.replaced) {
                   std::string tmp;
                   if (_owner._db->get(key, tmp)) {
                       auto [r] = Row::extract<DocID>(tmp);
                       if (r != _docinfo.cur_doc && r != _docinfo.prev_doc) {
                           throw make_exception(key, _owner._db, _docinfo.cur_doc, r);
                       }
                   }
               }
            }
            //check for duplicate key
            if constexpr(index_type == IndexType::multi || index_type == IndexType::unique_hide_dup) {
                key.append(_docinfo.cur_doc);
                if (!deleting) {
                    _ValueDef::to_binary(value, buff_iter);
                }
            } else {
                if (!deleting) {
                    Row::serialize_items(buff_iter, _docinfo.cur_doc);
                    _ValueDef::to_binary(value, buff_iter);
                }
            }
            if (deleting) {
                _b.Delete(key);
            } else {
                _b.Put(key, buffer);
            }
            _owner.notify_tx_observers(_b, key, value, _docinfo.cur_doc, deleting);
        }
    };


    void update() {
        //empty, as the index don't need update, but some object may try to call it
    }

protected:

    class Listener:public AbstractBatchNotificationListener { // @suppress("Miss copy constructor or assignment operator")
    public:
        Indexer *owner = nullptr;
        Listener(Indexer *owner):owner(owner) {}
        virtual void before_commit(Batch &b) override {
            owner->close_pending_rev(b);
        }
        virtual void after_commit(std::size_t rev) noexcept override {
            if constexpr(index_type == IndexType::unique) {
                owner->_locker.unlock_keys(rev);
            }
        };
        virtual void after_rollback(std::size_t rev) noexcept override  {
            if constexpr(index_type == IndexType::unique) {
                owner->_locker.unlock_keys(rev);
            }
        };
    };

    std::vector<TransactionObserver> _tx_observers;
    using KeyLocker = std::conditional_t<index_type == IndexType::unique, KeyLock<DocID>, std::nullptr_t>;
    KeyLocker _locker;
    Listener _listener;
    std::atomic<DocID> _last_written_rev = 0;
    std::atomic<std::size_t> _pending_writes = {0};



    using Update = typename Storage::Update;

    auto make_observer() {
        return [&](Batch &b, const Update &update) {
            record_pending_rev(update.new_doc_id);
            b.add_listener(&_listener);
            if constexpr(index_type == IndexType::unique || index_type == IndexType::unique_no_check) {
                if (!update.new_doc) {
                    indexFn(Emit<true>(*this, b, IndexedDoc{update.old_doc_id, update.old_old_doc_id}), *update.old_doc);
                } else {
                    indexFn(Emit<false>(*this, b, IndexedDoc{update.new_doc_id, update.old_doc_id}), *update.new_doc);
                }
            } else {
                if (update.old_doc) {
                    indexFn(Emit<true>(*this, b, IndexedDoc{update.old_doc_id, update.old_old_doc_id}), *update.old_doc);
                }
                if (update.new_doc) {
                    indexFn(Emit<false>(*this, b, IndexedDoc{update.new_doc_id, update.old_doc_id}), *update.new_doc);
                }
            }
        };
    }

    void record_pending_rev(DocID id) {
        _pending_writes.fetch_add(1, std::memory_order_relaxed);
        auto cur_id = _last_written_rev.load(std::memory_order_relaxed);
        while (cur_id < id
                && !_last_written_rev.compare_exchange_weak(cur_id, id, std::memory_order_relaxed));
    }

    void close_pending_rev(Batch &b) {
        auto cur_id = _last_written_rev.load(std::memory_order_relaxed);
        if (_pending_writes.fetch_sub(1, std::memory_order_relaxed) == 1) {
            //this is last pending write
            //so now update revision and last_written_rev
            //revision is +1 from last written doc
            b.Put(Database::get_private_area_key(this->_kid), Row(revision, cur_id+1));
        }
    }

    void update_revision() {
        Batch b;
        update_rev(b);
        this->_db->commit_batch(b);
    }

    void reindex() {
        this->_db->clear_table(this->_kid, false);
        reindex_from(0);
    }
    void reindex_from(DocID doc) {
        this->_storage.rescan_for(make_observer(), doc);
    }

    void notify_tx_observers(Batch &b, const Key &key, const ValueType &value, DocID id, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, value, id, erase);
        }
    }

    void check_for_dup_key(const Key &key, DocID prev_doc, DocID cur_doc) {
        std::string tmp;
        if (this->_db->get(key, tmp)) {
            auto [srcid] = Row::extract<DocID>(tmp);
            if (srcid != prev_doc) {
                throw DuplicateKeyException(key, this->_db, cur_doc, srcid);
            }
        }
    }

    void update_rev(Batch &) {
        //empty
    }

    static DuplicateKeyException make_exception(Key key, const PDatabase &db, DocID income, DocID stored) {
        std::string message("Duplicate key found in index: ");
        auto name = db->name_from_id(key.get_kid());
        if (name.has_value()) message.append(*name);
        else message.append("Unknown table KID ").append(std::to_string(static_cast<int>(key.get_kid())));
        message.append(". Indexed document: ").append(std::to_string(stored));
        message.append(". Conflicting document: ").append(std::to_string(income));
        return DuplicateKeyException(std::string(std::string_view(key)), std::move(message));

    }
};

}



#endif /* SRC_DOCDB_INDEXER_H_ */
