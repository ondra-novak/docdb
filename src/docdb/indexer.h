#pragma once
#ifndef SRC_DOCDB_INDEXER_H_
#define SRC_DOCDB_INDEXER_H_

#include "concepts.h"
#include "database.h"
#include "keylock.h"
#include "exceptions.h"

#include "index_view.h"


namespace docdb {

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
   requires std::invocable<T, IndexerEmitTemplate<_ValueDef>, typename Storage::DocType>;
   {T::revision} -> std::convertible_to<IndexRevision>;
});



template<std::size_t rev, auto function>
struct IndexFunction: decltype(function) {
    static constexpr std::size_t revision = rev;
};


template<DocumentStorageType Storage, typename _IndexFn, IndexType index_type = IndexType::multi, DocumentDef _ValueDef = RowDocument>
DOCDB_CXX20_REQUIRES(IndexFn<_IndexFn, Storage, _ValueDef>)
class Indexer: public IndexView<Storage, _ValueDef, index_type> {
public:

    static constexpr _IndexFn indexFn = {};
    static constexpr IndexRevision revision = _IndexFn::revision;
    static constexpr Purpose _purpose = index_type == IndexType::multi || index_type == IndexType::unique_hide_dup?Purpose::index:Purpose::unique_index;

    using DocType = typename Storage::DocType;
    using ValueType = typename _ValueDef::Type;


    Indexer(Storage &storage, std::string_view name)
        :Indexer(storage, storage.get_db()->open_table(name, _purpose)) {}

    Indexer(Storage &storage, KeyspaceID kid)
        :IndexView<Storage, _ValueDef, index_type>(storage.get_db(), kid, Direction::forward, {}, false,storage)
         ,_listener(this)
    {
        auto k = this->_db->get_private_area_key(this->_kid);
        auto doc = this->_db->get(k);
        if (!doc.has_value()) {
            reindex();
        } else {
            auto [rev, id] = Row::extract<IndexRevision, DocID>(*doc);
            if (rev != revision) {
                reindex();
            } else {
                _last_seen_id = id;
                storage.rescan_for(make_observer(), id+1);
            }
        }
        this->_storage.register_transaction_observer(make_observer());
    }

    Indexer(const Indexer &) = delete;
    Indexer &operator=(const Indexer &) = delete;

    using TransactionObserver = std::function<void(Batch &b, const Key& key, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    void rescan_for(TransactionObserver obs) {
        auto b = this->_db->begin_batch();
        for (const auto &x: this->select_all()) {
            b.reset();
            obs(b, x.key, false);
            b.commit();
        }
    }

    struct IndexedDoc {
        DocID cur_doc;
        DocID prev_doc;
    };

    using KeyLocker = std::conditional_t<index_type == IndexType::unique, KeyLock<DocID>, std::nullptr_t>;


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
            auto &k = key.set_kid(_owner._kid);
            auto &buffer = _b.get_buffer();
            auto buff_iter = std::back_inserter(buffer);
            if constexpr(index_type == IndexType::unique) {
                DocID need_doc = deleting?_docinfo.cur_doc:0;
                DocID new_doc = deleting?0:_docinfo.cur_doc;
                auto st = _owner._locker.lock_key_cas(_b.get_revision(),k, need_doc, new_doc);
                switch (st) {
                    default: break;
                    case KeyLockState::ok: {
                            auto val = _owner._db->get(k);
                            if (val) {
                                auto [r] = Row::extract<DocID>(*val);
                                if (r != _docinfo.cur_doc) {
                                    throw make_exception(key, _owner._db, _docinfo.cur_doc, r);
                                }
                            }
                        }
                        break;
                    case KeyLockState::cond_failed:
                        throw make_exception(key, _owner._db, _docinfo.cur_doc, need_doc);
                    case KeyLockState::deadlock:
                        throw make_deadlock_exception(key, _owner._db);
                }
            }
            //check for duplicate key
            if constexpr(index_type == IndexType::multi || index_type == IndexType::unique_hide_dup) {
                k.append(_docinfo.cur_doc);
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
                _b.Delete(k);
            } else {
                _b.Put(k, buffer);
            }
            _owner.notify_tx_observers(_b, key, deleting);
        }
    };


    void update() {
        //empty, as the index don't need update, but some object may try to call it
    }
    bool try_update() {
       //empty, as the index don't need update, but some object may try to call it
        return true;
    }

protected:

    class Listener:public AbstractBatchNotificationListener { // @suppress("Miss copy constructor or assignment operator")
    public:
        Indexer *owner = nullptr;
        Listener(Indexer *owner):owner(owner) {}
        virtual void before_commit(Batch &b) override {
            owner->update_rev(b);
        }
        virtual void after_commit(std::size_t rev) noexcept override {
            if constexpr(index_type == IndexType::unique) {
                owner->_locker.unlock_keys(rev);
            }
        };
        virtual void on_rollback(std::size_t rev) noexcept override  {
            if constexpr(index_type == IndexType::unique) {
                owner->_locker.unlock_keys(rev);
            }
        };
    };


    std::vector<TransactionObserver> _tx_observers;
    KeyLocker _locker;
    Listener _listener;
    std::atomic<DocID> _last_seen_id;

    using Update = typename Storage::Update;

    auto make_observer() {
        return [&](Batch &b, const Update &update) {
            b.add_listener(&_listener);
            {
                if (update.old_doc) {
                    indexFn(Emit<true>(*this, b, IndexedDoc{update.old_doc_id, update.old_old_doc_id}), *update.old_doc);
                }
                if (update.new_doc) {
                    indexFn(Emit<false>(*this, b, IndexedDoc{update.new_doc_id, update.old_doc_id}), *update.new_doc);
                    update_id(update.new_doc_id);
                }
            }
        };
    }


    void reindex() {
        this->_db->clear_table(this->_kid, false);
        this->_storage.rescan_for(make_observer(), 0);
    }
    void notify_tx_observers(Batch &b, const Key &key, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, erase);
        }
    }

    void update_id(DocID id) {
        DocID cur = _last_seen_id.load(std::memory_order_relaxed);
        DocID m;
        do {
             m = std::max(id, cur);
        } while (!_last_seen_id.compare_exchange_strong(cur, m, std::memory_order_relaxed));
    }

    void update_rev(Batch &b) {
        auto k = this->_db->get_private_area_key(this->_kid);
        b.Put(k, Row(revision, _last_seen_id.load(std::memory_order_relaxed)));
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

    static DeadlockKeyException make_deadlock_exception(Key key, const PDatabase &db) {
        std::string message("Deadlock (key locking): ");
        auto name = db->name_from_id(key.get_kid());
        if (name.has_value()) message.append(*name);
        else message.append("Unknown table KID ").append(std::to_string(static_cast<int>(key.get_kid())));
        return DeadlockKeyException(std::string(std::string_view(key)), std::move(message));

    }
};

}



#endif /* SRC_DOCDB_INDEXER_H_ */
