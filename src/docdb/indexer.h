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
        auto doc = this->_db->template get_as_document<FoundRecord<RowDocument> >(k);
        do {
            if (doc.has_value()) {
                auto [rev] = doc->template get<IndexRevision>();
                if (rev == revision) break;
            }
            reindex();
        }
        while (false);
        this->_storage.register_transaction_observer(make_observer());
    }



    using TransactionObserver = std::function<void(Batch &b, const Key& key, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    void rescan_for(TransactionObserver obs) {
        Batch b;
        for (const auto &x: this->select_all()) {
            if (b.is_big()) {
                this->_db->commit_batch(b);
            }
            obs(b, x.key, false);
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
        std::optional<ValueType> find(Key &key) const {

        }

    protected:
        Indexer &_owner;
        Batch &_b;
        const IndexedDoc &_docinfo;

        void put(Key &key, const ValueType &value) {
            key.change_kid(_owner._kid);
            auto &buffer = _b.get_buffer();
            auto buff_iter = std::back_inserter(buffer);
            if constexpr(index_type == IndexType::unique && !deleting) {
               if (!_owner._locker.lock_key(_b.get_revision(), key)) {
                   throw make_deadlock_exception(key, _owner._db);
               }
               std::string tmp;
               if (_owner._db->get(key, tmp)) {
                   auto [r] = Row::extract<DocID>(tmp);
                   if (r != _docinfo.cur_doc && r != _docinfo.prev_doc) {
                       throw make_exception(key, _owner._db, _docinfo.cur_doc, r);
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
    bool try_update() {
       //empty, as the index don't need update, but some object may try to call it
        return true;
    }

protected:

    class Listener:public AbstractBatchNotificationListener { // @suppress("Miss copy constructor or assignment operator")
    public:
        Indexer *owner = nullptr;
        Listener(Indexer *owner):owner(owner) {}
        virtual void before_commit(Batch &b) override {}
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
    using KeyLocker = std::conditional_t<index_type == IndexType::unique, KeyLock, std::nullptr_t>;
    KeyLocker _locker;
    Listener _listener;

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
                }
            }
        };
    }


    void reindex() {
        this->_db->clear_table(this->_kid, false);
        this->_storage.rescan_for(make_observer(), 0);
        Batch b;
        b.Put(Database::get_private_area_key(this->_kid), Row(revision));
        this->_db->commit_batch(b);
    }
    void reindex_from(DocID doc) {
    }

    void notify_tx_observers(Batch &b, const Key &key, const ValueType &value, DocID id, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, erase);
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
