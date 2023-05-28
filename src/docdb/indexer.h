#pragma once
#ifndef SRC_DOCDB_INDEXER_H_
#define SRC_DOCDB_INDEXER_H_

#include "concepts.h"
#include "database.h"
#include "keylock.h"

#include "index_view.h"
namespace docdb {

using IndexerRevision = std::size_t;


template<DocumentDef _ValueDef>
struct IndexerEmitTemplate {
    void operator()(Key,typename _ValueDef::Type);
    static constexpr bool erase = false;
    DocID id() const;
    DocID prev_id() const;
};
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

template<DocumentStorageType Storage, auto indexFn, IndexerRevision revision, IndexType index_type = IndexType::multi, DocumentDef _ValueDef = RowDocument>
CXX20_REQUIRES(std::invocable<decltype(indexFn), IndexerEmitTemplate<_ValueDef>, const typename Storage::DocType &>)
class Indexer: public IndexView<Storage, _ValueDef, index_type> {
public:

    static constexpr Purpose _purpose = index_type == IndexType::multi || index_type == IndexType::unique_hide_dup?Purpose::index:Purpose::unique_index;

    using DocType = typename Storage::DocType;
    using ValueType = typename _ValueDef::Type;


    Indexer(Storage &storage, std::string_view name)
        :Indexer(storage, storage.get_db()->open_table(name, _purpose)) {}

    Indexer(Storage &storage, KeyspaceID kid)
        :IndexView<Storage, _ValueDef, index_type>(storage.get_db(), kid, Direction::forward, {}, storage)
         ,_unlocker(this)
    {
        if (get_revision() != revision) {
            reindex();
        }
        this->_storage.register_transaction_observer(make_observer());
    }


    IndexerRevision get_revision() const {
        auto k = this->_db->get_private_area_key(this->_kid);
        auto doc = this->_db->template get_as_document<Document<RowDocument> >(k);
        if (doc.has_value()) {
            auto [cur_rev] = doc->template get<IndexerRevision>();
            return cur_rev;
        }
        else return 0;
    }

    using TransactionObserver = std::function<void(Batch &b, const Key& key, const ValueType &value, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    void rescan_for(TransactionObserver obs) {
        Batch b;
        for (const auto &x: this->select_all()) {
            if (b.is_big()) {
                this->_db->commit_batch(b);
            }
            obs(b, x.key, x.value, false);
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
                   throw DuplicateKeyException(key, _owner._db, _docinfo.cur_doc, st.locked_for);
               }
               if (!st.replaced) {
                   std::string tmp;
                   if (_owner._db->get(key, tmp)) {
                       auto [r] = Row::extract<DocID>(tmp);
                       if (r != _docinfo.prev_doc) {
                           throw DuplicateKeyException(key, _owner._db, _docinfo.cur_doc, r);
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
            _owner.notify_tx_observers(_b, key, value, deleting);
        }
    };


    void update() {
        //empty, as the index don't need update, but some object may try to call it
    }

protected:

    class Unlocker:public AbstractBatchNotificationListener {
    public:
        Indexer *owner = nullptr;
        Unlocker(Indexer *owner):owner(owner) {}
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
    using KeyLocker = std::conditional_t<index_type == IndexType::unique, KeyLock<DocID>, std::nullptr_t>;
    KeyLocker _locker;
    Unlocker _unlocker;




    using Update = typename Storage::Update;

    auto make_observer() {
        return [&](Batch &b, const Update &update) {
            if constexpr(index_type == IndexType::unique || index_type == IndexType::unique_no_check) {
                if constexpr(index_type == IndexType::unique) {
                    b.add_listener(&_unlocker);
                }
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

    void update_revision() {
        Batch b;
        b.Put(this->_db->get_private_area_key(this->_kid), Row(revision));
        this->_db->commit_batch(b);
    }

    void reindex() {
        this->_db->clear_table(this->_kid, false);
        this->_storage.rescan_for(make_observer());
        update_revision();
    }

    void notify_tx_observers(Batch &b, const Key &key, const ValueType &value, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, value, erase);
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
};

}



#endif /* SRC_DOCDB_INDEXER_H_ */
