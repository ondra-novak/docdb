#pragma once
#ifndef SRC_DOCDB_INCAGGR_H_
#define SRC_DOCDB_INCAGGR_H_

#include "concepts.h"
#include "database.h"
#include "keylock.h"
#include "exceptions.h"

#include "index_view.h"
namespace docdb {


template<typename Fn, typename ValueDef>
DOCDB_CXX20_CONCEPT(AggrMergeFn, requires(Fn fn, FoundRecord<ValueDef> value) {
    {fn(value)}->std::convertible_to<typename ValueDef::Type>;
});

template<DocumentDef _ValueDef>
struct AggregatorEmitTemplate {
    struct Unspec{};
    Unspec operator()(Key);
    DocID id() const;
    DocID prev_id() const;
};

template<typename T, typename Storage, typename _ValueDef>
DOCDB_CXX20_CONCEPT(IndexFn, requires{
   requires std::invocable<T, AggregatorEmitTemplate<_ValueDef>, typename Storage::DocType>;
   {T::revision} -> std::convertible_to<IndexRevision>;
});


template<std::size_t rev, auto function>
struct IncAggregatorFunction: decltype(function) {
    static constexpr std::size_t revision = rev;
};

template<DocumentDef _ValueDef>
using MapView = IndexViewGen<_ValueDef, IndexViewBaseEmpty<_ValueDef> >;

///Implements materialized aggregated view where aggregation are done incrementally
/**
 * This type of aggregator is ideal for sums and averages, whethever you can update
 * aggregation depend on whether the document is added or removed. You can also
 * perform other aggregations in environment, where documents are not removed.
 * Aggregation must be performed incrementally. Non-incremental aggreagation
 * will not work properly
 *
 * Incremental aggregator works as an unique index. During indexing, you
 * can read the current value for the given key and you can put update of the
 * value. For example if you calculates sum aggregation, you just add new value
 * to a current sum and put the new sum value back to the index
 *
 * @tparam Storage Source storage type
 * @tparam _IndexFn Index function see below
 * @tparam _ValueDef Definition of value (document type), default is RowDocument
 *
 * The index function is a callable class of following declaration
 *
 * @code
 * struct IndexFn {
 *      static constexpr IndexRevision revision = 1;
 *      template<typename Emit>
 *      void operator()(Emit emit, const Document &doc) const {
 *          auto key = ....; //key from doc
 *          auto value = ....;
 *          auto cur_value_row = emit(key);
 *          if (emit.erase) {
 *              if (cur_value_row) {
 *                  auto cur_value = cur_value_row.get<>();
 *                  auto new_value = cur_value - value;
 *                  cur_value_row.put(new_value);
 *              }
 *          } else {
 *              if (cur_value_row) {
 *                  auto cur_value = cur_value_row.get<>();
 *                  auto new_value = cur_value + value;
 *                  cur_value_row.put(new_value);
 *              } else {
 *                  cur_value_row.put(value);
 *              }
 *          }
 *      }
 * };
 * @endcode
 */
template<DocumentStorageType Storage, typename _IndexFn, DocumentDef _ValueDef = RowDocument>
DOCDB_CXX20_REQUIRES(IndexFn<_IndexFn, Storage, _ValueDef>)
class IncrementalAggregator: public MapView<_ValueDef> {
public:

    using Super = MapView<_ValueDef>;

    static constexpr _IndexFn indexFn = {};
    static constexpr IndexRevision revision = _IndexFn::revision;
    static constexpr Purpose _purpose = Purpose::unique_index;

    using DocType = typename Storage::DocType;
    using ValueType = typename _ValueDef::Type;


    IncrementalAggregator(Storage &storage, std::string_view name)
        :IncrementalAggregator(storage, storage.get_db()->open_table(name, _purpose)) {}

    IncrementalAggregator(Storage &storage, KeyspaceID kid)
        :Super(storage.get_db(), kid, Direction::forward, {}, false)
         ,_storage(storage)
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

    IncrementalAggregator(const IncrementalAggregator &) = delete;
    IncrementalAggregator &operator=(const IncrementalAggregator &) = delete;



    using TransactionObserver = std::function<void(Batch &b, const Key& key,  bool erase)>;

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

    class CurrentValue: public std::optional<ValueType> {
    public:
        CurrentValue(IncrementalAggregator &owner, Batch &b, RawKey &key)
        :_owner(owner),_b(b),_key(std::move(key))
        {
            if (_owner._db->get(_key, _tmp)) {
                auto at = _tmp.begin();
                auto end = _tmp.end();
                this->emplace(_ValueDef::from_binary(at, end));
            }
        }

        void put(const ValueType &val) {
            auto &buff = _b.get_buffer();
            auto iter = std::back_inserter(buff);
            _ValueDef::to_binary(val, iter);
            _b.Put(_key, buff);
        }
        void erase() {
            _b.Delete(_key);
        }

        CurrentValue(const CurrentValue &) = delete;
        CurrentValue &operator=(const CurrentValue &) = delete;

    protected:
        IncrementalAggregator &_owner;
        Batch &_b;
        RawKey _key;
        std::string _tmp;

    };


    template<bool deleting>
    class Emit {
    public:
        static constexpr bool erase = deleting;

        Emit(IncrementalAggregator &owner, Batch &b, const IndexedDoc &docinfo)
            :_owner(owner), _b(b), _docinfo(docinfo) {}

        CurrentValue operator()(Key &&key) {return put(key);}
        CurrentValue operator()(Key &key) {return put(key);}

        DocID id() const {return _docinfo.cur_doc;}
        DocID prev_id() const {return _docinfo.prev_doc;}

    protected:
        IncrementalAggregator &_owner;
        Batch &_b;
        const IndexedDoc &_docinfo;

        CurrentValue put(Key &key) {
            key.change_kid(_owner._kid);
            if (!_owner._locker.lock_key(_b.get_revision(), key)) {
                throw make_deadlock_exception(key, _owner._db);
            }
            _owner.notify_tx_observers(_b, key, erase);
            return CurrentValue(_owner, _b, key);
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
        IncrementalAggregator *owner = nullptr;
        Listener(IncrementalAggregator *owner):owner(owner) {}
        virtual void before_commit(Batch &b) override {}

        virtual void after_commit(std::size_t rev) noexcept override {
            owner->_locker.unlock_keys(rev);
        };
        virtual void after_rollback(std::size_t rev) noexcept override  {
            owner->_locker.unlock_keys(rev);
        };
    };

    Storage &_storage;
    std::vector<TransactionObserver> _tx_observers;
    KeyLock _locker;
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

    void notify_tx_observers(Batch &b, const Key &key, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, erase);
        }
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



#endif /* SRC_DOCDB_INCAGGR_H_ */
