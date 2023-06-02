#pragma once
#ifndef SRC_DOCDB_AGGREGATOR_H_
#define SRC_DOCDB_AGGREGATOR_H_
#include "aggregrator_source_concept.h"
#include "database.h"
#include "index_view.h"
#include <atomic>

namespace docdb {

template<typename ValueType>
class AggregatedResult {
public:


    AggregatedResult():_has_value(false) {}
    template<typename Arg, typename ... Args>
    AggregatedResult(Arg && p1, Args && ... px)
        :_value(std::forward<Arg>(p1), std::forward<Args>(px)...)
        ,_has_value(true) {}

    AggregatedResult(const AggregatedResult &other)
        :_has_value(other._has_value) {
        if (_has_value) {
            ::new(&_value) ValueType(other._value);
        }
    }
    AggregatedResult(AggregatedResult &&other)
        :_has_value(other._has_value) {
        if (_has_value) {
            ::new(&_value) ValueType(std::move(other._value));
        }
    }
    AggregatedResult &operator=(const AggregatedResult &other) {
        if (this != &other) {
            this->~AggregatedResult();
            ::new(this) AggregatedResult(other);
        }
        return *this;
    }

    AggregatedResult &operator=(AggregatedResult &&other) {
        if (this != &other) {
            this->~AggregatedResult();
            ::new(this) AggregatedResult(std::move(other));
        }
        return *this;
    }
    ~AggregatedResult(){
        if (_has_value) _value.~ValueType();
    }


    union {
        ValueType _value;
    };
    const bool _has_value;
};

template<DocumentDef _ValueDef>
using AggregatorView = IndexViewGen<_ValueDef, IndexViewBaseEmpty>;

using AggregatorRevision = std::size_t;

class AggregatedKey: public RawKey {
public:
    template<typename ... Args>
    AggregatedKey(const Args & ... args):RawKey(0, KeyspaceID(0), args...) {}

    void change_kid(const RawKey &src) {
        std::copy(src.begin(), src.end(), this->mutable_ptr());
    }
    Key get_as_key() const {
        return Key(RowView(std::string_view(*this).substr(sizeof(KeyspaceID))));
    }


};

///Defines update mode for the aggregation
enum class UpdateMode {
    /**
     * Update must be performed by calling a function update()
     */
    manual,
     /**
      * Update is automatic by starting background task, when one
      * or many keys are invalidated.
      *
      * You can still run manual update by calling function update() if you
      * need to ensure, that all calculations are done in time
      */
    automatic
};


template<AggregatorSource MapSrc, auto keyReduceFn, auto aggregatorFn,
                AggregatorRevision revision, UpdateMode update_mode, typename _ValueDef = RowDocument>
class Aggregator: public AggregatorView<_ValueDef> {
public:

    using ValueType = typename _ValueDef::Type;
    using DocType = typename MapSrc::ValueType;

    ///Construct aggregator
    /**
     * @param source source index
     * @param name name of keyspace where materialized aggregation is stored
     * @param manual_update use true to disable background update. In this case, you
     * need manually call update() to finish pending aggregations
     *
     */
    Aggregator(MapSrc &source, std::string_view name)
        :Aggregator(source, source.get_db()->open_table(name, Purpose::aggregation)) {}
    Aggregator(MapSrc &source, KeyspaceID kid)
        :AggregatorView<_ValueDef>(source.get_db(), kid, Direction::forward, {}, false)
        ,_source(source)
        ,_tcontrol(*this){

        if (get_revision() != revision) {
            rebuild();
        }
        this->_source.register_transaction_observer(make_observer());
    }

    AggregatorRevision get_revision() const {
        auto k = this->_db->get_private_area_key(this->_kid);
        auto doc = this->_db->template get_as_document<FoundRecord<RowDocument> >(k);
        if (doc.has_value()) {
            auto [cur_rev] = doc->template get<AggregatorRevision>();
            return cur_rev;
        }
        else return 0;
    }

    using TransactionObserver = std::function<void(Batch &b, const AggregatedKey& key, const ValueType &value, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    template<bool deleting>
    class Emit {
    public:
        static constexpr bool erase = deleting;

        Emit(Aggregator &owner, Batch &b)
            :_owner(owner), _b(b) {}

        void operator()(AggregatedKey &&key, const Key &value) {put(key, value);}
        void operator()(AggregatedKey &key, const Key &value) {put(key, value);}

    protected:
        Aggregator &_owner;
        Batch &_b;


        void put(AggregatedKey &key, const Key &value) {
            key.change_kid(Database::get_private_area_key(_owner._kid));
            key.append(_b.get_revision());
            auto &buffer = _b.get_buffer();
            auto buff_iter = std::back_inserter(buffer);
            _ValueDef::to_binary(value, buff_iter);
            _b.Put(key, buffer);
        }
    };


    ~Aggregator() {
        join();
    }

    ///Perform manual update
    /**Manual update still is possible in automatic mode.
     */
    void update() {
        _source.update();
        this->do_update();
    }

    ///Synchronizes current thread with finishing of background tasks
    /**
     * It is used in destructor, but you can use it anywhere you need. This
     * blocks current thread until background update is finished. Note that
     * function works with automatic background updates, not updates called
     * manually by the update() function.
     *
     * @note If many records are being inserted which triggers more and
     * more updates, the function cannot unblock. You need to stop pushing
     * new updates.
     */
    void join() {
        int v;
        v = _pending.load(std::memory_order_relaxed);
        while (v) {
            _pending.wait(v, std::memory_order_relaxed);
            v = _pending.load(std::memory_order_relaxed);
        }
    }

protected:

    class TaskControl: public AbstractBatchNotificationListener {
    public:
        TaskControl(Aggregator &owner):_owner(owner) {}
        TaskControl(const TaskControl &owner) = delete;
        TaskControl &operator=(const TaskControl &owner) = delete;

        virtual void after_commit(std::size_t) noexcept override {
            if (_owner.mark_dirty()) {
                if constexpr(update_mode == UpdateMode::automatic) {
                    ++_owner._pending;
                    _owner._db->run_async([this] {
                        try {
                            _owner.run_aggregation();
                        } catch (...) {
                            _e = std::current_exception();
                        }
                        auto &ref = _owner._pending;
                        if (--ref == 0) {
                            ref.notify_all();
                        }
                    });
                }
            }
        }
        virtual void before_commit(Batch &b) override {
            if (_e) {
                std::exception_ptr e = std::move(_e);
                std::rethrow_exception(e);
            }
        }
        virtual void after_rollback(std::size_t rev) noexcept override {}




    protected:
        Aggregator &_owner;
        std::exception_ptr _e;
    };

    MapSrc &_source;
    std::vector<TransactionObserver> _tx_observers;
    TaskControl _tcontrol;
    //object has been market dirty, must be updated
    //in automatic mode, this also indicates, that background thread is scheduled
    std::atomic<bool> _dirty = {true};
    //count of pending background requests. It is possible to have 2 of them, one processing, other pending
    std::atomic<int> _pending = {0};
    //locks aggregation to be procesed one thread at time
    std::mutex _mx;

    bool mark_dirty() {
        bool need = false;
        return _dirty.compare_exchange_strong(need, true,std::memory_order_relaxed);
    }

    bool do_update() {
        std::lock_guard _(_mx);
        bool need = true;
        if (!_dirty.compare_exchange_strong(need, false, std::memory_order_relaxed)) {
            return false;
        }
        run_aggregation();
        return true;
    }


    void run_aggregation() {
        Batch b;
        RecordSetBase rs(this->_db->make_iterator({}, this->_no_cache), {
                Database::get_private_area_key(this->_kid),
                Database::get_private_area_key(this->_kid+1),
                FirstRecord::excluded,
                LastRecord::excluded
        });

        RawKey prev_key((RowView()));

        while (!rs.empty()) {
            std::string_view keydata = rs.raw_key();
            std::string_view valuedata = rs.raw_value();
            keydata = keydata.substr(sizeof(KeyspaceID), keydata.length()-sizeof(KeyspaceID)- sizeof(std::size_t));
            RawKey nk((RowView(keydata)));
            if (nk != prev_key) {
                prev_key = nk;
                prev_key.mutable_buffer();
                AggregatedResult<ValueType> r = aggregatorFn(_source, Key(RowView(valuedata)));
                if (r._has_value) {
                    auto &buff = b.get_buffer();
                    _ValueDef::to_binary(r._value, std::back_inserter(buff));
                    b.Put(nk, buff);
                    notify_tx_observers(b, nk, r._value, false);
                } else {
                    if (_tx_observers.empty()) {
                        b.Delete(nk);
                    } else {
                        auto prev = this->find(nk);
                        if (prev.has_value()) {
                            b.Delete(nk);
                            notify_tx_observers(b, nk, *prev, true);
                        }
                    }
                }
            }
            b.Delete(to_slice(rs.raw_key()));
            rs.next();
        }
        this->_db->commit_batch(b);

    }

    template<typename ... Args>
    void call_emit(bool erase, Batch &b, const Key &k, Args  ... args) {
        if (erase) {
            keyReduceFn(Emit<true>(*this, b), Key(RowView(k)), args...);
        } else {
            keyReduceFn(Emit<false>(*this, b), Key(RowView(k)), args...);
        }
    }

    auto make_observer() {
         return [&](Batch &b, const Key &k, const DocType &value, DocID docid, bool erase) {
             if constexpr(std::invocable<decltype(keyReduceFn), Emit<false>, Key>) {
                 call_emit(erase, b, k);
             }else if constexpr(std::invocable<decltype(keyReduceFn), Emit<false>, Key, const DocType &>) {
                 call_emit(erase, b, k, value);
             }else if constexpr(std::invocable<decltype(keyReduceFn), Emit<false>, Key, const DocType &, DocID>) {
                 call_emit(erase, b, k, value, docid);
             } else {
                 struct X{};
                 static_assert(defer_false<X>, "Key reduce function has unsupported prototype");
             }
             b.add_listener(&_tcontrol);
         };
     }
    void update_revision() {
        Batch b;
        b.Put(this->_db->get_private_area_key(this->_kid), Row(revision));
        this->_db->commit_batch(b);
    }

    void rebuild() {
        this->_db->clear_table(this->_kid, false);
        this->_source.rescan_for(make_observer());
        update_revision();
    }

    void notify_tx_observers(Batch &b, const AggregatedKey &key, const ValueType &value, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, value, erase);
        }
    }

};

template<typename ... Args>
constexpr auto reduceKey() {
    return [](auto emit, Key key, const auto &val) {
        auto kk = key.get<Args...>();
        emit(kk, kk);
    };
}

}




#endif /* SRC_DOCDB_AGGREGATOR_H_ */
