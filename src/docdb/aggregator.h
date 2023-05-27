#pragma once
#ifndef SRC_DOCDB_AGGREGATOR_H_
#define SRC_DOCDB_AGGREGATOR_H_
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


template<typename MapSrc, auto keyReduceFn, auto aggregatorFn,
                AggregatorRevision revision, typename _ValueDef = RowDocument>
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
    Aggregator(MapSrc &source, std::string_view name, bool manual_update = false)
        :Aggregator(source, source.get_db()->open_table(name, Purpose::aggregation), manual_update) {}
    Aggregator(MapSrc &source, KeyspaceID kid, bool manual_update = false)
        :AggregatorView<_ValueDef>(source.get_db(), kid, Direction::forward, {})
        ,_source(source)
        ,_tcontrol(*this)
        ,_manual_update(manual_update){

        if (get_revision() != revision) {
            rebuild();
        }
        this->_source.register_transaction_observer(make_observer());
    }

    AggregatorRevision get_revision() const {
        auto k = this->_db->get_private_area_key(this->_kid);
        auto doc = this->_db->template get_as_document<Document<RowDocument> >(k);
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
        _tcontrol.sync();
    }

    ///Perform manual update
    void update() {
        _tcontrol.update();
        _tcontrol.sync();
    }

    ///Synchronize with background task
    /**
     * Block thread until any background task currently pending is finished.
     *
     * Call this function after insert if you need to retrieve result of aggregation
     */
    void sync() {
        if (_manual_update) update();
        else _tcontrol.sync();
    }

protected:

    class TaskControl: public AbstractBatchNotificationListener {
    public:
        TaskControl(Aggregator &owner):_owner(owner) {}
        TaskControl(const TaskControl &owner) = delete;
        TaskControl &operator=(const TaskControl &owner) = delete;

        virtual void after_commit(std::size_t) noexcept override {
            update();
        }
        virtual void before_commit(Batch &b) override {
            if (_e) {
                std::exception_ptr e = std::move(_e);
                std::rethrow_exception(e);
            }
        }
        virtual void after_rollback(std::size_t rev) noexcept override {}
        std::size_t get_pending_counter() const {
            return _pending_counter;
        }
        void update() {
            if (!_active.test_and_set(std::memory_order_relaxed)) {
                ++_pending_counter;
                _owner._db->run_async([this] {
                    _active.clear();
                    try {
                        _owner.run_aggregation();
                        ++_done_counter;
                        _done_counter.notify_all();
                    } catch (...) {
                        _e = std::current_exception();
                    }
                });
            }
        }

        void sync() {
            std::size_t r = _pending_counter.load(std::memory_order_relaxed);
            std::size_t cr = _done_counter.load(std::memory_order_relaxed);
            while (r > cr) {
                _done_counter.wait(cr, std::memory_order_relaxed);
                cr = _done_counter.load(std::memory_order_relaxed);
            }
        }




    protected:
        Aggregator &_owner;
        std::atomic_flag _active;
        std::atomic<std::size_t> _pending_counter = 0;
        std::atomic<std::size_t> _done_counter = {};
        std::exception_ptr _e;
    };

    MapSrc &_source;
    std::vector<TransactionObserver> _tx_observers;
    TaskControl _tcontrol;
    bool _manual_update;


    void run_aggregation() {
        Batch b;
        RecordSetBase rs(this->_db->make_iterator(false, {}), {
                Database::get_private_area_key(this->_kid),
                Database::get_private_area_key(this->_kid+1),
                FirstRecord::excluded,
                LastRecord::excluded
        });

        RawKey prev_key((RowView()));

        while (!rs.is_at_end()) {
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
                        auto prev = this->get(nk);
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

    auto make_observer() {
         return [&](Batch &b, const Key &k, const DocType &value, bool erase) {
             if (erase) {
                 keyReduceFn(Emit<true>(*this, b), Key(RowView(k)), value);
             } else {
                 keyReduceFn(Emit<false>(*this, b), Key(RowView(k)), value);
             }
             if (!_manual_update) {
                 b.add_listener(&_tcontrol);
             }
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
