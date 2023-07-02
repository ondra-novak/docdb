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

struct KeyReduceEmitTemplate {
    void operator()(AggregatedKey key, const Key &value);
    static constexpr bool erase = false;
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

template<typename T, typename Storage>
DOCDB_CXX20_CONCEPT(KeyReduceFn, requires{
    std::invocable<T, KeyReduceEmitTemplate, Key, const typename Storage::DocType &, DocID>
        && std::invocable<T, KeyReduceEmitTemplate, Key, const typename Storage::DocType &>
        && std::invocable<T, KeyReduceEmitTemplate, Key>;
   {T::revision} -> std::convertible_to<AggregatorRevision>;
});

template<typename T, typename Storage>
DOCDB_CXX20_CONCEPT(AggregatorFn, requires{
    std::invocable<T, const Storage &, Key>;
   {T::revision} -> std::convertible_to<AggregatorRevision>;
});


template<AggregatorSource MapSrc, typename _KeyReduceFn, typename _AggregatorFn,
                typename _ValueDef = RowDocument>
DOCDB_CXX20_REQUIRES(KeyReduceFn<_KeyReduceFn, MapSrc> && AggregatorFn<_AggregatorFn, MapSrc>)
class Aggregator: public AggregatorView<_ValueDef> {
public:

    static constexpr _KeyReduceFn keyReduceFn = {};
    static constexpr _AggregatorFn aggregatorFn = {};
    static constexpr AggregatorRevision revision = _KeyReduceFn::revision + 0x9e3779b9 + (_AggregatorFn::revision<<6) + (_AggregatorFn::revision>>2);
    using ValueType = typename _ValueDef::Type;
    using DocType = typename MapSrc::ValueType;

    ///Construct aggregator
    /**
     * @param source source index
     * @param name name of keyspace where materialized aggregation is stored
     * @param autoupdate_treshold  count of dirty keys needed to perform automatic update. You
     * can set this value to 1, if you need to run update after every write. Manual update is
     * possible anytime
     *
     */
    Aggregator(MapSrc &source, std::string_view name, std::size_t autoupdate_threshold = 10000)
        :Aggregator(source, source.get_db()->open_table(name, Purpose::aggregation), autoupdate_threshold) {}
    Aggregator(MapSrc &source, KeyspaceID kid, std::size_t autoupdate_threshold = 10000)
        :AggregatorView<_ValueDef>(source.get_db(), kid, Direction::forward, {}, false)
        ,_source(source)
        ,_dirty_max(autoupdate_threshold)
        ,_tcontrol(*this) {

        if (get_revision() != revision) {
            rebuild();
        } else {
            load_dirty();
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

    using TransactionObserver = std::function<void(Batch &b, const AggregatedKey& key, const ValueType &value, DocID docid, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }

    template<bool deleting>
    class Emit {
    public:
        static constexpr bool erase = deleting;

        Emit(Aggregator &owner, Batch &b)
            :_owner(owner), _b(b) {}

        void operator()(const Row &key, const Row &value) {put(key, value);}

    protected:
        Aggregator &_owner;
        Batch &_b;


        void put(const Row &key, const Row &value) {
            RawKey kk = Database::get_private_area_key(_owner._kid, key);
            auto &buffer = _b.get_buffer();
            auto buff_iter = std::back_inserter(buffer);
            RowDocument::to_binary(value, buff_iter);
            std::lock_guard lk(_owner._dirty_lock);
            _b.Put(kk, buffer); //backup dirty key in leveldb
            //but we prefer to store dirty key in a map
            _owner._dirty_keys.emplace(std::move(kk), std::move(buffer));
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
        this->do_update(false);
    }

    bool try_update() {
        if (!_source.try_update()) return false;
        return this->do_update(true);
    }

    ///Synchronizes current thread with finishing of background tasks
    void join() {
        ///just acquire lock, this blocks until background thread ends
        std::unique_lock lk(_dirty_lock);
        if (_aggr_running) {
            std::condition_variable cv;
            _awaiters.push_back([&]{
                cv.notify_all();
            });
            cv.wait(lk);
        }
    }

protected:

    class TaskControl: public AbstractBatchNotificationListener {
    public:
        TaskControl(Aggregator &owner):_owner(owner) {}
        TaskControl(const TaskControl &owner) = delete;
        TaskControl &operator=(const TaskControl &owner) = delete;

        virtual void after_commit(std::size_t) noexcept override {
            _owner.after_commit();
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
    using DirtyMap = std::unordered_map<RawKey, Batch::BufferType, std::hash<std::string_view> >;
    DirtyMap _dirty_keys;
    std::mutex _dirty_lock;
    std::size_t _dirty_max;
    TaskControl _tcontrol;
    bool _aggr_running = false;
    std::vector<std::function<void()> > _awaiters;

    //load dirty keys from leveldb to internal memory map
    void load_dirty() {
        RecordSetBase rs(this->_db->make_iterator({}, true), {
                Database::get_private_area_key(this->_kid),
                Database::get_private_area_key(this->_kid+1),
                FirstRecord::excluded,
                LastRecord::excluded
        });
        while (!rs.empty()) {
          std::string_view keydata = rs.raw_key();
          std::string_view valuedata = rs.raw_value();

          RawKey key((RowView(keydata)));
          Batch::BufferType value(valuedata);
          key.mutable_buffer();

          _dirty_keys.emplace(std::move(key), std::move(value));
          rs.next();
      }
    }

    void after_commit() {
        std::unique_lock lk(_dirty_lock);
        if (_dirty_keys.size() >= _dirty_max && !_aggr_running) {
            _aggr_running = true;
            DirtyMap kmap = std::move(_dirty_keys);
            lk.unlock();
            std::thread thr([this, kmap = std::move(kmap)]() mutable {
                run_aggregation_from_thread(kmap);
            });
            thr.detach();
        }
    }


    void run_aggregation_from_thread(DirtyMap &dirty) {
        std::unique_lock<std::mutex> lk(_dirty_lock, std::defer_lock);
        do {
            run_aggregation(dirty, lk);
            if (_dirty_keys.size() < _dirty_max) break;
            dirty.clear();
            std::swap(dirty, _dirty_keys);
            lk.unlock();
        } while (true);
        _aggr_running = false;
        auto awt = std::move(_awaiters);
        lk.unlock();
        for (auto &x: awt) x();


    }

    void run_aggregation(const DirtyMap &dirty, std::unique_lock<std::mutex> &lk) {
        Batch b;
        for (const auto &[key,value]: dirty) {
            auto [nkrow] = key.get<RowView>();
            RawKey nk(nkrow);
            AggregatedResult<ValueType> r = aggregatorFn(_source, Key(Blob(value)));
            if (r._has_value) {
                auto &buff = b.get_buffer();
                _ValueDef::to_binary(r._value, std::back_inserter(buff));
                b.Put(nk, buff);
                notify_tx_observers(b, nk, r._value, false);
            } else if (_tx_observers.empty()) {
                b.Delete(nk);
            } else {
                auto prev = this->find(nk);
                if (prev.has_value()) {
                    b.Delete(nk);
                    notify_tx_observers(b, nk, *prev, true);
                }
            }
            this->_db->commit_batch(b);

        }
        lk.lock();
        for (const auto &[key,value]: dirty) {
            auto iter = _dirty_keys.find(key);
            if (iter == _dirty_keys.end()) {
                b.Delete(key);
            }
        }
        this->_db->commit_batch(b);
    }


    bool do_update(bool non_block) {
        std::unique_lock lk(_dirty_lock);
        while (_aggr_running) {
            if (non_block) return false;
            std::condition_variable cv;
            _awaiters.push_back([&]{
                cv.notify_all();
            });
            cv.wait(lk);
        }
        DirtyMap dmap (std::move(_dirty_keys));
        if (!dmap.empty()) {
            _aggr_running = true;
            lk.unlock();
            run_aggregation(dmap, lk);
            _aggr_running = false;
            auto awt = std::move(_awaiters);
            lk.unlock();
            for (auto &x: awt) x();
        }
        return true;
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
             if constexpr(std::invocable<_KeyReduceFn, Emit<false>, Key>) {
                 call_emit(erase, b, k);
             }else if constexpr(std::invocable<_KeyReduceFn, Emit<false>, Key, const DocType &>) {
                 call_emit(erase, b, k, value);
             }else if constexpr(std::invocable<_KeyReduceFn, Emit<false>, Key, const DocType &, DocID>) {
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
        this->_db->clear_table(this->_kid, true);
        this->_source.rescan_for(make_observer());
        update_revision();
    }

    void notify_tx_observers(Batch &b, const AggregatedKey &key, const ValueType &value, bool erase) {
        for (const auto &f: _tx_observers) {
            f(b, key, value, 0, erase);
        }
    }

};

template<typename ... Args>
struct ReduceKey {
    static constexpr int revision = 1;
    template<typename Emit, typename Val>
    void operator()(Emit emit, Key key, const Val &) const {
        auto kk = key.get<Args...>();
        emit(kk, kk);
    }
};


}




#endif /* SRC_DOCDB_AGGREGATOR_H_ */
