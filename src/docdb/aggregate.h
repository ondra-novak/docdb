/*
 * aggregator.h
 *
 *  Created on: 19. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_AGGREGATE_H_
#define SRC_DOCDB_AGGREGATE_H_

#include "concepts.h"
#include "map_concept.h"
#include "map.h"
namespace docdb {

///Simple aggregator
/**
 * Helps to aggregate values in index.
 *
 * @tparam Index Source index
 * @tparam _ValueDef Definition of aggregated value
 */
template<typename Index, DocumentDef _ValueDef>
class Aggregator {
public:
    using ValueType = typename _ValueDef::Type;
    using IndexIterator = typename Index::Iterator;



    ///Aggregation function
    /**
     * @param iter iterator, contains selected values to aggregate into one value
     * @return aggregated value
     * @note function must also accept an empty range
     */
    using AggregateFn = std::function<std::optional<ValueType>(Index &index, Key &key)>;


    ///Construct simple aggregator
    /**
     * @param index reference to index
     * @param aggr_fn aggregation function
     */
    Aggregator(Index &index, AggregateFn &&aggr_fn)
        :_index(index), aggr_fn(std::move(aggr_fn)) {}

    Aggregator(Aggregator &&) = default;
    Aggregator(const Aggregator &) = delete;
    Aggregator &operator=(const Aggregator &) = delete;


    ///Performs aggregation
    /**
     * @param key key to aggregate
     * @return aggregated value
     */
    ValueType aggregate(Key &&key) {
        return aggregate(key);
    }
    ValueType aggregate(Key &key) {
        IndexIterator iter = _index.scan_prefix(key);
        return aggr_fn(index, iter);
    }


protected:
    Index &_index;
    AggregateFn aggr_fn;
};

template<MapType Index, DocumentDef _ValueDef>
class AggregateIndex: public MapView<_ValueDef> {
public:

    using ValueType = typename _ValueDef::Type;
    using IndexIterator = typename Index::Iterator;
    using Iterator = typename MapView<_ValueDef>::Iterator;

    ///Function is using to reduce key
    using TempMap = std::unordered_map<RawKey, ValueType>;
    using UpdateObserver =  typename MapView<_ValueDef>::UpdateObserver;
    using Super = MapView<_ValueDef>;

    class KeyEmit {
    public:
        KeyEmit(ObserverList<UpdateObserver> &observers,
            Batch &batch,
            KeyspaceID kid,
            KeyspaceID srchkid)
            :_observers(observers)
            ,_batch(batch)
            ,_kid(kid),_srchkid(srchkid) {}

        void operator()(const Key &aggr_key,const Key &search_key) {
            put(aggr_key,search_key);
        }

    protected:
        void put(const Key &aggr_key,const Key &search_key) {
            std::uint32_t rev = static_cast<std::uint32_t>(_batch.get_revision());
            RawKey ak = Database::get_private_area_key(_kid, rev, aggr_key);
            _batch.Put(ak, search_key);
            _observers.call(_batch, Key(RowView(aggr_key)));
        }

        ObserverList<UpdateObserver> &_observers;
        Batch &_batch;
        KeyspaceID _kid;
        KeyspaceID _srchkid;
    };

    using KeyMapping = std::function<void(KeyEmit &, const Key &)>;

    class AggrEmit {
    public:
        AggrEmit(Batch &b, const RawKey &k, Buffer<char, 128> &buffer)
            :_buffer(buffer)
            ,_b(b)
            ,_k(k) {}

        void operator()(const ValueType &value) {
            _ValueDef::to_binary(value, std::back_inserter(_buffer));
            _b.Put(_k, _buffer);
            _buffer.clear();
            _delit = false;
        }
        ~AggrEmit() {
            if (_delit) _b.Delete(_k);
        }
    protected:
        Buffer<char, 128> &_buffer;
        Batch &_b;
        const RawKey &_k;
        bool _delit = true;
    };


    using AggregateFn = std::function<void(AggrEmit &, IndexIterator &)>;

    ///Construct aggregated index
    /**
     * @param index source index, which is subject of aggregation
     * @param name name of this index
     * @param revision index revision
     * @param key_mapping Function which maps source key to two keys. The first key
     * is key under which the aggregation will be stored. Second key is
     * key which will be used to lookup for values
     * @param aggr_fn
     * @param dir
     * @param snap
     */
    AggregateIndex(Index &index, std::string_view name,
            int revision,
            KeyMapping &&key_mapping, AggregateFn &&aggr_fn,
            Direction dir = Direction::forward,
            const PSnapshot &snap = {})
        :MapView<_ValueDef>(index.get_db(), name, dir, snap)
        ,_index(index)
        ,_aggr_fn(std::move(aggr_fn))
        ,_rev(revision)
    {
        connect_indexer(std::move(key_mapping));
        if (!check_revision()) reindex();
    }


    AggregateIndex(Index &index, KeyspaceID kid,
            KeyMapping &&key_mapping, AggregateFn &&aggr_fn,
            int revision,
            Direction dir = Direction::forward,
            const PSnapshot &snap = {})
        :MapView<_ValueDef>(index.get_db(), kid, dir, snap)
         ,_index(index)
         ,_aggr_fn(std::move(aggr_fn))
         ,_rev(revision)
    {
        connect_indexer(std::move(key_mapping));
        if (!check_revision()) reindex();
    }



    void refresh() {
        std::unique_lock _(_mx);
        if (!_change.exchange(false, std::memory_order_relaxed)) return;

        RawKey start = Database::get_private_area_key(this->_kid);
        RawKey end = start.prefix_end();
        GenIterator<StringDocument> iter(this->_db->make_iterator(false),{
                start, end, FirstRecord::excluded, FirstRecord::excluded
        });
        Batch b;
        Buffer<char, 128> buffer;
        while (iter.next()) {
            Row k(RowView(iter.raw_key()));
            auto [a1, a2, a3,kk] = k.get<KeyspaceID,KeyspaceID, std::uint32_t, Blob>();
            Key srchkey(RowView(iter.raw_value()));
            IndexIterator iter = _index.scan_prefix(srchkey);
            {
                RawKey rk(this->_kid,kk);
               AggrEmit emit(b,rk , buffer);
               _aggr_fn(emit, iter);
            }
            b.Delete(k);
            this->_db->commit_batch(b);
        }

    }

    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    auto get(Key &key) const {
        refresh();
        return Super::get(key);
    }
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    auto get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    auto operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    auto operator[](Key &&key) const {return get(key);}



    Iterator scan(Direction dir = Direction::normal) {
        refresh();
        return Super::scan(dir);
    }

    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key,dir);}
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        refresh();
        return Super::scan(key,dir);
    }
    Iterator scan_prefix(Key &&key, Direction dir = Direction::normal) {return scan_prefix(key,dir);}
    Iterator scan_prefix(Key &key, Direction dir = Direction::normal) {
        refresh();
        return Super::scan_prefix(key, dir);
    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded) {
        refresh();
        return Super::scan_prefix(from, to, last_record);
    }

    void reindex() {
        this->_db->clear_table(this->_kid, false);
        this->_db->clear_table(this->_kid, true);
        this->_index.rescan_for(_aggregator->create_observer());
        update_revision();
    }

    ///Register new observer
    /**
     * @param observer new observer to register (index)
     * @return id id of observer
     */
    std::size_t register_observer(UpdateObserver observer) {
        return _observers.register_observer(std::move(observer));
    }
    ///Unregister observer (by id)
    void unregister_observer(std::size_t id) {
        _observers.unregister_observer(id);
    }



protected:
    using IndexObserver = typename Index::UpdateObserver;
    class Aggregator {
    public:
        virtual IndexObserver create_observer() = 0;
        virtual ~Aggregator() = default;
    };

    Index &_index;
    AggregateFn _aggr_fn;
    int _rev;
    std::size_t observer_id = 0;
    std::unique_ptr<Aggregator> _aggregator;
    //typename Index::UpdateObserver _indexer;
    ObserverList<UpdateObserver> _observers;
    std::atomic<bool> _change = true;
    std::mutex _mx;

    bool check_revision() {
        auto d=this->_db->template get_as_document<Document<RowDocument> >(
                                   Database::get_private_area_key(this->_kid));
        if (d) {
             auto [cur_rev] = (*d).template get<int>();
             return (cur_rev == _rev);
        }
        return false;
    }
    void update_revision() {
        Batch b;
        b.Put(Database::get_private_area_key(this->_kid), Row(_rev));
        this->_db->commit_batch(b);
    }



    void set_flag(bool commit) {
        if (commit) _change.store(true, std::memory_order_relaxed);
    }

    using Hook = typename Batch::Hook;

    template<typename Fn>
    class Aggregator_Fn: public Aggregator {
    public:
        Aggregator_Fn(AggregateIndex &owner, Fn &&fn):_owner(owner),_fn(std::forward<Fn>(fn)) {}

        bool run(Batch &b, const Key &key) {
            KeyEmit emit(_owner._observers, b, _owner._kid, _owner._index.get_kid());
            _fn(emit, key);
            b.add_hook(Hook::member_fn<&AggregateIndex::set_flag>(&_owner));
            return true;
        }
        virtual IndexObserver create_observer() override {
            return IndexObserver::template member_fn<&Aggregator_Fn::run>(this);
        }

    protected:
        AggregateIndex &_owner;
        Fn _fn;
    };


    template<typename Fn>
    void connect_indexer(Fn &&fn) {
        _aggregator = std::make_unique<Aggregator_Fn<Fn> >(*this, std::forward<Fn>(fn));
        observer_id = this->_index.register_observer(_aggregator->create_observer());
    }


};

namespace _details {
    template<typename T, typename ... Args>
    void fill_key(Key &out, const Row &key) {
        auto [item, rest] = key.get<T, Blob>();
        out.append(item);
        fill_key<Args...>(out, std::string_view(rest));
    }
    void fill_key(Key &out, const Row &key) {
        //empty
    }
}

template<typename ... Items>
struct KeyReduce {
public:
    template<typename Emit>
    void operator()(Emit &emit, const Key &key) {
        Key out;
        auto [row] = key.get<Row>();
        _details::fill_key(out, row);
        emit(out, out);
    }
};

}



#endif /* SRC_DOCDB_AGGREGATE_H_ */
