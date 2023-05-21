/*
 * aggregator.h
 *
 *  Created on: 19. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_AGGREGATE_H_
#define SRC_DOCDB_AGGREGATE_H_

#include "concepts.h"

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

template<typename Index, DocumentDef _ValueDef>
class AggregateIndex: public MapView<_ValueDef> {
public:

    using ValueType = typename _ValueDef::Type;
    using IndexIterator = typename Index::Iterator;
    using Iterator = typename MapView<_ValueDef>::Iterator;

    ///Function is using to reduce key
    using AggregateFn = std::function<Value<_ValueDef>(IndexIterator &iter)>;
    using TempMap = std::unordered_map<RawKey, ValueType>;
    using UpdateObserver =  std::function<bool(Batch &, const BasicRowView &)>;
    using RowInfo = typename MapView<_ValueDef>::RowInfo;
    using Super = MapView<_ValueDef>;

    class Emit {
    public:
        Emit(ObserverList<UpdateObserver> &observers,
            Batch &batch,
            KeyspaceID kid,
            KeyspaceID srchkid)
            :_observers(observers)
            ,_batch(batch)
            ,_kid(kid),_srchkid(srchkid) {}

        void operator()(const BasicRow &aggr_key,const BasicRow &search_key) {
            put(aggr_key,search_key);
        }

    protected:
        void put(const BasicRow &aggr_key,const BasicRow &search_key) {
            std::uint32_t rev = static_cast<std::uint32_t>(_batch.get_revision());
            RawKey ak = Database::get_private_area_key(_kid, rev, aggr_key);
            _batch.Put(ak, search_key);
            _observers.call(_batch, aggr_key);
        }

        ObserverList<UpdateObserver> &_observers;
        Batch &_batch;
        KeyspaceID _kid;
        KeyspaceID _srchkid;
    };

    using KeyMapping = std::function<void(Emit &, const BasicRowView &)>;


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
                start, end, FirstRecord::included, FirstRecord::excluded
        });
        Batch b;
        Buffer<char, 128> buffer;
        while (iter.next()) {
            BasicRowView k(iter.raw_key());
            auto [a1, a2, a3, kk] = k.get<KeyspaceID, KeyspaceID, std::uint32_t, Blob>();
            Key srchkey(Blob(iter.raw_value()));
            IndexIterator iter = _index.scan_prefix(srchkey);
            Value<_ValueDef> vdef = _aggr_fn(iter);
            if (vdef.has_value()) {
                b.Put(RawKey(this->_kid,kk), vdef.get_serialized());
                buffer.clear();
            } else {
                b.Delete(RawKey(this->_kid,kk));
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
    RowInfo get(Key &key) const {
        refresh();
        return Super::get(key);
    }
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    RowInfo get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    RowInfo operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    RowInfo operator[](Key &&key) const {return get(key);}



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
        this->_index.rescan_for([&](Batch &b, const BasicRowView &key){
            return _indexer(b, key);
        });
        update_revision();
    }

protected:
    Index &_index;
    AggregateFn _aggr_fn;
    int _rev;
    std::size_t observer_id = 0;
    typename Index::UpdateObserver _indexer;
    ObserverList<UpdateObserver> _observers;
    std::atomic<bool> _change = true;
    std::mutex _mx;

    bool check_revision() {
        std::string v;
        if (!this->_db->get(RawKey(this->_kid), v)) return false;
        BasicRowView row(v);
        auto [cur_rev] = row.get<int>();
        return (cur_rev == _rev);
    }
    void update_revision() {
        Batch b;
        b.Put(RawKey(this->_kid), BasicRow(_rev));
        this->_db->commit_batch(b);
    }

    void set_flag(bool commit) {
        if (commit) _change.store(true, std::memory_order_relaxed);
    }

    using Hook = typename Batch::Hook;

    template<typename Fn>
    void connect_indexer(Fn &&fn) {
        _indexer = [this, fn = std::move(fn)](Batch &b, const BasicRowView &key){
            Emit emit(_observers, b, this->_kid, this->_index.get_kid());
            fn(emit, key);
            b.add_hook(Hook::member_fn<&AggregateIndex<Index,_ValueDef>::set_flag>(this));
            return true;
        };
        observer_id = this->_index.register_observer([&](Batch &b, const BasicRowView &key){
           return _indexer(b, key);
        });

    }


};

namespace _details {
    template<typename T, typename ... Args>
    void fill_key(Key &out, const BasicRowView &keys) {
        auto [item, rest] = keys.get<T, Blob>();
        out.append(item);
        fill_key<Args...>(out, std::string_view(rest));
    }
    void fill_key(Key &out, const BasicRowView &keys) {
        //emptys
    }
}

template<typename ... Items>
struct KeyReduce {
public:
    Key operator()(const BasicRowView &keys) {
        Key out;
        _details::fill_key(out, keys);
        return out;
    }
};

}



#endif /* SRC_DOCDB_AGGREGATE_H_ */
