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
    using AggregateFn = std::function<std::optional<ValueType>(IndexIterator &iter)>;


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
        return aggr_fn(iter);
    }


protected:
    Index &_index;
    AggregateFn aggr_fn;
};

template<typename Index, DocumentDef _ValueDef>
class AggregateIndex: public MapView<_ValueDef>, public Aggregator<Index, _ValueDef> {
public:

    using ValueType = typename _ValueDef::Type;
    using IndexIterator = typename Index::Iterator;

    ///Function is using to reduce key
    using KeyReduceFn = std::function<Key(const BasicRowView &)>;
    using AggregateFn = std::function<ValueType(IndexIterator &iter)>;
    using TempMap = std::unordered_map<RawKey, ValueType>;

    AggregateIndex(Index &index, std::string_view name,
            KeyReduceFn &&key_reduce, AggregateFn &&aggr_fn,
            Direction dir = Direction::forward,
            const PSnapshot &snap = {})
        :MapView<_ValueDef>(index.get_db(), name, dir, snap)
        ,Aggregator<Index, _ValueDef>(index, std::move(aggr_fn)) {
        create_handler(std::move(key_reduce));
    }


    AggregateIndex(Index &index, KeyspaceID kid,
            KeyReduceFn &&key_reduce, AggregateFn &&aggr_fn,
            Direction dir = Direction::forward,
            const PSnapshot &snap = {})
        :MapView<_ValueDef>(index.get_db(), kid, dir, snap)
         ,Aggregator<Index, _ValueDef>(index, std::move(aggr_fn)) {
        create_handler(std::move(key_reduce));
    }


    std::optional<ValueType> aggregate_and_store(Batch &b, Key &&key) {
        return aggregate_and_store(key);
    }
    std::optional<ValueType> aggregate_and_store(Batch &b, Key &key) {
        std::optional<ValueType> res = this->aggregate(key);
        key.change_kid(this->_kid);
        if (res.has_value()) {
            auto &buffer = b.get_buffer();
            _ValueDef::to_binary(res, std::back_inserter(buffer));
            b.Put(key, buffer);
        } else {
            b.Delete(key);
        }
        //TODO observers
        return res;
    }

    std::optional<ValueType> aggregate_and_store(Key &&key) {
        return aggregate_and_store(key);
    }
    std::optional<ValueType> aggregate_and_store(Key &key) {
        Batch b;
        std::optional<ValueType> r = aggregate_and_store(b,key);
        this->_db->commit_batch(b);
        return r;
    }

    std::optional<ValueType> get(Key &key) {
        key.change_kid(this->_kid);
        std::string tmp;
        if (this->_db->get(key, this->_snap)) {
            if (tmp.empty()) {
                if (this->_snap) return this->aggregate(key);
                return this->aggregate_and_store(key);
            } else {
                return _ValueDef::from_binary(tmp.begin(), tmp.end());
            }
        } else {
            return {};
        }

    }

    using Iterator = GenIterator<_ValueDef>;


protected:

    struct Shared {
        Batch b;
        std::string_view aggregated;;
    };

    class Flt: public Iterator::AbstractFilter {
    public:
        Flt(AggregateIndex &owner):_owner(owner) {}

        virtual bool filter(const Iterator &iter) {
            auto str = iter.raw_value();
            _val.reset();
            if (!str.empty()) return true;
            Key k(iter.key());
            if (_owner._snap) {
                _val = _owner.aggregate(k);
            } else {
                _val = _owner.aggregate_and_store(k);
            }
            return _val.has_value();
        }
        virtual ValueType transform(const std::string_view &data) {
            if (_val.has_value()) return *_val;
            else {
                return _ValueDef::from_binary(data.begin(), data.end());
            }
        }

    protected:
        AggregateIndex &_owner;
        Batch b;
        std::optional<ValueType> _val;

    };

    typename Iterator::Config make_cfg(typename Iterator::Config &&src) {

        src.filter = typename Iterator::PFilter(new Flt(*this));
        return src;
    }

    void create_handler(KeyReduceFn &&fn) {

        auto handler = [kid = this->_kid, fn = std::move(fn)](
                        Batch &b, const BasicRowView  &key) {
            b.Put(fn(key), {});
        };

        auto phandler = std::make_shared<decltype(handler)>(std::move(handler));
        this->_index.register_observer([me = std::weak_ptr(phandler)]
                                  (Batch &b, const BasicRowView &k){
            auto lk = me.lock();
            if (lk) {
                (*lk)(b, k);
                return true;
            } else {
                return false;
            }
        });
    }

};


}



#endif /* SRC_DOCDB_AGGREGATE_H_ */
