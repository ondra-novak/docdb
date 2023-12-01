#pragma once
#ifndef SRC_DOCDB_MAP_H_
#define SRC_DOCDB_MAP_H_

#include "database.h"
#include "index_view.h"

namespace docdb {


template<DocumentDef _ValueDef>
using MapView = IndexViewGen<_ValueDef, IndexViewBaseEmpty<_ValueDef> >;


///Map is simple key-value storage with support of transaction observers
/**
 * Map can be used instead the Storage if you want to manage a primary key. Note that map
 * cannot be indexed using Indexer. However, Map can be aggregated by aggregator
 *
 * @tparam _ValueDef Document definition of value
 */
template<DocumentDef _ValueDef>
class Map : public MapView<_ValueDef> {
public:

    static constexpr Purpose _purpose = Purpose::map;

    using ValueType = typename _ValueDef::Type;


    Map(const PDatabase &db, std::string_view name)
        :Map(db, db->open_table(name, _purpose)) {}

    Map(const PDatabase &db, KeyspaceID kid)
        :MapView< _ValueDef>(db, kid, Direction::forward, {}, false)
    {}


    using TransactionObserver = std::function<void(Batch &b, const Key& key, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }


    void put(Key &&key, const ValueType &val) {return put(key, val);}
    void put(Key &key, const ValueType &val) {
        auto b = this->_db->begin_batch();
        store(b, key, val);
        b.commit();
    }

    void put(Batch &b, Key &&key, const ValueType &val) {return put(b, key, val);}
    void put(Batch &b, Key &key, const ValueType &val) {
        store(b, key, val);
    }

    void erase(Key &&key) {return erase(key);}
    void erase(Key &key) {
        auto b = this->_db->begin_batch();
        clear(b, key);
        b.commit();
    }

    void erase(Batch &b, Key &&key) {return erase(b, key);}
    void erase(Batch &b, Key &key) {
        clear(b, key);
    }

protected:

    std::vector<TransactionObserver> _tx_observers;

    void store(Batch &b, Key &key, const ValueType &val) {
        auto &buff = b.get_buffer();
        _ValueDef::to_binary(val, std::back_inserter(buff));
        b.Put(key.set_kid(this->_kid), buff);
        for (const auto &c: _tx_observers) c(b,key, false);
    }

    void clear(Batch &b, Key &key) {
        for (const auto &c: _tx_observers) c(b,key,true);
        b.Delete(key.set_kid(this->_kid));
    }


};

}



#endif /* SRC_DOCDB_MAP_H_ */
