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


    using TransactionObserver = std::function<void(Batch &b, const Key& key, const ValueType &value, bool erase)>;

    void register_transaction_observer(TransactionObserver obs) {
        _tx_observers.push_back(std::move(obs));
    }


    void put(Key &&key, const ValueType &val) {return put(key, val);}
    void put(Key &key, const ValueType &val) {
        Batch b;
        store(b, key, val);
        this->_db->commit_batch(b);
    }

    void put(Batch &b, Key &&key, const ValueType &val) {return put(b, key, val);}
    void put(Batch &b, Key &key, const ValueType &val) {
        store(b, key, val);
    }

    void erase(Key &&key) {return erase(key);}
    void erase(Key &key) {
        Batch b;
        clear(b, key);
        this->_db->commit_batch(b);
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
        key.change_kid(this->_kid);
        b.Put(key, buff);
        for (const auto &c: _tx_observers) c(b,key,val, false);
    }

    void clear(Batch &b, Key &key) {
        key.change_kid(this->_kid);
        //no observers, so map deletion is way more easier
        if (_tx_observers.empty()) {
            b.Delete(key);
            return;
        }
        //we need to pickup value from the map to be send to the observer
        auto lkp = this->find(key);
        //if not found, nothing to delete
        if (lkp.has_value()) {
            //delete key
            b.Delete(key);
            //send to observers
            for (const auto &c: _tx_observers) c(b,key,*lkp, true);
        }
    }


};

}



#endif /* SRC_DOCDB_MAP_H_ */
