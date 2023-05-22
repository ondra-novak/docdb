/*
 * map.h
 *
 *  Created on: 18. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_MAP_H_
#define SRC_DOCDB_MAP_H_

#include "database.h"

namespace docdb {

template<DocumentDef _ValueDef>
class MapView: public ViewBase {
public:

    using ViewBase::ViewBase;


    using ValueType = typename _ValueDef::Type;


    MapView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return MapView( _kid, _dir, _db->make_snapshot());
    }

    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    Document<_ValueDef> get(Key &key) const {
        key.change_kid(_kid);
        return _db->get_as_document< Document<_ValueDef> >(key, _snap);
    }
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    Document<_ValueDef> get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    Document<_ValueDef> operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    Document<_ValueDef> operator[](Key &&key) const {return get(key);}


    using Iterator = GenIterator<_ValueDef>;

    Iterator scan(Direction dir = Direction::normal) {
        if (isForward(_dir)) {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return Iterator(_db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key,dir);}
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return Iterator(_db->make_iterator(false,_snap),{
                    key,RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_db->make_iterator(false,_snap),{
                    key,RawKey(_kid),
                    FirstRecord::included, LastRecord::included
            });
        }
    }
    Iterator scan_prefix(Key &&key, Direction dir = Direction::normal) {return scan_prefix(key,dir);}
    Iterator scan_prefix(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        RawKey end = key.prefix_end();
        if (isForward(_dir)) {
            return Iterator(_db->make_iterator(false,_snap),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_db->make_iterator(false,_snap),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded) {
        from.change_kid(_kid);
        to.change_kid(_kid);
        return Iterator(_db->make_iterator(false,_snap),{
                from,to,
                FirstRecord::included, LastRecord::excluded
        });
    }

    ///Observes changes of keys in the index;
    using UpdateObserver = SimpleFunction<bool, Batch &, const BasicRowView &>;

    void rescan_for(const UpdateObserver &observer) {

        Batch b;
        auto iter = this->scan();
        while (iter.next()) {
            if (b.is_big()) {
                this->_db->commit_batch(b);
            }
            auto k = iter.raw_key();
            k = k.substr(sizeof(KeyspaceID), k.size() - sizeof(KeyspaceID) - sizeof(DocID));
            if (!observer(b, k)) break;
        }
        this->_db->commit_batch(b);
    }

};

template<DocumentDef _ValueDef>
class Map: public MapView<_ValueDef> {

    using MapView<_ValueDef>::MapView;
    using ValueType = typename _ValueDef::Type;

    ///Observes changes of keys in the index;
    using UpdateObserver = typename MapView<_ValueDef>::UpdateObserver;


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


    void put(Key &&key, const ValueType &val) {put(key, val);}
    void put(Key &key, const ValueType &val) {
        Batch b;
        put(b,key,val);
        this->_db->commit_batch(b);
    }

    void put(Batch &b, Key &&key, const ValueType &val) {put(b,key,val);}
    void put(Batch &b, Key &key, const ValueType &val) {
        auto buffer = b.get_buffer();
        key.change_kid(this->_kid);
        _ValueDef::to_binary(val, std::back_inserter(buffer));
        b.Put(key, buffer);
        _observers.call(b, std::string_view(key).substr(sizeof(KeyspaceID)));
    }

    void erase(Key &&key) {erase(key);}
    void erase(Key &key) {
        Batch b;
        erase(b, key);
        this->_db->commit_batch(b);
    }
    void erase(Batch &b, Key &&key) {erase(key);}
    void erase(Batch &b, Key &key) {
        b.Delete(key);
        _observers.call(b, std::string_view(key).substr(sizeof(KeyspaceID)));
    }


protected:
    ObserverList<UpdateObserver> _observers;



};


}





#endif /* SRC_DOCDB_MAP_H_ */
