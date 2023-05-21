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

    ///Information retrieved from database
    class RowInfo {
    public:
        ///Retrieve the key (it is always in BasicRow format)
        BasicRowView key() const {return _key;}
        ///contains true, if row exists (was found)
        bool exists;
        ///the value of the row is available (non-null)
        bool available;
        ///Retrieve the value (parse the value);
        ValueType value() const {
            return _ValueDef::from_binary(_bin_data.data(), _bin_data.data()+_bin_data.size());
        }
        ///Converts to document
        operator ValueType() const {return value();}

        ///Returns true if document is available
        operator bool() const {return available;}

        ///Returns true if document is available
        bool operator==(std::nullptr_t) const {return available;}

        ///Returns true if document exists (but can be deleted)
        bool has_value() const {return exists;}

    protected:
        std::string_view _key;
        std::string _bin_data;
        RowInfo(const PDatabase &db, const PSnapshot &snap, const RawKey &key)
            :_key(std::string_view(key).substr(sizeof(KeyspaceID))) {
            exists = db->get(key, _bin_data, snap);
            available = !_bin_data.empty();
        }
        friend class MapView;

    };


    MapView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return MapView( _kid, _dir, _db->make_snapshot());
    }

    ///Retrieves exact row
    /**
     * @param key key to search
     * @return
     */
    RowInfo get(Key &key) const {
        key.change_kid(_kid);
        return RowInfo(_db, _snap, key);
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
};

template<DocumentDef _ValueDef>
class Map: public MapView<_ValueDef> {

    using MapView<_ValueDef>::MapView;
    using ValueType = typename _ValueDef::Type;

    ///Observes changes of keys in the index;
    using UpdateObserver = std::function<bool(Batch &b, const BasicRowView  &)>;


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
