#pragma once
#ifndef SRC_DOCDB_VIEWBASE_H_
#define SRC_DOCDB_VIEWBASE_H_
#include "database.h"

namespace docdb {


///Retrieved value
/**
 * Value cannot be retrieved directly, you still need to test whether
 * the key realy exists in the database. You also need to keep internal
 * serialized data as the some document still may reffer to serialized
 * area. This helps to speed-up deserialization, but the document cannot
 * be separated from its serialized version. So to access the document
 * you need to keep this structure.
 *
 * @tparam _ValueDef Defines format of value/document
 * @tparam Buffer type of buffer
 */
template<DocumentDef _ValueDef, typename Buffer = std::string>
class FoundRecord {
public:

    using ValueType = typename _ValueDef::Type;

    ///Construct object using function
    /**
     * This is adapted to leveldb's Get operation
     *
     * @param rt function receives buffer, and returns true if the buffer
     * was filled with serialized data, or false, if not
     */
    template<typename Rtv>
    DOCDB_CXX20_REQUIRES(std::same_as<decltype(std::declval<Rtv>()(std::declval<Buffer &>())), bool>)
    FoundRecord(Rtv &&rt) {
        _found = rt(_buff);
        init_if_found();

    }

    ///Creates empty value
    FoundRecord():_found(false) {}

    ///Serializes document into buffer
    FoundRecord(const ValueType &val):_found(true),_inited(true),_storage(val) {
        _ValueDef::to_binary(val, std::back_inserter(_buff));
    }
    ///Serializes document into buffer
    FoundRecord(ValueType &&val):_found(true),_inited(true),_storage(std::move(val)) {
        _ValueDef::to_binary(val, std::back_inserter(_buff));
    }

    FoundRecord(const FoundRecord &other):_found(other._found), _buff(other._buff) {
        init_if_found();
    }
    FoundRecord(FoundRecord &&other):_found(other._found), _buff(std::move(other._buff)) {
        if (other._inited) {
            ::new(&_storage) ValueType(std::move(other._storage));
            _inited = true;
        }
    }

    FoundRecord &operator=(const FoundRecord &other) {
        if (this != &other) {
            this->~FoundRecord();
            ::new(this) FoundRecord(other);
        }
    }
    FoundRecord &operator=(FoundRecord &&other) {
        if (this != &other) {
            this->~FoundRecord();
            ::new(this) FoundRecord(std::move(other));
        }
        return *this;
    }

    ~FoundRecord() {
        if (_inited) _storage.~ValueType();
    }

    ///Deserialized the document
    const ValueType &operator*() const {
        return _storage;
    }
    ///Tests whether value has been set
    bool has_value() const {
        return _found;
    }

    operator bool() const {return has_value();}

    const Buffer &get_serialized() const {return _buff;}

    const ValueType *operator->() const {
        return &_storage;
    }

protected:
    bool _found;
    mutable bool _inited =false;
    Buffer _buff;
    union {
        ValueType _storage;
    };

    void init_if_found() {
        if (_found) {
            ::new(&_storage) ValueType(_ValueDef::from_binary(_buff.begin(), _buff.end()));
            _inited =true;
        }
    }

};




template<DocumentDef _DocDef>
class ViewBase {
public:

    ViewBase(const PDatabase &db, KeyspaceID kid, Direction dir, const PSnapshot &snap, bool no_cache)
    :_db(db),_snap(snap),_kid(kid),_dir(dir),_no_cache(no_cache) {}

    const PDatabase& get_db() const {
        return _db;
    }

    Direction get_directory() const {
        return _dir;
    }

    KeyspaceID get_kid() const {
        return _kid;
    }

    const PSnapshot& get_snapshot() const {
        return _snap;
    }

    using DocType = typename _DocDef::Type;


    FoundRecord<_DocDef> find(Key &&key) {
        return find(key);
    }
    FoundRecord<_DocDef> find(Key &key) {
        key.change_kid(_kid);
        return _db->get_as_document<FoundRecord<_DocDef> >(key, _snap);

    }


protected:

    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
    Direction _dir;
    bool _no_cache;


};


}




#endif /* SRC_DOCDB_VIEWBASE_H_ */
