#pragma once
#ifndef SRC_DOCDB_VIEWBASE_H_
#define SRC_DOCDB_VIEWBASE_H_
#include "exceptions.h"
#include "database.h"

namespace docdb {


///Found record
/**
 * Anything found using directly calling find() function on the ViewBase,
 * you receive this object as result. This object works similar as a std::optional
 * or as a smart pointer. It holds the found record (document, value) with
 * necessary memory allocated for it. The object can be converted to bool
 * to test, whether it contains anything (because if nothing found, you receive
 * empty instance). Then you can access the content by dereference * or
 * by operator ->.
 *
 * The object can be moved, but not copied. It can be used ast std::unique_ptr.
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

    FoundRecord(FoundRecord &&other):_found(other._found), _buff(std::move(other._buff)) {
        if (other._inited) {
            ::new(&_storage) ValueType(std::move(other._storage));
            _inited = true;
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
    ///Tests whether value has been set
    bool has_value() const {
        return _found;
    }

    operator bool() const {return has_value();}


    ///Deserialized the document
    const ValueType &operator*() const {
        if (!_found) [[unlikely]] throw RecordNotFound();
        return _storage;
    }


    const ValueType *operator->() const {
        if (!_found) [[unlikely]] throw RecordNotFound();
        return &_storage;
    }

    const Buffer &get_serialized() const {
        if (!_found) [[unlikely]] throw RecordNotFound();
        return _buff;
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


    FoundRecord<_DocDef> find(Key &&key) const {
        return find(key);
    }
    FoundRecord<_DocDef> find(Key &key) const {
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

namespace _details {

    template<typename T, typename ... Args>
    PSnapshot create_snapshot_object(const T &view, const Args & ...) {
        return view.get_db()->make_snapshot();
    }
}

///create snapshot for multiple views
/**
 * @param views multiple views, indexes or storages to make snapshot
 * @return tuple carrying current snapshot (read only views) for given views, indexes and storages
 */
template<typename ... Args>
auto make_snapshot(const Args &... views) {
    PSnapshot snap = _details::create_snapshot_object(views...);
    return std::make_tuple(views.get_snapshot(snap,false)...);
}

///create snapshot for multiple views. Read data will not be cached
/**
 * @param views multiple views, indexes or storages to make snapshot
 * @return tuple carrying current snapshot (read only views) for given views, indexes and storages
 */
template<typename ... Args>
auto make_snapshot_nocache(const Args &... views) {
    PSnapshot snap = _details::create_snapshot_object(views...);
    return std::make_tuple(views.get_snapshot(snap,true)...);
}


}




#endif /* SRC_DOCDB_VIEWBASE_H_ */
