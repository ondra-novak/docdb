#pragma once
#ifndef SRC_DOCDB_INDEX_VIEW_H_
#define SRC_DOCDB_INDEX_VIEW_H_

#include "iterator.h"
#include "row.h"

namespace docdb {

enum class IndexType {
    ///unique index
    /**
     * Unique index.
     * It expects, that there is no duplicate keys, but
     * it cannot enforce this. In case of failure of this rule,
     * key is overwritten, so old document is dereferenced.
     *
     * This index is the simplest and the most efficient
     */
    unique,
    ///Unique index with enforcement
    /**
     * Checks, whether keys are really unique. In case of failure of this
     * rule, the transaction is rejected and exception is thrown
     *
     * This index can be slow, because it needs to search the database
     * for every incoming record
     */
    unique_enforced,

    ///Unique index with enforcement but expect single thread insert
    /**
     * This is little faster index in compare to unique_enforced, as it
     * expects that only one thread is inserting. Failing this requirement
     * can cause that duplicate key can be inserted during parallel insert (
     * as the conflicting key is still in transaction, not in the database
     * itself). This index type doesn't check for keys in transaction,
     * so the implementation is less complicated
     */
    unique_enforced_single_thread,

    ///Non-uniuqe, multivalue key
    /**
     * Duplicated keys are allowed for different documents (it
     * is not allowed to have duplicated key in the single document).
     *
     * This index type can handle the most cases of use
     *
     * This index requires to store document id as last item of the key. This
     * is handled automatically by the index function. However, because
     * of this, there can be difficulties with some operations. For example
     * retrieval function (get) needs whole key and document ID, otherwise
     * nothing is retrieved. Using a blob as key is undefined behavior.
     */
    multi,
};



class IndexViewBaseEmpty {
public:

    template<typename DocDef>
    using Iterator = GenIterator<DocDef>;

    template<typename DocDef>
    Iterator<DocDef> create_iterator(std::unique_ptr<leveldb::Iterator> &&iter, typename Iterator<DocDef>::Config &&config) {
        return Iterator<DocDef>(std::move(iter), std::move(config));
    }

};

template<typename _Storage, auto docid_parser>
class IndexViewBaseWithStorage {
public:

    IndexViewBaseWithStorage(_Storage &storage):_storage(storage) {}

    template<typename DocDef>
    class Iterator: public GenIterator<DocDef> {
    public:
        Iterator(_Storage &stor, std::unique_ptr<leveldb::Iterator> &&iter, typename GenIterator<DocDef>::Config &&config)
        :GenIterator<DocDef>(std::move(iter), std::move(config))
        ,_storage(stor) {}

        auto id() const {
            return docid_parser(*this);
        }
        auto doc() const {
            return _storage[id()];
        }

    protected:
        _Storage &_storage;
    };

    template<typename DocDef>
    Iterator<DocDef> create_iterator(std::unique_ptr<leveldb::Iterator> &&iter, typename GenIterator<DocDef>::Config &&config) {
        return Iterator<DocDef>(_storage, std::move(iter), std::move(config));
    }

protected:
    _Storage &_storage;
};

template<typename _Storage>
using IndexViewBaseWithStorage_IDinkey = IndexViewBaseWithStorage<_Storage,[](const auto &iter){
    std::string_view rawkey =iter.raw_key();
    auto docidstr = rawkey.substr(rawkey.length()-sizeof(DocID));
    auto [id] = Row::extract<DocID>(docidstr);
    return id;
}>;

template<typename _Storage>
using IndexViewBaseWithStorage_IDinvalue = IndexViewBaseWithStorage<_Storage,[](const auto &iter){
   auto [id] = Row::extract<DocID>(iter.raw_value());
   return id;
}>;

template<typename _ValueDef, typename IndexBase>
class IndexViewGen: public IndexBase {
public:

    using Iterator = typename IndexBase::template Iterator<_ValueDef>;

    template<typename ... Args>
    IndexViewGen(const PDatabase &db,KeyspaceID kid,Direction dir,const PSnapshot &snap, Args && ... baseArgs)
        :IndexBase(std::forward<Args>(baseArgs)...),_db(db),_kid(kid),_dir(dir),_snap(snap) {}

    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> get(Key &key) const {
        key.change_kid(_kid);
        return _db->get_as_document< Document<_ValueDef> >(key, _snap);
    }
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> operator[](Key &&key) const {return get(key);}

    Iterator scan(Direction dir = Direction::normal)const  {
        if (isForward(_dir)) {
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key,dir);}
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key,RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            Key pfx = key.prefix_end();
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    pfx,RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }
    Iterator scan_prefix(Key &&key, Direction dir = Direction::normal) const {return scan_prefix(key,dir);}
    Iterator scan_prefix(Key &key, Direction dir = Direction::normal) const {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_iterator<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded)const  {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) const {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) const {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded)const  {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            if (last_record == LastRecord::included) {
                Key pfx = from.prefix_end();
                return IndexBase::template create_iterator<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        from,pfx,
                        FirstRecord::included, LastRecord::excluded
                });
            } else {
                return IndexBase::template create_iterator<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        from,to,
                        FirstRecord::included, LastRecord::excluded
                });
            }

        } else {
            Key xfrom = from.prefix_end();
            if (last_record == LastRecord::included) {
                return IndexBase::template create_iterator<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        xfrom,to,
                        FirstRecord::excluded, LastRecord::included
                });
            } else {
                Key xto = to.prefix_end();
                return IndexBase::template create_iterator<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        xfrom,xto,
                        FirstRecord::excluded, LastRecord::included
                });
            }
        }
    }


protected:
    PDatabase _db;
    KeyspaceID _kid;
    Direction _dir;
    PSnapshot _snap;
};

template<typename _DocDef>
struct SkipDocIDDcument {
    using Type = typename _DocDef::Type;
    template<typename Iter>
    Type from_binary(Iter from, Iter to) {
        std::advance(from, sizeof(DocID));
        return _DocDef::from_binary(from, to);
    }
    template<typename Iter>
    void to_binary(const Type &val, Iter push) {
        static_assert(defer_false<Iter>, "Not implemented");
    }
};

template<typename _Storage, typename _ValueDef=RowDocument, IndexType index_type = IndexType::multi>
using IndexView = IndexViewGen<_ValueDef,
        std::conditional_t<index_type == IndexType::multi,
                IndexViewBaseWithStorage_IDinkey<_Storage>,
                IndexViewBaseWithStorage_IDinvalue<_Storage>> >;

}




#endif /* SRC_DOCDB_INDEX_VIEW_H_ */
