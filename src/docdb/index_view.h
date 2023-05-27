#pragma once
#ifndef SRC_DOCDB_INDEX_VIEW_H_
#define SRC_DOCDB_INDEX_VIEW_H_

#include "recordset.h"
#include "row.h"

#include "doc_storage_concept.h"
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
    struct IteratorValueType {
        Key key;
        typename DocDef::Type value;
    };

    template<typename DocDef>
    class RecordSet : public RecordSetBase {
    public:
        using RecordSetBase::RecordSetBase;

        using Iterator = RecordSetIterator<RecordSet, IteratorValueType<DocDef> >;

        auto begin() {
            return Iterator(this, false);
        }
        auto end() {
            return Iterator(this, true);
        }

        IteratorValueType<DocDef> get_item() const {
            auto rv = this->raw_value();
            return {Key(RowView(this->raw_key())), DocDef::from_binary(rv.begin(), rv.end())};
        }

    };

    template<typename DocDef>
    RecordSet<DocDef> create_recordset(std::unique_ptr<leveldb::Iterator> &&iter, typename RecordSet<DocDef>::Config &&config) const {
        return RecordSet<DocDef>(std::move(iter), std::move(config));
    }

};

template<DocumentStorageViewType _Storage, typename DocIDExtract>
class IndexViewBaseWithStorage {
public:

    IndexViewBaseWithStorage(_Storage &storage):_storage(storage) {}


    template<typename DocDef>
    struct IteratorValueType: IndexViewBaseEmpty::IteratorValueType<DocDef> {
        DocID id;
        _Storage *storage = nullptr;
        auto document() const {
            return (*storage)[id];
        }
    };

    template<typename DocDef>
    class RecordSet: public RecordSetBase {
    public:
        RecordSet(_Storage &stor, std::unique_ptr<leveldb::Iterator> &&iter, typename RecordSetBase::Config &&config)
        :RecordSetBase(std::move(iter), std::move(config))
        ,_storage(stor) {}

        using Iterator = RecordSetIterator<RecordSet, IteratorValueType<DocDef> >;


        IteratorValueType<DocDef> get_item() const {
            auto rk = this->raw_key();
            auto rv = this->raw_value();
            auto id = _extract(rk, rv);
            return {
                Key(RowView(rk)),
                DocDef::from_binary(rv.begin(), rv.end()),
                id,
                &_storage
            };
        }

        Iterator begin()  {return {this, false};}
        Iterator end() {return {this, true};}

    protected:
        _Storage &_storage;
        [[no_unique_address]] DocIDExtract _extract;
    };

    template<typename DocDef>
    RecordSet<DocDef> create_recordset(std::unique_ptr<leveldb::Iterator> &&iter, typename RecordSetBase::Config &&config) const {
        return RecordSet<DocDef>(_storage, std::move(iter), std::move(config));
    }

protected:
    _Storage &_storage;
};

struct ExtractDocumentIDFromKey {
    DocID operator()(const std::string_view &key, const std::string_view &value) const {
        auto docidstr = key.substr(key.length()-sizeof(DocID));
        auto [id] = Row::extract<DocID>(docidstr);
        return id;
    }
};

struct ExtractDocumentIDFromValue {
    DocID operator()(const std::string_view &key, const std::string_view &value) const{
        auto [id] = Row::extract<DocID>(value);
        return id;
    }
};


template<typename _ValueDef, typename IndexBase>
class IndexViewGen: public IndexBase {
public:

    using RecordSet = typename IndexBase::template RecordSet<_ValueDef>;

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

    RecordSet select_all(Direction dir = Direction::normal)const  {
        if (isForward(_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    RecordSet lookup(Key &key) const {return select_prefix(key);}
    RecordSet lookup(Key &&key) const {return select_prefix(key);}

    RecordSet select_from(Key &&key, Direction dir = Direction::normal) {return select_from(key,dir);}
    RecordSet select_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key,RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            Key pfx = key.prefix_end();
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    pfx,RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }
    RecordSet select_prefix(Key &&key, Direction dir = Direction::normal) const {return select_prefix(key,dir);}
    RecordSet select_prefix(Key &key, Direction dir = Direction::normal) const {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    _db->make_iterator(false,_snap),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    RecordSet select_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded)const  {return select_range(from, to, last_record);}
    RecordSet select_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) const {return select_range(from, to, last_record);}
    RecordSet select_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) const {return select_range(from, to, last_record);}
    RecordSet select_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded)const  {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            if (last_record == LastRecord::included) {
                Key pfx = from.prefix_end();
                return IndexBase::template create_recordset<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        from,pfx,
                        FirstRecord::included, LastRecord::excluded
                });
            } else {
                return IndexBase::template create_recordset<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        from,to,
                        FirstRecord::included, LastRecord::excluded
                });
            }

        } else {
            Key xfrom = from.prefix_end();
            if (last_record == LastRecord::included) {
                return IndexBase::template create_recordset<_ValueDef>(
                        _db->make_iterator(false,_snap),{
                        xfrom,to,
                        FirstRecord::excluded, LastRecord::included
                });
            } else {
                Key xto = to.prefix_end();
                return IndexBase::template create_recordset<_ValueDef>(
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
    static Type from_binary(Iter from, Iter to) {
        std::advance(from, sizeof(DocID));
        return _DocDef::from_binary(from, to);
    }
    template<typename Iter>
    static Iter to_binary(const Type &val, Iter push) {
        static_assert(defer_false<Iter>, "Not implemented");
        return push;
    }
    //WriteIteratorConcept to_binary(const Type &val, WriteIteratorConcept push);
};

template<DocumentStorageViewType _Storage, typename _ValueDef, IndexType index_type>
using IndexView =
        std::conditional_t<index_type == IndexType::multi,
        IndexViewGen<_ValueDef,IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromKey> >,
        IndexViewGen<SkipDocIDDcument<_ValueDef>, IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromValue> > >;

}




#endif /* SRC_DOCDB_INDEX_VIEW_H_ */
