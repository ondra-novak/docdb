#pragma once
#ifndef SRC_DOCDB_INDEX_VIEW_H_
#define SRC_DOCDB_INDEX_VIEW_H_

#include "viewbase.h"
#include "recordset.h"
#include "row.h"


namespace docdb {

enum class IndexType {
    ///unique index
    /**
     * Only unique keys are allowed. Attempt to place duplicated key
     * causes exception and rollback of current batch
     *
     * @note performing check on keys can reduce performance. You can
     * disable the check by using unique_no_check index type.
     */
    unique,
    ///Unique index with no check
    /**
     * Only unique keys. However, the duplication is not checked. Duplicated
     * keys overwrites one over second, so only last document with duplicated
     * key appear in the index.
     *
     * @note this index has better performance for writing new documents
     *
     */
    unique_no_check,

    ///Unique index hiding duplicated keys
    /**
     * Duplicated keys are indexed, but they are hidden in the recordset,
     *
     * @note this index is implemented as 'multi' with filter option in
     * recordset
     */
    unique_hide_dup,

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


template<typename T>
DOCDB_CXX20_CONCEPT(DocumentStorageViewType , requires(T x) {
    {x.get_db() } -> std::convertible_to<PDatabase>;
    {x.find(std::declval<DocID>())->document } -> std::convertible_to<const typename T::DocType &>;
    {x.find(std::declval<DocID>()) } -> std::convertible_to<bool>;
    {x.select_all()} -> std::derived_from<RecordsetBase>;
    {x.select_from(std::declval<DocID>())} -> std::derived_from<RecordsetBase>;
});

template<typename T>
DOCDB_CXX20_CONCEPT(DocumentStorageType , requires(T x) {
   requires DocumentStorageViewType<T>;
    {x.register_transaction_observer(std::declval<std::function<void(Batch &, const typename T::Update &)> >())} -> std::same_as<void>;
    {x.rescan_for(std::declval<std::function<void(Batch &, const typename T::Update &)> >(), std::declval<DocID>())};
    {x.get_rev()} -> std::same_as<DocID>;
    {T::Update::old_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::new_doc}->std::convertible_to<const typename T::DocType *>;
    {T::Update::old_doc_id}->std::convertible_to<DocID>;
    {T::Update::old_old_doc_id}->std::convertible_to<DocID>;
    {T::Update::new_doc_id}->std::convertible_to<DocID>;
});



template<typename _ValueDef>
struct ValueAndDocID {
    using ValueType = typename _ValueDef::Type;
    DocID id;
    ValueType value;
};

template<typename _ValueDef>
struct ValueAndDocIDDocument {
    using Type = ValueAndDocID<_ValueDef>;
    template<typename Iter>
    static Type from_binary(Iter from, Iter to) {
        DocID id = Row::deserialize_item<DocID>(from, to);
        return {id, _ValueDef::from_binary(from, to)};
    }
    template<typename Iter>
    static Iter to_binary(const Type &, Iter push) {
        static_assert(defer_false<Iter>, "Not implemented");
        return push;
    }

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
    static Iter to_binary(const Type &, Iter push) {
        static_assert(defer_false<Iter>, "Not implemented");
        return push;
    }
    //WriteIteratorConcept to_binary(const Type &val, WriteIteratorConcept push);
};


template<typename DocDef>
class IndexViewBaseEmpty {
public:

    struct IteratorValueType {
        Key key;
        typename DocDef::Type value;
    };

    class Recordset : public RecordsetBase {
    public:
        using RecordsetBase::RecordsetBase;

        using Iterator = RecordsetIterator<Recordset, IteratorValueType >;

        auto begin() {
            return Iterator(this, false);
        }
        auto end() {
            return Iterator(this, true);
        }

        IteratorValueType get_item() const {
            auto rv = this->raw_value();
            return {Key::from_string(this->raw_key()), DocDef::from_binary(rv.begin(), rv.end())};
        }

    };

    Recordset create_recordset(std::unique_ptr<leveldb::Iterator> &&iter, typename Recordset::Config &&config) const {
        return Recordset(std::move(iter), std::move(config));
    }

};

template<DocumentStorageViewType _Storage, typename DocIDExtract, std::size_t hide_dup_sz = 0>
class IndexViewBaseWithStorage {
public:

    using ValueDef = typename DocIDExtract::ValueDef;

    IndexViewBaseWithStorage(_Storage &storage):_storage(storage) {}


    struct IteratorValueType: IndexViewBaseEmpty<ValueDef>::IteratorValueType {
        DocID id;
    };

    class Recordset: public RecordsetBase {
    public:
        Recordset(_Storage &stor, std::unique_ptr<leveldb::Iterator> &&iter, typename RecordsetBase::Config &&config)
        :RecordsetBase(std::move(iter), std::move(config))
        ,_storage(stor) {}

        using Iterator = RecordsetIterator<Recordset, IteratorValueType >;


        IteratorValueType get_item() const {
            auto rk = this->raw_key();
            auto rv = this->raw_value();
            auto id = _extract(rk, rv);
            return {
                {Key::from_string(rk),
                ValueDef::from_binary(rv.begin(), rv.end())},
                id,
            };
        }

        Iterator begin()  {return {this, false};}
        Iterator end() {return {this, true};}

    protected:
        _Storage &_storage;
        [[no_unique_address]] DocIDExtract _extract;
    };

    Recordset create_recordset(std::unique_ptr<leveldb::Iterator> &&iter, RecordsetBase::Config &&config) const {
        if constexpr(hide_dup_sz > 0) {
            config.filter = [prev = std::string()](const RecordsetBase &b) mutable {
                std::string_view r = b.raw_key();
                if (r.size() <= hide_dup_sz) return false;
                r = r.substr(0, r.size()-hide_dup_sz);
                if (prev == r) return false;
                prev = r;
                return true;
            };
        }
        return Recordset(_storage, std::move(iter), std::move(config));
    }

    _Storage &get_storage() const {
        return _storage;
    }

    auto get_document(const IteratorValueType &x) const {
        return _storage.find(x.id);
    }

protected:
    _Storage &_storage;
};

template<typename _ValueDef>
struct ExtractDocumentIDFromKey {
    DocID operator()(const std::string_view &key, const std::string_view &) const {
        auto docidstr = key.substr(key.length()-sizeof(DocID));
        auto [id] = Row::extract<DocID>(docidstr);
        return id;
    }
    using ValueDef = _ValueDef;
};

template<typename _ValueDef>
struct ExtractDocumentIDFromValue {
    DocID operator()(const std::string_view &, const std::string_view &value) const{
        auto [id] = Row::extract<DocID>(value);
        return id;
    }
    using ValueDef = SkipDocIDDcument<_ValueDef>;
};


template<typename _ValueDef, typename IndexBase>
class IndexViewGen: public ViewBase<_ValueDef>, public IndexBase {
public:

    using Recordset = typename IndexBase::Recordset;

    template<typename ... Args>
    IndexViewGen(const PDatabase &db,KeyspaceID kid,Direction dir,const PSnapshot &snap, bool no_cache, Args && ... baseArgs)
        :ViewBase<_ValueDef>(db,kid,dir,snap,no_cache)
        ,IndexBase(std::forward<Args>(baseArgs)...) {}



    IndexViewGen get_snapshot(bool no_cache = true) const {
        if (this->_snap) return *this;
        return IndexViewGen(this->_db, this->_kid, this->_dir, this->_db->make_snapshot(), no_cache, static_cast<const IndexBase &>(*this));
    }

    IndexViewGen get_snapshot(PSnapshot snap, bool no_cache = true) const {
        return IndexViewGen(this->_db, this->_kid, this->_dir, snap, no_cache, static_cast<const IndexBase &>(*this));
    }

    IndexViewGen reverse() const {
        return IndexViewGen(this->_db, this->_kid, isForward(this->_dir)?Direction::backward:Direction::forward, this->_snap, this->_no_cache, static_cast<const IndexBase &>(*this));
    }

    Recordset select_all(Direction dir = Direction::normal)const  {
        if (isForward(changeDirection(this->_dir, dir))) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    RawKey(this->_kid),RawKey(this->_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    RawKey(this->_kid+1),RawKey(this->_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    Recordset select_from(Key &&key, Direction dir = Direction::normal) {return select_from(key,dir);}
    Recordset select_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(this->_kid);
        if (isForward(changeDirection(this->_dir, dir))) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(false,this->_snap),{
                    key,RawKey(this->_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            RawKey pfx = key.prefix_end();
            return IndexBase::create_recordset(
                    this->_db->make_iterator(false,this->_snap),{
                    pfx,RawKey(this->_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }
    Recordset select(Key &&key, Direction dir = Direction::normal) const {return select(key,dir);}
    Recordset select(Key &key, Direction dir = Direction::normal) const {
        key.change_kid(this->_kid);
        if (isForward(changeDirection(this->_dir,dir))) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    Recordset select_between(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded)const  {return select_between(from, to, last_record);}
    Recordset select_between(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) const {return select_between(from, to, last_record);}
    Recordset select_between(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) const {return select_between(from, to, last_record);}
    Recordset select_between(Key &from, Key &to, LastRecord last_record = LastRecord::excluded)const  {
        from.change_kid(this->_kid);
        to.change_kid(this->_kid);
        if (from <= to) {
            if (last_record == LastRecord::included) {
                RawKey pfx = to.prefix_end();
                return IndexBase::create_recordset(
                        this->_db->make_iterator(this->_snap, this->_no_cache),{
                        from,pfx,
                        FirstRecord::included, LastRecord::excluded
                });
            } else {
                return IndexBase::create_recordset(
                        this->_db->make_iterator(this->_snap, this->_no_cache),{
                        from,to,
                        FirstRecord::included, LastRecord::excluded
                });
            }

        } else {
            RawKey xfrom = from.prefix_end();
            if (last_record == LastRecord::included) {
                return IndexBase::create_recordset(
                        this->_db->make_iterator(this->_snap,this->_no_cache),{
                        xfrom,to,
                        FirstRecord::excluded, LastRecord::included
                });
            } else {
                RawKey xto = to.prefix_end();
                return IndexBase::create_recordset(
                        this->_db->make_iterator(this->_snap,this->_no_cache),{
                        xfrom,xto,
                        FirstRecord::excluded, LastRecord::included
                });
            }
        }
    }

    Recordset operator > (Key &&x) const {return select_greater_then(x);}
    Recordset operator > (Key &x) const {return select_greater_then(x);}
    Recordset select_greater_then (Key &&x) const {return select_less_then (x);}
    Recordset select_greater_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x.prefix_end(), RawKey(this->_kid+1),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid+1),x.prefix_end(),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    Recordset operator < (Key &&x) const {return select_less_then(x);}
    Recordset operator < (Key &x) const {return select_less_then(x);}
    Recordset select_less_then (Key &&x) const {return select_less_then (x);}
    Recordset select_less_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid),x,
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x,RawKey(this->_kid),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    Recordset operator >= (Key &&x) const {return select_greater_or_equal_then(x);}
    Recordset operator >= (Key &x) const {return select_greater_or_equal_then(x);}
    Recordset select_greater_or_equal_then (Key &&x) const {return select_greater_or_equal_then (x);}
    Recordset select_greater_or_equal_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x, RawKey(this->_kid+1),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid+1),x,
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    Recordset operator <= (Key &&x) const {return select_less_or_equal_then(x);}
    Recordset operator <= (Key &x) const {return select_less_or_equal_then(x);}
    Recordset select_less_or_equal_then (Key &&x) const {return select_less_or_equal_then (x);}
    Recordset select_less_or_equal_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid),x.prefix_end(),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::create_recordset(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x.prefix_end(),RawKey(this->_kid),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    Recordset operator == (Key &&x) const {return select_equal_to(x);}
    Recordset operator == (Key &x) const {return select_equal_to(x);}
    Recordset select_equal_to (Key &&x) const {return select_equal_to (x);}
    Recordset select_equal_to (Key &x) const {return select(x);}

};


template<typename _ValueDef, typename IndexBase>
auto operator<(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index > k;
}
template<typename _ValueDef, typename IndexBase>
auto operator<(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index > k;
}
template<typename _ValueDef, typename IndexBase>
auto operator>(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index < k;
}
template<typename _ValueDef, typename IndexBase>
auto operator>(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index < k;
}

template<typename _ValueDef, typename IndexBase>
auto operator<=(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index >= k;
}
template<typename _ValueDef, typename IndexBase>
auto operator<=(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index >= k;
}
template<typename _ValueDef, typename IndexBase>
auto operator>=(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index <= k;
}
template<typename _ValueDef, typename IndexBase>
auto operator>=(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index <= k;
}
template<typename _ValueDef, typename IndexBase>
auto operator==(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index == k;
}
template<typename _ValueDef, typename IndexBase>
auto operator==(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    return index == k;
}

template<typename _ValueDef, typename IndexBase>
auto operator!=(Key &, const IndexViewGen<_ValueDef, IndexBase> &) {
    static_assert(defer_false<IndexBase>);
}
template<typename _ValueDef, typename IndexBase>
auto operator!=(Key &&, const IndexViewGen<_ValueDef, IndexBase> &) {
    static_assert(defer_false<IndexBase>);
}


template<DocumentStorageViewType _Storage, typename _ValueDef, IndexType index_type>
using IndexView =
        std::conditional_t<index_type == IndexType::multi || index_type == IndexType::unique_hide_dup,
        IndexViewGen<_ValueDef,IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromKey<_ValueDef>, index_type==IndexType::unique_hide_dup?sizeof(DocID):0> >,
        IndexViewGen<ValueAndDocIDDocument<_ValueDef>, IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromValue<_ValueDef> > > >;



using IndexRevision = std::size_t;
}



#endif /* SRC_DOCDB_INDEX_VIEW_H_ */
