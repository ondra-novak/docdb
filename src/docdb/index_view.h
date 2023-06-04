#pragma once
#ifndef SRC_DOCDB_INDEX_VIEW_H_
#define SRC_DOCDB_INDEX_VIEW_H_

#include "viewbase.h"
#include "recordset.h"
#include "row.h"

#include "doc_storage_concept.h"

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

template<DocumentStorageViewType _Storage, typename DocIDExtract, std::size_t hide_dup_sz = 0>
class IndexViewBaseWithStorage {
public:

    IndexViewBaseWithStorage(_Storage &storage):_storage(storage) {}


    template<typename DocDef>
    struct IteratorValueType: IndexViewBaseEmpty::IteratorValueType<DocDef> {
        DocID id;
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
                {Key(RowView(rk)),
                DocDef::from_binary(rv.begin(), rv.end())},
                id,
            };
        }

        Iterator begin()  {return {this, false};}
        Iterator end() {return {this, true};}

    protected:
        _Storage &_storage;
        [[no_unique_address]] DocIDExtract _extract;
    };

    template<typename DocDef>
    RecordSet<DocDef> create_recordset(std::unique_ptr<leveldb::Iterator> &&iter, RecordSetBase::Config &&config) const {
        if constexpr(hide_dup_sz > 0) {
            config.filter = [prev = std::string()](const RecordSetBase &b) mutable {
                std::string_view r = b.raw_key();
                if (r.size() <= hide_dup_sz) return false;
                r = r.substr(0, r.size()-hide_dup_sz);
                if (prev == r) return false;
                prev = r;
                return true;
            };
        }
        return RecordSet<DocDef>(_storage, std::move(iter), std::move(config));
    }

    _Storage &get_storage() const {
        return _storage;
    }

    template<typename DocDef>
    auto get_document(const IteratorValueType<DocDef> &x) const {
        return _storage.find(x.id);
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
class IndexViewGen: public ViewBase<_ValueDef>, public IndexBase {
public:

    using RecordSet = typename IndexBase::template RecordSet<_ValueDef>;

    template<typename ... Args>
    IndexViewGen(const PDatabase &db,KeyspaceID kid,Direction dir,const PSnapshot &snap, bool no_cache, Args && ... baseArgs)
        :ViewBase<_ValueDef>(db,kid,dir,snap,no_cache)
        ,IndexBase(std::forward<Args>(baseArgs)...) {}



    IndexViewGen get_snapshot(bool no_cache = true) const {
        if (this->_snap) return *this;
        return IndexViewGen(this->_db, this->_kid, this->_dir, this->_db->make_snapshot(), true, static_cast<const IndexBase &>(*this));
    }

    IndexViewGen reverse() const {
        return IndexViewGen(this->_db, this->_kid, isForward(this->_dir)?Direction::backward:Direction::forward, this->_snap, this->_no_cache, static_cast<const IndexBase &>(*this));
    }

    RecordSet select_all(Direction dir = Direction::normal)const  {
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    RawKey(this->_kid),RawKey(this->_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    RawKey(this->_kid+1),RawKey(this->_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    RecordSet select_from(Key &&key, Direction dir = Direction::normal) {return select_from(key,dir);}
    RecordSet select_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(false,this->_snap),{
                    key,RawKey(this->_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            Key pfx = key.prefix_end();
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(false,this->_snap),{
                    pfx,RawKey(this->_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }
    RecordSet select(Key &&key, Direction dir = Direction::normal) const {return select(key,dir);}
    RecordSet select(Key &key, Direction dir = Direction::normal) const {
        key.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap,this->_no_cache),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    RecordSet select_between(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded)const  {return select_between(from, to, last_record);}
    RecordSet select_between(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) const {return select_between(from, to, last_record);}
    RecordSet select_between(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) const {return select_between(from, to, last_record);}
    RecordSet select_between(Key &from, Key &to, LastRecord last_record = LastRecord::excluded)const  {
        from.change_kid(this->_kid);
        to.change_kid(this->_kid);
        if (from <= to) {
            if (last_record == LastRecord::included) {
                Key pfx = from.prefix_end();
                return IndexBase::template create_recordset<_ValueDef>(
                        this->_db->make_iterator(false,this->_snap),{
                        from,pfx,
                        FirstRecord::included, LastRecord::excluded
                });
            } else {
                return IndexBase::template create_recordset<_ValueDef>(
                        this->_db->make_iterator(false,this->_snap),{
                        from,to,
                        FirstRecord::included, LastRecord::excluded
                });
            }

        } else {
            Key xfrom = from.prefix_end();
            if (last_record == LastRecord::included) {
                return IndexBase::template create_recordset<_ValueDef>(
                        this->_db->make_iterator(this->_snap,this->_no_cache),{
                        xfrom,to,
                        FirstRecord::excluded, LastRecord::included
                });
            } else {
                Key xto = to.prefix_end();
                return IndexBase::template create_recordset<_ValueDef>(
                        this->_db->make_iterator(this->_snap,this->_no_cache),{
                        xfrom,xto,
                        FirstRecord::excluded, LastRecord::included
                });
            }
        }
    }

    RecordSet operator > (Key &&x) const {return select_greater_then(x);}
    RecordSet operator > (Key &x) const {return select_greater_then(x);}
    RecordSet select_greater_then (Key &&x) const {return select_less_then (x);}
    RecordSet select_greater_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x.prefix_end(), RawKey(this->_kid+1),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid+1),x.prefix_end(),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    RecordSet operator < (Key &&x) const {return select_less_then(x);}
    RecordSet operator < (Key &x) const {return select_less_then(x);}
    RecordSet select_less_then (Key &&x) const {return select_less_then (x);}
    RecordSet select_less_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid),x,
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x,RawKey(this->_kid),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    RecordSet operator >= (Key &&x) const {return select_greater_or_equal_then(x);}
    RecordSet operator >= (Key &x) const {return select_greater_or_equal_then(x);}
    RecordSet select_greater_or_equal_then (Key &&x) const {return select_greater_or_equal_then (x);}
    RecordSet select_greater_or_equal_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x, RawKey(this->_kid+1),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid+1),x,
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    RecordSet operator <= (Key &&x) const {return select_less_or_equal_then(x);}
    RecordSet operator <= (Key &x) const {return select_less_or_equal_then(x);}
    RecordSet select_less_or_equal_then (Key &&x) const {return select_less_or_equal_then (x);}
    RecordSet select_less_or_equal_then (Key &x) const {
        x.change_kid(this->_kid);
        if (isForward(this->_dir)) {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 RawKey(this->_kid),x.prefix_end(),
                 FirstRecord::included, FirstRecord::excluded
            });
        } else {
            return IndexBase::template create_recordset<_ValueDef>(
                    this->_db->make_iterator(this->_snap, this->_no_cache),{
                 x.prefix_end(),RawKey(this->_kid),
                 FirstRecord::excluded, FirstRecord::included
            });

        }
    }
    RecordSet operator == (Key &&x) const {return select_equal_to(x);}
    RecordSet operator == (Key &x) const {return select_equal_to(x);}
    RecordSet select_equal_to (Key &&x) const {return select_equal_to (x);}
    RecordSet select_equal_to (Key &x) const {return select(x);}

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
auto operator!=(Key &k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    static_assert(defer_false<IndexBase>);
}
template<typename _ValueDef, typename IndexBase>
auto operator!=(Key &&k, const IndexViewGen<_ValueDef, IndexBase> &index) {
    static_assert(defer_false<IndexBase>);
}

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
        std::conditional_t<index_type == IndexType::multi || index_type == IndexType::unique_hide_dup,
        IndexViewGen<_ValueDef,IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromKey, index_type==IndexType::unique_hide_dup?sizeof(DocID):0> >,
        IndexViewGen<SkipDocIDDcument<_ValueDef>, IndexViewBaseWithStorage<_Storage, ExtractDocumentIDFromValue> > >;

}




#endif /* SRC_DOCDB_INDEX_VIEW_H_ */
