#pragma once
#ifndef SRC_DOCDB_STORAGE_VIEW_H_
#define SRC_DOCDB_STORAGE_VIEW_H_
#include "database.h"

#include "viewbase.h"
namespace docdb {

///Type of document ID
using DocID = std::uint64_t;



template<typename _DocDef>
static bool document_is_deleted(const typename _DocDef::Type &t) {
    if constexpr(DocumentCustomDeleted<_DocDef>) {
        return _DocDef::is_deleted(t);
    }
    return false;
}


///Contains whole record from the storage
/**
 * This class is accessible from DocRecordT and from Storage::DocRecord. For document
 * without default constructor, the struct DocRecord_Un is used
 *
 * @tparam _DocDef Document definition struct
 */
template<typename _DocDef>
struct DocRecord_Def {
    using DocType = typename _DocDef::Type;
    ///Contains document itself
    DocType document = {};
    ///Contains ID of a document which has been replaced
    DocID previous_id = 0;
    ///Contains true if this record is deleted document
    bool deleted = true;
    ///There is value loaded in (even if it deleted)
    const bool has_value = false;

    DocRecord_Def(DocID previous_id):previous_id(previous_id),deleted(true) {}
    template<typename Iter>
    DocRecord_Def(Iter b, Iter e):has_value(std::distance(b ,e) > static_cast<std::ptrdiff_t>(sizeof(DocID))) {
        previous_id = Row::deserialize_item<DocID>(b, e);
        if (has_value) {
            document =  _DocDef::from_binary(b,e);
            deleted = document_is_deleted<_DocDef>(document);
        } else {
            deleted = true;
        }
    }


};

///Contains whole record from the storage
/**
 * This class is accessible from DocRecordT and from Storage::DocRecord. This
 * struct is used when document type has no default constructor
 *
 * @tparam _DocDef Document definition struct
 */
template<typename _DocDef>
struct DocRecord_Un {
    using DocType = typename _DocDef::Type;
    ///Contains document itself
    /**
     * @note If the document is erased, it can be still constructed, you need to
     * check has_value before accessing the object
     *
     */
    union {
        DocType document;
    };
    ///Contains ID of a document which has been replaced
    DocID previous_id = 0;
    ///contains true, if the document contains a value, when false, do not access this variable
    const bool has_value = false;
    ///contains true, if the document is marked as deleted
    bool erased = true;


    DocRecord_Un(DocID previous_id):previous_id(previous_id)  {}
    ~DocRecord_Un() {if (has_value) document.~DocType();}
    template<typename Iter>
    DocRecord_Un(Iter b, Iter e):has_value(load_info(b,e,previous_id)) {
        erased = document_is_deleted<_DocDef>(b, e);
        if (has_value) {
            const_cast<bool &>(has_value) = true;
            ::new(&document) DocType(_DocDef::from_binary(b,e));
        }
    }
    DocRecord_Un(const DocRecord_Un &other)
        :previous_id(other.previous_id),has_value(other.has_value), erased(other.erased) {
        if (has_value) new(&document) DocType(other.document);
    }
    DocRecord_Un(DocRecord_Un &&other):previous_id(other.previous_id),has_value(other.has_value), erased(other.erased) {
        if (has_value) new(&document) DocType(std::move(other.document));
    }
    DocRecord_Un &operator=(const DocRecord_Un &other) {
        if (this != &other) {
            this->~DocRecord_Un();
            ::new(this) DocRecord_Un(other);
        }
        return *this;
    }
    DocRecord_Un &operator=(DocRecord_Un &&other) {
        if (this != &other) {
            this->~DocRecord_Un();
            ::new(this) DocRecord_Un(std::move(other));
        }
        return *this;
    }
    template<typename Iter>
    static bool load_info(Iter &b, Iter e, DocID &prvid) {
        prvid = Row::deserialize_item<DocID>(b, e);
        return b == e;

    }

};

template<typename _DocDef>
using DocRecordT = std::conditional_t<std::is_default_constructible_v<typename _DocDef::Type>,DocRecord_Def<_DocDef>,DocRecord_Un<_DocDef> >;

template<typename _DocDef>
struct DocRecordDef {
    using Type = DocRecordT<_DocDef>;
    template<typename Iter>
    static Type from_binary(Iter b, Iter e) {
        return Type(b,e);
    }
    template<typename Iter>
    static Iter to_binary(const Type &type, Iter iter) {
        Row::serialize_items(iter, type.previous_id);
        if (type.deleted) return iter;
        return _DocDef::to_binary(type.document, iter);
    }

};

struct ExportedDocument {
    DocID id;
    std::vector<char> data;
};


template<DocumentDef _DocDef = RowDocument>
class StorageView: public ViewBase<DocRecordDef<_DocDef> > {
public:

    using DocType = typename _DocDef::Type;

    StorageView(const PDatabase &db, KeyspaceID kid, Direction dir, const PSnapshot &snap, bool no_cache)
        :ViewBase<DocRecordDef<_DocDef> >(db,kid,dir,snap,no_cache) {}

    StorageView get_snapshot(bool no_cache = true) const {
        if (this->_snap) return *this;
        return StorageView(this->_db, this->_kid, this->_dir, this->_db->make_snapshot(), no_cache);
    }

    StorageView get_snapshot(PSnapshot snap, bool no_cache = true) const {
        return StorageView(this->_db, this->_kid, this->_dir, snap, no_cache);
    }

    StorageView reverse() const {
        return StorageView(this->_db, this->_kid, isForward(this->_dir)?Direction::backward:Direction::forward, this->_snap, this->_no_cache);
    }


    using DocRecord = DocRecordT<_DocDef>;


    struct IteratorValueType: DocRecord {
        DocID id = 0;
        IteratorValueType(std::string_view raw_key, std::string_view raw_value)
            :DocRecord(DocRecordDef<_DocDef>::from_binary(raw_value.begin(),raw_value.end()))
             {
                Key k = Key::from_string(raw_key);
                id  = std::get<0>(k.get<DocID>());
             }
    };



    ///Iterator
    class Recordset: public RecordsetBase {
    public:
        using RecordsetBase::RecordsetBase;

        using Iterator = RecordsetIterator<Recordset, IteratorValueType>;

        auto begin() {
            return Iterator {this, false};
        }
        auto end() {
            return Iterator {this, true};
        }

        IteratorValueType get_item() const {


            return {this->raw_key(), this->raw_value()};
        }
    };

    ///Scan whole storage
    Recordset select_all(Direction dir=Direction::normal) const {
        if (isForward(changeDirection(this->_dir, dir))) {
            return Recordset(this->_db->make_iterator(this->_snap, this->_no_cache),{
                    RawKey(this->_kid),RawKey(this->_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Recordset(this->_db->make_iterator(this->_snap, this->_no_cache),{
                    RawKey(this->_kid+1),RawKey(this->_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }

    ///Scan from given document for given direction
    Recordset select_from(DocID start_pt, Direction dir = Direction::normal) const {
        if (isForward(changeDirection(this->_dir, dir))) {
            return Recordset(this->_db->make_iterator(this->_snap, this->_no_cache),{
                    RawKey(this->_kid, start_pt),RawKey(this->_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Recordset(this->_db->make_iterator(this->_snap, this->_no_cache),{
                    RawKey(this->_kid, start_pt),RawKey(this->_kid),
                    FirstRecord::included, LastRecord::excluded
            });
        }
    }

    ///Scan for range
    Recordset select_range(DocID start_id, DocID end_id, LastRecord last_record = LastRecord::excluded) const {
        return Recordset(this->_db->make_iterator(false,this->_snap),{
                RawKey(this->_kid, start_id),RawKey(this->_kid, end_id),
                FirstRecord::included, last_record
        });
    }

    ///Retrieve ID of last document
    /**
     * @return id of last document, or zero if database is empty
     */
    DocID get_last_document_id() const {
        auto rs = select_all(Direction::backward);
        auto beg = rs.begin();
        if (beg == rs.end()) return 0;
        return beg->id;
    }

    template<std::invocable<ExportedDocument> Fn>
    static void export_documents(Recordset &rc, Fn &&export_fn) {
        ExportedDocument x;
        while (!rc.empty()) {
            auto [tmp1, id] = Row::extract<KeyspaceID, DocID>(rc.raw_key());
            auto v = rc.raw_value();
            x.id = id;
            x.data.clear();
            std::copy(v.begin(), v.end(), std::back_inserter(x.data));
            export_fn(x);
            rc.next();
        }

    }

};


}




#endif /* SRC_DOCDB_STORAGE_VIEW_H_ */
