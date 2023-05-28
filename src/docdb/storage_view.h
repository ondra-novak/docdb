#pragma once
#ifndef SRC_DOCDB_STORAGE_VIEW_H_
#define SRC_DOCDB_STORAGE_VIEW_H_
#include "database.h"

namespace docdb {

///Type of document ID
using DocID = std::uint64_t;



template<typename _DocDef, typename Iter>
static bool document_is_deleted(Iter b, Iter e) {
    if (b == e) return true;
    if constexpr(DocumentCustomDeleted<_DocDef>) {
        return _DocDef::is_deleted(b, e);
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
    DocType content = {};
    ///Contains ID of a document which has been replaced
    DocID previous_id = 0;
    ///Contains true if this record is deleted document
    bool deleted = true;
    DocRecord_Def(DocID previous_id):previous_id(previous_id),deleted(true) {}
    template<typename Iter>
    DocRecord_Def(Iter b, Iter e) {
        previous_id = Row::deserialize_item<DocID>(b, e);
        deleted = document_is_deleted<_DocDef>(b, e);
        if (b != e) {
            content =  _DocDef::from_binary(b,e);
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
        DocType content;
    };
    ///Contains ID of a document which has been replaced
    DocID previous_id = 0;
    ///contains true, if the content contains a value, when false, do not access this variable
    const bool has_value = false;
    ///contains true, if the document is marked as deleted
    bool erased = true;


    DocRecord_Un(DocID previous_id):previous_id(previous_id)  {}
    ~DocRecord_Un() {if (has_value) content.~DocType();}
    template<typename Iter>
    DocRecord_Un(Iter b, Iter e):has_value(load_info(b,e,previous_id)) {
        erased = document_is_deleted<_DocDef>(b, e);
        if (has_value) {
            const_cast<bool &>(has_value) = true;
            ::new(&content) DocType(_DocDef::from_binary(b,e));
        }
    }
    DocRecord_Un(const DocRecord_Un &other)
        :previous_id(other.previous_id),has_value(other.has_value), erased(other.erased) {
        if (has_value) new(&content) DocType(other.content);
    }
    DocRecord_Un(DocRecord_Un &&other):previous_id(other.previous_id),has_value(other.has_value), erased(other.erased) {
        if (has_value) new(&content) DocType(std::move(other.content));
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
        return _DocDef::to_binary(type.content, iter);
    }

};


template<DocumentDef _DocDef = RowDocument>
class StorageView {
public:

    using DocType = typename _DocDef::Type;

    StorageView(const PDatabase &db, KeyspaceID kid, Direction dir, const PSnapshot &snap)
        :_db(db),_snap(snap), _kid(kid),_dir(dir) {}

    StorageView get_snapshot() {
        if (_snap) return *this;
        return StorageView(_db, _kid, _dir, _db->make_snapshot());
    }

    using DocRecord = DocRecordT<_DocDef>;


    ///Access directly to document
    /**
     * @param id id of document
     * @return Document proxy, which cotains document. This also reports whether the document
     * is found or not. To access the document, use -> or *. Note that you can access DocRecord
     * which is still proxy object through "document" variable
     *
     * @code
     * auto doc_proxy = _storage[id];
     * if (doc_proxy) { //found
     *     if (doc_proxy->document.has_value()) { //not deleted
     *          DocType doc = *doc_proxy->document
     *          //use doc
     *     } else {
     *        //found, but deleted
     *     }
     * }  else {
     *     //not found
     * }
     *
     */
    Document<DocRecordDef<_DocDef> > operator[](DocID id) const {
        return _db->get_as_document<Document<DocRecordDef<_DocDef> > >(RawKey(_kid, id));
    }

    struct IteratorValueType: DocRecord {
        DocID id = 0;
        IteratorValueType(std::string_view raw_key, std::string_view raw_value)
            :DocRecord(DocRecordDef<_DocDef>::from_binary(raw_value.begin(),raw_value.end()))
             {
                Key k ((RowView(raw_key)));
                id  = std::get<0>(k.get<DocID>());
             }
    };



    ///Iterator
    class RecordSet: public RecordSetBaseT<DocRecordDef<_DocDef> > {
    public:
        using RecordSetBaseT<DocRecordDef<_DocDef> >::RecordSetBaseT;

        using Iterator = RecordSetIterator<RecordSet, IteratorValueType>;

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
    RecordSet select_all(Direction dir=Direction::normal) const {
        if (isForward(changeDirection(_dir, dir))) {
            return RecordSet(_db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return RecordSet(_db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }

    ///Scan from given document for given direction
    RecordSet select_from(DocID start_pt, Direction dir = Direction::normal) const {
        if (isForward(changeDirection(_dir, dir))) {
            return RecordSet(_db->make_iterator(false,_snap),{
                    RawKey(_kid, start_pt),RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return RecordSet(_db->make_iterator(false,_snap),{
                    RawKey(_kid, start_pt),RawKey(_kid),
                    FirstRecord::included, LastRecord::excluded
            });
        }
    }

    ///Scan for range
    RecordSet select_range(DocID start_id, DocID end_id, LastRecord last_record = LastRecord::excluded) const {
        return RecordSet(_db->make_iterator(false,_snap),{
                RawKey(_kid, start_id),RawKey(_kid, end_id),
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


    const PDatabase &get_db() const {return _db;}

protected:
    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
    Direction _dir;

};


}




#endif /* SRC_DOCDB_STORAGE_VIEW_H_ */
