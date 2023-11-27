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


///Contains record with a document as it is stored in the storage
/**
 * A document stored in main storage is stored along with previous id. It can be
 * also deleted or without a value
 *
 * This object contains all above informations.
 *
 * @tparam _DocDef document definition structure
 */
template<typename _DocDef>
struct DocRecord {

    ///Contains type of document
    using DocType =  typename _DocDef::Type;

    union {
        ///Contains document itself. This field is only valid if has_value is true
        DocType document;
    };
    ///Contains previous document id
    DocID previous_id = 0;
    ///is set to true, when document is deleted
    /** Document is either not present (just empty record stored) or it is flagged
     * as deleted. You need to check `has_value` to determine state
     */
    bool deleted;
    ///is set to true, if document is present
    /** It is possible to have a document which is marked as deleted, however
     * this document still can contain important information about deletion.
     * In this case, deleted = true and has_value = true.
     */
    bool has_value;

    DocRecord(DocID previd):previous_id(previd),deleted(true),has_value(false) {}
    template<typename Iter>
    DocRecord(Iter &b, Iter e) {
        previous_id = Row::deserialize_item<DocID>(b, e);
        if (b == e) {
            deleted =true;
            has_value = false;
        } else {
            new(&document) DocType(_DocDef::from_binary(b,e));
            deleted = document_is_deleted<_DocDef>(document);
            has_value =true;
        }

    }
    template<typename Iter>
    DocRecord(Iter &&b, Iter e):DocRecord(b,e) {}

    DocRecord(const DocRecord &other)
        :previous_id(other.previous_id)
        ,deleted(other.deleted)
        ,has_value(other.has_value) {
        if (has_value) new(&document) DocType(other.document);
    }

    DocRecord &operator=(const DocRecord &other) {
        if (this != &other) {
            if (has_value) document.~DocType();
            previous_id = other.previous_id;
            deleted = other.deleted;
            has_value = other.has_value;
            if (has_value) new(&document) DocType(other.document);
        }
        return *this;
    }
    DocRecord(DocRecord &&other)
        :previous_id(other.previous_id)
        ,deleted(other.deleted)
        ,has_value(other.has_value) {
        if (has_value) {
            new(&document) DocType(std::move(other.document));
            other.document.~DocType();
            other.has_value = false;
        }
    }

    DocRecord &operator=(DocRecord &&other) {
        if (this != &other) {
            if (has_value) document.~DocType();
            previous_id = other.previous_id;
            deleted = other.deleted;
            has_value = other.has_value;
            if (has_value) {
                new(&document) DocType(std::move(other.document));
                other.document.~DocType();
                other.has_value = false;

            }
        }
        return *this;
    }

    ~DocRecord() {
        if (has_value) document.~DocType();
    }
};



template<typename _DocDef>
struct DocRecordDef {
    using Type = DocRecord<_DocDef>;
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


    using DocRecord = docdb::DocRecord<_DocDef>;


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

    ///Get document from the storage
    /**
     * @param docId document id
     * @return optional value. If the document exists, returns it, otherwise
     * it returns empty value. Note that function can return documents
     * marked as deleted, if they are present. If document record exists,
     * but doesn't contain a document, the function returns empty value.
     *
     * @note to more detailed result, use find(docId)
     */
    std::optional<DocType> get(DocID docId) const {
        auto v = this->_db->get(RawKey(this->_kid, docId));
        if (v) {
            auto iter = v->begin();
            auto end = v->end();
            Row::deserialize_item<DocID>(iter, end);
            if (iter != end) {
                return EmplaceByReturn([&]{
                    return _DocDef::from_binary(iter,end);
                });
            }
        }
        return {};
    }

};


}




#endif /* SRC_DOCDB_STORAGE_VIEW_H_ */
