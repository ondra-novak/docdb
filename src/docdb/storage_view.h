#pragma once
#ifndef SRC_DOCDB_STORAGE_VIEW_H_
#define SRC_DOCDB_STORAGE_VIEW_H_
#include "database.h"

namespace docdb {

///Type of document ID
using DocID = std::uint64_t;


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

    ///Contains a record read from the storage
    struct DocRecord {
        ///ID of document which has been replaced by this document (previous revision)
        DocID previous_id;
        ///Contains document itself. The value is not defined, if the ID contains a deleted document
        std::optional<DocType> content;
    };


    template<typename Iter>
    static bool is_deleted(Iter b, Iter e) {
        if (b == e) return true;
        if constexpr(DocumentCustomDeleted<_DocDef>) {
            return _DocDef::is_deleted(b, e);
        }
        return false;
    }

    struct DocRecordDef {
        using Type = DocRecord;
        template<typename Iter>
        static Type from_binary(Iter b, Iter e) {
            Type out;
            out.previous_id = Row::deserialize_item<DocID>(b, e);
            if (is_deleted(b, e)) return out;
            out.content.emplace(_DocDef::from_binary(b,e));
            return out;
        }
        template<typename Iter>
        static Iter to_binary(const Type &type, Iter iter) {
            Row::serialize_items(iter, type.previous_id);
            return _DocDef::to_binary(*type.content, iter);
        }
    };

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
    Document<DocRecordDef> operator[](DocID id) const {
        return _db->get_as_document<Document<DocRecordDef> >(RawKey(_kid, id));
    }

    struct IteratorValueType: DocRecord {
        DocID id = 0;
        IteratorValueType(std::string_view raw_key, std::string_view raw_value)
            :DocRecord(DocRecordDef::from_binary(raw_value.begin(),raw_value.end()))
             {
                Key k ((RowView(raw_key)));
                id  = std::get<0>(k.get<DocID>());
             }
    };



    ///Iterator
    class RecordSet: public RecordSetBase {
    public:
        using RecordSetBase::RecordSetBase;

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


    const PDatabase get_db() const {return _db;}

protected:
    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
    Direction _dir;

};


}




#endif /* SRC_DOCDB_STORAGE_VIEW_H_ */
