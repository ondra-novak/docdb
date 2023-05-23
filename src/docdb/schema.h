#pragma once
#ifndef SRC_DOCDB_SCHEMA_H_
#define SRC_DOCDB_SCHEMA_H_

#include "batch.h"
#include "database.h"

#include <tuple>
#include "concepts.h"


namespace docdb {


template<IsTuple TupleHandlers, typename ... Args>
class ChainExecutor {
public:
    constexpr ChainExecutor(std::string_view name, TupleHandlers handlers):_name(name),_handlers(handlers) {}

    template<IsTuple Instances>
    constexpr void operator()(Instances &inst, Args ... args) const {
        static_assert(std::tuple_size_v<Instances> == std::tuple_size_v<TupleHandlers> );
        auto call_each = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
             (std::get<Is>(_handlers)(std::get<Is>(inst), args ...), ...);
        };
        call_each(std::make_index_sequence<std::tuple_size_v<TupleHandlers> >());
    }


protected:

    template<typename ... InitArgs>
    constexpr auto init_handlers(InitArgs && ... args) const {
        auto call_each = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
            return std::make_tuple(std::get<Is>(_handlers).connect(std::forward<InitArgs>(args) ...)...);
        };
        return call_each(std::make_index_sequence<std::tuple_size_v<TupleHandlers>>());
    }

    std::string_view _name;
    TupleHandlers _handlers;
};

using DocID = std::uint64_t;


template<DocumentDef _DocDef>
class StorageSchemaBase {
public:
    struct Update {
        using DocType = typename _DocDef::Type;
        const DocType *new_doc;
        const DocType *old_doc;
        DocID new_doc_id;
        DocID old_doc_id;
        DocID old_old_doc_id;
    };
};

template<typename _DocDef>
class StorageView: public ViewBase {
public:

    using ViewBase::ViewBase;

   StorageView make_snapshot() const {
       if (_snap != nullptr) return *this;
       return StorageView(_db, _kid, _dir, _db->make_snapshot());
   }
   using DocType = typename _DocDef::Type;
   using DocID = ::docdb::DocID;;

   class DocInfo: public Document<_DocDef, std::string_view> {
   public:
       using Document<_DocDef, std::string_view>::Document;

       template<typename Rtv>
       CXX20_REQUIRES(std::same_as<decltype(std::declval<Rtv>()(std::declval<std::string &>())), bool>)
       DocInfo(Rtv &&fn) {
           this->_found = fn(_s);
           if (this->_found) {
               auto [p,d] = Row::extract<DocID, Blob>(_s);
               this->_buff = d;
               _prev_id = p;
           }
       }

       bool deleted() const {return this->_buff.empty();}

       DocID prev_id() const {return _prev_id;}


   protected:
       DocID _prev_id;
       std::string _s;
   };



   ///Retrieve document under given id
   DocInfo get(DocID id) const {
       return _db->get_as_document<DocInfo>(RawKey(_kid,id),this->_snap);
   }

   ///Operator[]
   DocInfo operator[](DocID id) const {
       return get(id);
   }

   struct _IterHelper {
       using Type = std::optional<DocType>;
       template<typename Iter>
       static std::optional<DocType> from_binary(Iter beg, Iter end) {
           std::advance(beg, sizeof(DocID));
           if (beg == end) return {};
           return _DocDef::from_binary(beg, end);
       }
       template<typename Iter>
       static Iter to_binary(const Type &, Iter x) {return x;}
   };

   ///Iterator
   class Iterator: public GenIterator<_IterHelper>{
   public:
       using GenIterator<_IterHelper>::GenIterator;

       ///Retrieve document id
       DocID id() const {
           Key k = this->key();
           auto [id] = k.get<DocID>();
           return id;
       }

       ///retrieve id of replaced document
       DocID prev_id() const {

           auto [id] = Row::extract<DocID>(this->raw_value());
           return id;
       }

       auto doc() const {return this->value();}

   };

   ///Scan whole storage
   Iterator scan(Direction dir=Direction::normal) const {
       if (isForward(changeDirection(_dir, dir))) {
           return Iterator(_db->make_iterator(false,_snap),{
                   RawKey(_kid),RawKey(_kid+1),
                   FirstRecord::included, LastRecord::excluded
           });
       } else {
           return Iterator(_db->make_iterator(false,_snap),{
                   RawKey(_kid+1),RawKey(_kid),
                   FirstRecord::excluded, LastRecord::included
           });
       }
   }

   ///Scan from given document for given direction
   Iterator scan_from(DocID start_pt, Direction dir = Direction::normal) const {
       if (isForward(changeDirection(_dir, dir))) {
           return Iterator(_db->make_iterator(false,_snap),{
                   RawKey(_kid, start_pt),RawKey(_kid+1),
                   FirstRecord::included, LastRecord::excluded
           });
       } else {
           return Iterator(_db->make_iterator(false,_snap),{
                   RawKey(_kid, start_pt),RawKey(_kid),
                   FirstRecord::included, LastRecord::excluded
           });
       }
   }

   ///Scan for range
   Iterator scan_range(DocID start_id, DocID end_id, LastRecord last_record = LastRecord::excluded) const {
       return Iterator(_db->make_iterator(false,_snap),{
               RawKey(_kid, start_id),RawKey(_kid, end_id),
               FirstRecord::included, LastRecord::excluded
       });
   }

   ///Retrieve ID of last document
   /**
    * @return id of last document, or zero if database is empty
    */
   DocID get_last_document_id() const {
       auto iter = scan(Direction::backward);
       if (iter.next()) return iter.id();
       else return 0;
   }


protected:

};


template<DocumentDef _DocDef, IsTuple _TupleHandlers>
class StorageSchema: public StorageSchemaBase<_DocDef>,
                     public ChainExecutor<_TupleHandlers, Batch &, const typename StorageSchemaBase<_DocDef>::Update &> {
public:

    using Update = typename StorageSchemaBase<_DocDef>::Update;
    using DocType = typename _DocDef::Type;

    using ChainExecutor<_TupleHandlers, Batch &, const Update &>::ChainExecutor;

    template<typename Inst>
    class Instance: public StorageView<_DocDef> {
    public:
        Instance(const PDatabase &db, KeyspaceID kid, const StorageSchema &schema, Inst &&inst)
            :StorageView<_DocDef>(db, kid),_schema(schema), _handlers(std::move(inst)) {}

        DocID put(const DocType &doc, DocID update_id = 0) {
            Batch b;
            DocID id = write(b, &doc, update_id);
            this->_db->commit_batch(b);
            return id;
        }
        DocID put(Batch &b, const DocType &doc, DocID update_id = 0) {
            return write(b, &doc, update_id);
        }

        DocID erase(DocID id) {
            Batch b;
            DocID ret = write(b, nullptr, id);
            this->_db->commit_batch(b);
            return ret;
        }

        DocID erase(Batch &b, DocID id) {
            return write(b, nullptr, id);
        }


    protected:

        DocID write(Batch &b, const DocType *doc, DocID update_id) {
            DocID id = _next_id.fetch_add(1, std::memory_order_relaxed);
            auto buff = b.get_buffer();
            auto iter = std::back_inserter(buff);
            Row::serialize_items(iter, update_id);
            if (doc) {
                _DocDef::to_binary(*doc, iter);
            }
            b.Put(RawKey(this->_kid, id), buff);
            if (update_id) {
                std::string tmp;
                if (this->_db->get(RawKey(this->_kid, update_id), tmp)) {
                    Row rw((RowView(tmp)));
                    auto [old_old_doc_id, bin] = rw.get<DocID, Blob>();
                    if (!bin.empty()) {
                        auto old_doc = _DocDef::from_binary(bin.begin(), bin.end());
                        _schema(_handlers, b, Update {
                           doc,&old_doc,id,update_id,old_old_doc_id
                        });
                        return id;
                    }
                    _schema(_handlers, b, Update{
                            doc,nullptr,id,update_id,old_old_doc_id
                    });
                    return id;
                }
            }
            _schema(_handlers, b, Update{
                    doc,nullptr,id,update_id,0
            });
            return id;

        }

        const StorageSchema &_schema;
        Inst _handlers;
        std::atomic<DocID> _next_id = 1;
    };

    auto connect(const PDatabase &db) const {
        KeyspaceID kid = db->open_table(this->_name);
        auto inst = this->init_handlers(db);
        return Instance<decltype(inst)>(db, kid, *this, std::move(inst));
    }

};

template<auto x> struct SchemaType;
template<typename T, T *x> struct SchemaType<x> {
    using Type = decltype(x->connect(std::declval<PDatabase>()));
};

template<auto x> using SchemaType_t = typename SchemaType<x>::Type;



template<DocumentDef _DocDef, typename ... Handlers>
constexpr auto create_storage(std::string_view name, Handlers ... handlers) {
    using Tup = decltype(std::make_tuple(handlers...));
    return StorageSchema<_DocDef, std::tuple<Handlers...>>(name, Tup(handlers...));
}

template<DocumentDef _DocDef>
class IndexerSchema {
public:

    constexpr IndexerSchema(std::string_view name):_name(name) {}

    class Instance {
    public:
        Instance(const PDatabase &db, KeyspaceID kid,const IndexerSchema &schema)
            :_schema(schema), _db(db), _kid(kid) {}
    protected:
        const IndexerSchema &_schema;
        PDatabase _db;
        KeyspaceID _kid;
    };

    using Update = typename StorageSchemaBase<_DocDef>::Update;

    void operator()(Instance &inst, Batch &b, const Update &up) const {
        std::cout << "Index:" << up.new_doc_id << std::endl;
    }

    Instance connect(const PDatabase &db) const {
        return Instance(db, db->open_table(this->_name), *this);
    }

    std::string_view _name;
};

template<DocumentDef _DocDef>
constexpr auto create_index(std::string_view name) {
    return IndexerSchema<_DocDef>(name);
}



}




#endif /* SRC_DOCDB_SCHEMA_H_ */
