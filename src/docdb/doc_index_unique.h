#pragma once
#ifndef SRC_DOCDB_DOC_INDEX_UNIQUE_H_
#define SRC_DOCDB_DOC_INDEX_UNIQUE_H_


#include "map.h"
#include "doc_storage.h"


namespace docdb {

template<DocumentDef _ValueDef, DocumentStorageType _DocStorage>
class DocumentUniqueIndex: public Map<_ValueDef> {
public:

    using DocType = typename _DocStorage::DocType;
    using DocID = typename _DocStorage::DocID;
    using DocInfo = typename _DocStorage::DocInfo;
    using ValueType = typename _ValueDef::Type;
    using Update = typename _DocStorage::Update;

    ///Observes changes of keys in the index;
    using UpdateObserver = Observer<bool(Batch &b, const BasicRowView  &)>;

    class Emit {
    public:
        Emit(ObserverList<UpdateObserver> &observers,
            Batch &batch,
            KeyspaceID kid,bool deleting)
            :_observers(observers)
            ,_batch(batch)
            ,_kid(kid),_deleting(deleting) {}

        void operator()(Key &k, const ValueType &v) {put(k,v);}
        void operator()(Key &&k, const ValueType &v) {put(k,v);}
        operator bool() const {return !_deleting;}

    protected:
        void put(Key &k, const ValueType &v) {
            std::string_view ks(k);
            k.change_kid(_kid);
            if (_deleting) {
                _batch.Delete(k);
            } else {
                auto &buffer = _batch.get_buffer();
                _ValueDef::to_binary(v, std::back_inserter(buffer));
                _batch.Put(k, buffer);
            }
            _observers.call(_batch, BasicRowView(ks));
        }

        ObserverList<UpdateObserver> &_observers;
        Batch &_batch;
        KeyspaceID _kid;
        bool _deleting;

        friend class DocumentUniqueIndex;
    };

    struct DocMetadata {
        ///Document id
        DocID id;
        ///Previous document id (if replacing existing one)
        DocID prev_id;
        ///true, if document being deleted
        bool deleteing;
    };




    class AbstractIndexer {
    public:
        virtual void update(Batch &b, const Update &update) = 0;
        virtual void register_observer(UpdateObserver &&observer) = 0;
        virtual bool check_revision() = 0;
        virtual void update_revision() = 0;
        virtual ~AbstractIndexer() = default;
    };

    void register_observer(UpdateObserver &&observer) {
        _ptr->register_observer(std::move(observer));
    }



    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentUniqueIndex(_DocStorage &storage, std::string_view name, int revision, Fn &&indexer)
        :DocumentIndexView<_ValueDef, _DocStorage>(storage, name)
    {
        _ptr = std::make_shared<Indexer<Fn> >(this->_db, this->_kid, std::forward<Fn>(indexer), revision);
        check_reindex();
        register_storage_observer();
    }

    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentUniqueIndex(_DocStorage &storage, KeyspaceID kid, int revision, Fn &&indexer)
        :DocumentIndexView<_ValueDef, _DocStorage>(storage, kid)
    {
        _ptr = std::make_shared<Indexer<Fn> >(this->_db, this->_kid, std::forward<Fn>(indexer), revision);
        check_reindex();
        register_storage_observer();
    }

    ///Checks, whether reindex is necessery. If does, starts reindexing
    void check_reindex() {
        if (_ptr->check_revision()) return;
        reindex();
    }


    ///Reindex whole index
    void reindex() {
        this->_db->clear_table(this->_kid);
        auto iter = this->_storage.scan();
        Batch b;
        while (iter.next()) {
            DocType doc = iter.doc();
            DocID id = iter.id();
            DocID prev_id = iter.prev_id();
            if (prev_id) {
               DocInfo dinfo = this->_storage[prev_id];
               if (dinfo.available) {
                   DocType old_doc = dinfo.doc();
                   _ptr->update(b, Update {&old_doc,&doc,dinfo.prev_id,prev_id,id});
                   this->_db->commit_batch(b);
                   continue;
               }
            }
            _ptr->update(b, Update {nullptr,&doc,0,prev_id,id});
            this->_db->commit_batch(b);
        }
    }

protected:

    void register_storage_observer() {
        this->_storage.register_observer([wk = std::weak_ptr<AbstractIndexer>(_ptr)](Batch &b, const Update &update){
            auto lk = wk.lock();
            if (lk) {
                lk->update(b,update);
                return true;
            } else {
                return false;
            }
        });
    }

    template<typename Fn>
    class Indexer: public AbstractIndexer{
    public:
        Indexer(const PDatabase &db, KeyspaceID kid, Fn &&fn, int rev)
            :_db(db),_kid(kid),_fn(std::forward<Fn>(fn)), _rev(rev) {
        }

        virtual void update(Batch &b, const Update &update){
            if constexpr(std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>) {
                if (update.old_doc) {
                    Emit emt(_observers, b, _kid, true);
                    _fn(emt, *update.old_doc, {update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, _kid, false);
                    _fn(emt, *update.new_doc, {update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(_observers, b, _kid, true);
                    _fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, _kid, false);
                    _fn(emt, *update.new_doc);
                }

            }
        }


        virtual void register_observer(UpdateObserver &&observer) {
            _observers.register_observer(std::move(observer));
        }

        virtual bool check_revision() {
            std::string v;
            if (!_db->get(RawKey(_kid), v)) return false;
            BasicRowView row(v);
            auto [cur_rev] = row.get<int>();
            return (cur_rev == _rev);
        }
        virtual void update_revision() {
            Batch b;
            b.Put(RawKey(_kid), BasicRow(_rev));
            _db->commit_batch(b);
        }


    protected:
        PDatabase _db;
        KeyspaceID _kid;
        Fn _fn;
        int _rev;
        ObserverList<UpdateObserver> _observers;
    };

    std::shared_ptr<AbstractIndexer> _ptr;


};



}



#endif /* SRC_DOCDB_DOC_INDEX_UNIQUE_H_ */
