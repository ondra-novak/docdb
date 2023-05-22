#pragma once
#ifndef SRC_DOCDB_DOC_INDEX_UNIQUE_H_
#define SRC_DOCDB_DOC_INDEX_UNIQUE_H_


#include "map.h"
#include "doc_storage.h"


namespace docdb {

template<DocumentStorageType _DocStorage, DocumentDef _ValueDef=BasicRowDocument>
class DocumentUniqueIndex: public Map<_ValueDef> {
public:

    using DocType = typename _DocStorage::DocType;
    using DocID = typename _DocStorage::DocID;
    using DocInfo = typename _DocStorage::DocInfo;
    using ValueType = typename _ValueDef::Type;
    using Update = typename _DocStorage::Update;

    ///Observes changes of keys in the index;
    using UpdateObserver = std::function<bool(Batch &b, const BasicRowView  &)>;

    class Emit {
    public:
        Emit(ObserverList<UpdateObserver> &observers,
            Batch &batch,
            KeyspaceID kid,bool deleting)
            :_observers(observers)
            ,_batch(batch)
            ,_kid(kid),_deleting(deleting) {}

        void operator()(Key &k, const DocConstructType_t<_ValueDef> &v) {put(k,v);}
        void operator()(Key &&k, const DocConstructType_t<_ValueDef> &v) {put(k,v);}
        operator bool() const {return !_deleting;}

    protected:
        void put(Key &k, const ValueType &v) {
            k.change_kid(_kid);
            if (_deleting) {
                _batch.Delete(k);
            } else {
                auto &buffer = _batch.get_buffer();
                _ValueDef::to_binary(v, std::back_inserter(buffer));
                _batch.Put(k, buffer);
            }
            std::string_view ks(k);
            ks = ks.substr(sizeof(KeyspaceID), ks.length()-sizeof(KeyspaceID));
            _observers.call(_batch, ks);
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

    ///Register new observer
    /**
     * @param observer new observer to register (index)
     * @return id id of observer
     */
    std::size_t register_observer(UpdateObserver observer) {
        return _observers.register_observer(std::move(observer));
    }
    ///Unregister observer (by id)
    void unregister_observer(std::size_t id) {
        _observers.unregister_observer(id);
    }



    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentUniqueIndex(_DocStorage &storage, std::string_view name, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage,_ValueDef>(storage, name)
    {
        connect_indexer(std::forward<Fn>(indexer));
        check_reindex();
    }

    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentUniqueIndex(_DocStorage &storage, KeyspaceID kid, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage,_ValueDef>(storage, kid)
    {
        connect_indexer(std::forward<Fn>(indexer));
        check_reindex();
    }

    ~DocumentUniqueIndex() {
        this->_storage.unregister_observer(observer_id);
    }

    ///Checks, whether reindex is necessery. If does, starts reindexing
    void check_reindex() {
        if (check_revision()) return;
        reindex();
    }


    ///Reindex whole index
      void reindex() {
          this->_db->clear_table(this->_kid, false);
          this->_storage.replay_for(_indexer);
          update_revision();
      }

protected:

    int _rev;
    std::size_t observer_id;
    ObserverList<UpdateObserver> _observers;
    typename _DocStorage::UpdateObserver _indexer;

    bool check_revision() {
        std::string v;
        if (!this->_db->get(RawKey(this->_kid), v)) return false;
        BasicRowView row(v);
        auto [cur_rev] = row.get<int>();
        return (cur_rev == _rev);
    }
    void update_revision() {
        Batch b;
        b.Put(RawKey(this->_kid), BasicRow(_rev));
        this->_db->commit_batch(b);
    }

    template<typename Fn>
    void connect_indexer(Fn &&fn) {
        _indexer = [this, fn = std::move(fn)](Batch &b, const Update &update){
            if constexpr(std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>) {
                if (update.old_doc) {
                    Emit emt(_observers, b,  this->_kid, true);
                    fn(emt, *update.old_doc, {update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, this->_kid, false);
                    fn(emt, *update.new_doc, {update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(_observers, b, this->_kid, true);
                    fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, this->_kid, false);
                    fn(emt, *update.new_doc);
                }
            }
            return true;
        };
        observer_id = this->_storage.register_observer([this](Batch &b, const Update &update){
            return _indexer(b,update);
        });
    }


};



}



#endif /* SRC_DOCDB_DOC_INDEX_UNIQUE_H_ */
