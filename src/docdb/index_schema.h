#pragma once
#ifndef SRC_DOCDB_INDEX_SCHEMA_H_
#define SRC_DOCDB_INDEX_SCHEMA_H_

#include "schema.h"
#include "index_view.h"

namespace docdb {

template<DocumentDef _DocDef, typename Fn>
class IndexerSchemaBase {
public:

    constexpr IndexerSchemaBase(Fn fn)
        :_fn(fn) {}

protected:
    Fn _fn;

};


struct DocMetadata {
    ///Document id
    DocID id;
    ///Previous document id (if replacing existing one)
    DocID prev_id;
    ///true, if document being deleted
    bool deleting;
};



template<DocumentDef _DocDef, typename Fn, typename _TupleHandlers, IndexType index_type>
class IndexerSchema:public IndexerSchemaBase<_DocDef, Fn>,
                    public ChainExecutor<_TupleHandlers, Batch &, const Key &> {
public:


    using Super = ChainExecutor<_TupleHandlers, Batch &, const Key &>;
    using ValueType = typename _DocDef::Type;

    constexpr IndexerSchema(Fn fn, std::string_view name, _TupleHandlers handlers)
        :IndexerSchemaBase<_DocDef, Fn>(fn),Super(name, handlers) {}

    template<typename Inst>
    class Instance: public IndexView<_DocDef, index_type> {
    public:
        Instance(const PDatabase &db, KeyspaceID kid,const IndexerSchema &schema, Inst &&inst)
            :IndexView<_DocDef, index_type>(db, kid)
            ,_schema(schema),_inst(std::move(inst)) {}


        class Emit {
        public:
            Emit(Instance &owner, Batch &b, DocID cur_doc, bool deleting)
                :_owner(owner), _batch(b), _cur_doc(cur_doc), _deleting(deleting) {}


            void operator()(Key &k, const ValueType &v) {put(k,v);}
            void operator()(Key &&k, const ValueType &v) {put(k,v);}
            operator bool() const {return !_deleting;}

        protected:
            Instance &_owner;
            Batch &_batch;
            DocID _cur_doc;
            bool _deleting;

            void put(Key &k, const ValueType &v) {
                k.change_kid(_owner._kid);
                if constexpr(index_type == IndexType::multi) {
                    k.append(_cur_doc);
                }
                if (_deleting) {
                    _batch.Delete(k);
                } else {
                    /*if constexpr(index_type == IndexType::unique_enforced) {
                        _lks.push_back(_owner.lock_key(k));
                    } else*/ if constexpr(index_type == IndexType::unique_enforced_single_thread) {
                        std::string dummy;
                        if (_owner._db->get(k, dummy)) throw std::runtime_error("Conflict (unique index)");
                    }

                    auto &buffer = _batch.get_buffer();
                    _DocDef::to_binary(v, std::back_inserter(buffer));
                    _batch.Put(k, buffer);
                }
                _owner._schema.send(_owner._inst, _batch, Key(RowView(k)));
            }


            friend class DocumentIndex;
        };


        template<typename Update>
        bool do_index(Batch &b, const Update &update) {
            using DocType = decltype(*update.new_doc);
//            KeyLocks lks;
            if constexpr(std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>) {
                if (update.old_doc) {
                    Emit emt(*this, b,/* lks,*/ update.old_doc_id, true);
                    _schema._fn(emt, *update.old_doc, DocMetadata{update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(*this, b,/* lks,*/ update.new_doc_id, false);
                    _schema._fn(emt, *update.new_doc, DocMetadata{update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(*this, b,/* lks,*/ update.old_doc_id, true);
                    _schema._fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(*this, b,/* lks,*/ update.new_doc_id, false);
                    _schema._fn(emt, *update.new_doc);
                }
            }
  /*          if constexpr(index_type == IndexType::unique_enforced) {
                Unlocker *unlk = new Unlocker(_owner, std::move(lks));
                b.add_hook(Batch::Hook::member_fn<&Unlocker::unlock>(unlk));
            }*/
            return true;
        }


    protected:
        const IndexerSchema &_schema;
        Inst _inst;

    };

    template<typename Inst, typename Update>
    void operator()(Instance<Inst> &inst, Batch &b, const Update &up) const {
        inst.do_index(b, up);
    }

    auto connect(const PDatabase &db) const {
        KeyspaceID kid = db->open_table(this->_name);
        auto inst = this->init_handlers(db);
        return Instance<decltype(inst)>(db, kid, *this, std::move(inst));
    }


};

template<DocumentDef _DocDef, IndexType index_type = IndexType::multi, typename Fn, typename ... Handlers>
constexpr auto create_index(std::string_view name, Fn fn, Handlers ... handlers) {
    using Tup = decltype(std::make_tuple(handlers...));
    return IndexerSchema<_DocDef, Fn, std::tuple<Handlers...>, index_type>(fn, name, Tup(handlers...));
}


#if 0

template<DocumentStorageType _DocStorage, DocumentDef _ValueDef = RowDocument, IndexType index_type = IndexType::multi>
class DocumentIndex: public DocumentIndexView<_DocStorage, _ValueDef, index_type> {
public:

    using DocType = typename _DocStorage::DocType;
    using DocID = typename _DocStorage::DocID;
    using DocInfo = typename _DocStorage::DocInfo;
    using ValueType = typename _ValueDef::Type;
    using Update = typename _DocStorage::Update;

    ///Observes changes of keys in the index;
    using UpdateObserver = SimpleFunction<bool, Batch &, const Key &>;


    using KeyLocks = std::conditional_t<index_type == IndexType::unique_enforced,
            std::vector<KeyHolder>, std::monostate>;

    class Emit {
    public:
        Emit(DocumentIndex &owner, Batch &b, KeyLocks &lks, DocID cur_doc, bool deleting)
            :_owner(owner), _batch(b), _lks(lks),_cur_doc(cur_doc), _deleting(deleting) {}

        void operator()(Key &k, const DocConstructType_t<_ValueDef> &v) {put(k,v);}
        void operator()(Key &&k, const DocConstructType_t<_ValueDef> &v) {put(k,v);}
        operator bool() const {return !_deleting;}

    protected:
        DocumentIndex &_owner;
        Batch &_batch;
        KeyLocks &_lks;
        DocID _cur_doc;
        bool _deleting;

        void put(Key &k, const ValueType &v) {
            k.change_kid(_owner._kid);
            if constexpr(index_type == IndexType::multi) {
                k.append(_cur_doc);
            }
            if (_deleting) {
                _batch.Delete(k);
            } else {
                if constexpr(index_type == IndexType::unique_enforced) {
                    _lks.push_back(_owner.lock_key(k));
                } else if constexpr(index_type == IndexType::unique_enforced_single_thread) {
                    std::string dummy;
                    if (_owner._db->get(k, dummy)) throw std::runtime_error("Conflict (unique index)");
                }

                auto &buffer = _batch.get_buffer();
                _ValueDef::to_binary(v, std::back_inserter(buffer));
                _batch.Put(k, buffer);
            }
            _owner._observers.call(_batch, Key(RowView(k)));
        }


        friend class DocumentIndex;
    };

    struct DocMetadata {
        ///Document id
        DocID id;
        ///Previous document id (if replacing existing one)
        DocID prev_id;
        ///true, if document being deleted
        bool deleting;
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
    DocumentIndex(_DocStorage &storage, std::string_view name, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage, _ValueDef, index_type>(storage, name)
        ,_rev(revision)
    {
        init_observer(std::forward<Fn>(indexer));
        check_reindex();
    }

    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentIndex(_DocStorage &storage, KeyspaceID kid, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage,_ValueDef, index_type>(storage, kid)
         ,_rev(revision)
    {
        init_observer(std::forward<Fn>(indexer));
        check_reindex();
    }

    ~DocumentIndex() {
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
        this->_storage.rescan_for(_indexer->create_observer());
        update_revision();
    }

    void rescan_for(const UpdateObserver &observer) {

        Batch b;
        auto iter = this->scan();
        while (iter.next()) {
            if (b.is_big()) {
                this->_db->commit_batch(b);
            }
            auto k = iter.raw_key();
            k = k.substr(sizeof(KeyspaceID), k.size() - sizeof(KeyspaceID) - sizeof(DocID));
            if (!observer(b, Key(RowView(k)))) break;
        }
        this->_db->commit_batch(b);
    }

protected:

    using StorageObserver = typename _DocStorage::UpdateObserver;

    class Indexer {
    public:
        virtual StorageObserver create_observer() = 0;
        virtual ~Indexer() = default;
    };

    int _rev;
    std::size_t observer_id;
    ObserverList<UpdateObserver> _observers;
    std::unique_ptr<Indexer> _indexer;

    using LockKey = std::conditional_t<index_type == IndexType::unique_enforced, KeyLock, std::monostate>;

    LockKey _lock_key;

    class Unlocker {
    public:
        Unlocker(DocumentIndex &owner, KeyLocks &&lk)
            :_owner(owner), _lk(std::move(lk)) {}

        void unlock(bool) {
            _owner.unlock_keys(_lk);
            delete this;
        }

    protected:
        DocumentIndex &_owner;
        KeyLocks _lk;
    };

    template<typename Fn>
    class Indexer_Fn: public Indexer {
    public:
        Indexer_Fn(DocumentIndex &owner, Fn &&fn):_owner(owner),_fn(std::forward<Fn>(fn)) {}

        bool run(Batch &b, const Update &update) {
            KeyLocks lks;
            if constexpr(std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>) {
                if (update.old_doc) {
                    Emit emt(_owner, b, lks, update.old_doc_id, true);
                    _fn(emt, *update.old_doc, {update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(_owner, b, lks, update.new_doc_id, false);
                    _fn(emt, *update.new_doc, {update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(_owner, b, lks, update.old_doc_id, true);
                    _fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(_owner, b, lks, update.new_doc_id, false);
                    _fn(emt, *update.new_doc);
                }
            }
            if constexpr(index_type == IndexType::unique_enforced) {
                Unlocker *unlk = new Unlocker(_owner, std::move(lks));
                b.add_hook(Batch::Hook::member_fn<&Unlocker::unlock>(unlk));
            }
            return true;
        }
        virtual StorageObserver create_observer() {
            return StorageObserver::template member_fn<&Indexer_Fn::run>(this);
        }
    protected:
        DocumentIndex &_owner;
        Fn _fn;
    };


    template<typename Fn>
    void init_observer(Fn &&fn) {
        _indexer = std::make_unique<Indexer_Fn<Fn> >(*this, std::forward<Fn>(fn));
        observer_id = this->_storage.register_observer(_indexer->create_observer());
    }

    bool check_revision() {
        auto d=this->_db->template get_as_document<Document<RowDocument> >(
                                   Database::get_private_area_key(this->_kid));
        if (d) {
             auto [cur_rev] = (*d).template get<int>();
             return (cur_rev == _rev);
        }
        return false;
    }
    void update_revision() {
        Batch b;
        b.Put(Database::get_private_area_key(this->_kid), Row(_rev));
        this->_db->commit_batch(b);
    }

    KeyHolder lock_key(const std::string_view &key) {
       if constexpr(index_type == IndexType::unique_enforced) {
           std::string dummy;
           if (!this->_db->get(key, dummy)) {
               KeyHolder hld(key);
               if (_lock_key.lock_key(hld)) {
                   return hld;
               }
           }
           throw std::runtime_error("Conflict (duplicate key on unique index)");
       } else {
           throw std::runtime_error("Unreachable code");
       }
    }

    void unlock_keys(const KeyLocks &keys) {
        if constexpr(index_type == IndexType::unique_enforced) {
            _lock_key.unlock_keys(keys.begin(), keys.end());
        }
    }




};

#endif

}




#endif /* SRC_DOCDB_INDEX_SCHEMA_H_ */
