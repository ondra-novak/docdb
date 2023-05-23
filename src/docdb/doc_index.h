#pragma once
#ifndef SRC_DOCDB_DOC_INDEX_H_
#define SRC_DOCDB_DOC_INDEX_H_

#include "database.h"
#include "serialize.h"
#include "map.h"
#include "keylock.h"

#include "doc_storage_concept.h"

namespace docdb {

enum class IndexType {
    ///unique index
    /**
     * Unique index.
     * It expects, that there is no duplicate keys, but
     * it cannot enforce this. In case of failure of this rule,
     * key is overwritten, so old document is dereferenced.
     *
     * This index is the simplest and the most efficient
     */
    unique,
    ///Unique index with enforcement
    /**
     * Checks, whether keys are really unique. In case of failure of this
     * rule, the transaction is rejected and exception is thrown
     *
     * This index can be slow, because it needs to search the database
     * for every incoming record
     */
    unique_enforced,

    ///Unique index with enforcement but expect single thread insert
    /**
     * This is little faster index in compare to unique_enforced, as it
     * expects that only one thread is inserting. Failing this requirement
     * can cause that duplicate key can be inserted during parallel insert (
     * as the conflicting key is still in transaction, not in the database
     * itself). This index type doesn't check for keys in transaction,
     * so the implementation is less complicated
     */
    unique_enforced_single_thread,

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




template<DocumentStorageViewType _DocStorage, DocumentDef _ValueDef = RowDocument, IndexType index_type = IndexType::multi>
class DocumentIndexView: public ViewBase {
public:

    using DocType = typename _DocStorage::DocType;
    using DocID = typename _DocStorage::DocID;
    using DocInfo = typename _DocStorage::DocInfo;

    using ValueType = typename _ValueDef::Type;


    class Iterator: public GenIterator<_ValueDef> {
    public:
        Iterator(_DocStorage &stor,
                std::unique_ptr<leveldb::Iterator> &&iter,
                typename GenIterator<_ValueDef>::Config &&cfg)
        :GenIterator<_ValueDef>(std::move(iter), cfg),_storage(stor) {}


        ///Retrieve associated document's id
        /** This operation is defined for IndexType::multi */
        template<typename ... X>
        DocID id(X &&...) const {
            static_assert(index_type == IndexType::multi || defer_false<X...>, "Operation is not defined for this index type");
            std::string_view key = this->raw_key();
            auto x = key.substr(key.length()-sizeof(DocID));
            auto [id] = Row::extract<DocID>(x);
            return id;
        }
        ///retrieve associated document
        /**
         * @return returns DocInfo, which is defined on DocumentStorageView<>::DocInfo
         * @note You need always check available flag before you retrieve the document itself
         */
        DocInfo doc() const {
            return _storage[id()];
        }

    protected:
        _DocStorage &_storage;
    };

    DocumentIndexView(_DocStorage &storage, std::string_view name, Direction dir = Direction::forward, const PSnapshot snap = {})
        :ViewBase(storage.get_db(),name, dir, snap)
        ,_storage(storage) {}

    DocumentIndexView(_DocStorage &storage, KeyspaceID kid, Direction dir = Direction::forward, const PSnapshot snap = {})
        :ViewBase(storage.get_db(),kid, dir, snap)
        ,_storage(storage) {}

    DocumentIndexView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return DocumentIndexView(_storage, _kid, _dir, _db->make_snapshot());
    }

    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> get(Key &key) const {
        key.change_kid(_kid);
        return _db->get_as_document< Document<_ValueDef> >(key, _snap);
    }
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    Document<_ValueDef> operator[](Key &&key) const {return get(key);}



    ///Lookup for single key
    /**
     * @param key key to find
     * @return Iterator - as the index allows multiple values for single key, you can retrieve multiple results
     */
    Iterator lookup(Key &&key) const {return lookup(key);}
    ///Lookup for single key
    /**
     * @param key key to find
     * @return Iterator - as the index allows multiple values for single key, you can retrieve multiple results
     */
    Iterator lookup(Key &key) const {
        return scan_prefix(key, Direction::normal);
    }


    Iterator scan(Direction dir = Direction::normal)const  {
        if (isForward(_dir)) {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    RawKey(_kid),RawKey(_kid+1),
                    FirstRecord::excluded, LastRecord::excluded
            });
        } else {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::excluded
            });
        }
    }

    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key,dir);}
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    key,RawKey(_kid+1),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            Key pfx = key.prefix_end();
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    pfx,RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
            });
        }
    }
    Iterator scan_prefix(Key &&key, Direction dir = Direction::normal) const {return scan_prefix(key,dir);}
    Iterator scan_prefix(Key &key, Direction dir = Direction::normal) const {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    key,key.prefix_end(),
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    key.prefix_end(),key,
                    FirstRecord::excluded, LastRecord::included
            });
        }

    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded)const  {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) const {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) const {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded)const  {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            if (last_record == LastRecord::included) {
                Key pfx = from.prefix_end();
                return Iterator(_storage,_db->make_iterator(false,_snap),{
                        from,pfx,
                        FirstRecord::included, LastRecord::excluded
                });
            } else {
                return Iterator(_storage,_db->make_iterator(false,_snap),{
                        from,to,
                        FirstRecord::included, LastRecord::excluded
                });
            }

        } else {
            Key xfrom = from.prefix_end();
            if (last_record == LastRecord::included) {
                return Iterator(_storage,_db->make_iterator(false,_snap),{
                        xfrom,to,
                        FirstRecord::excluded, LastRecord::included
                });
            } else {
                Key xto = to.prefix_end();
                return Iterator(_storage,_db->make_iterator(false,_snap),{
                        xfrom,xto,
                        FirstRecord::excluded, LastRecord::included
                });
            }
        }
    }

protected:
    _DocStorage &_storage;
};

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


}

#endif /* SRC_DOCDB_DOC_INDEX_H_ */


