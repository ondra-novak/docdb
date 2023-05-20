#pragma once
#ifndef SRC_DOCDB_DOC_INDEX_H_
#define SRC_DOCDB_DOC_INDEX_H_

#include "database.h"
#include "serialize.h"
#include "map.h"

#include "doc_storage_concept.h"

namespace docdb {

template<DocumentStorageViewType _DocStorage, DocumentDef _ValueDef = BasicRowDocument>
class DocumentIndexView: public ViewBase {
public:

    using DocType = typename _DocStorage::DocType;
    using DocID = typename _DocStorage::DocID;
    using DocInfo = typename _DocStorage::DocInfo;

    using ValueType = typename _ValueDef::Type;

    ///Information retrieved from database
    class RowInfo {
    public:
        ///Retrieve the key (it is always in BasicRow format)
        BasicRowView key() const {return _key;}
        ///contains true, if row exists (was found)
        bool exists;
        ///the value of the row is available (non-null)
        bool available;
        ///Retrieve the value (parse the value);
        ValueType value() const {
            return _ValueDef::from_binary(_bin_data.data(), _bin_data.data()+_bin_data.size());
        }
        ///Converts to document
        operator ValueType() const {return value();}

        ///Returns true if document is available
        operator bool() const {return available;}

        ///Returns true if document is available
        bool operator==(std::nullptr_t) const {return available;}

        ///Returns true if document exists (but can be deleted)
        bool has_value() const {return exists;}

        DocID id() const {
            std::string_view r = _key.substr(_key.length()- sizeof(DocID));
            auto [id] = BasicRowView(r).get<DocID>();
            return id;
        }

    protected:
        std::string_view _key;
        std::string _bin_data;
        RowInfo(const PDatabase &db, const PSnapshot &snap, const RawKey &key)
            :_key(std::string_view(key).substr(sizeof(KeyspaceID))) {
            exists = db->get(key, _bin_data, snap);
            available = !_bin_data.empty();
        }
        friend class DocumentIndexView;

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
    RowInfo get(Key &key) const {
        key.change_kid(_kid);
        return RowInfo(_db, _snap, key);
    }
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    RowInfo get(Key &&key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    RowInfo operator[](Key &key) const {return get(key);}
    ///Retrieves exact row
    /**
     * @param key key to search. Rememeber, you also need to append document id at the end, because it is also
     * part of the key, otherwise, the function cannot find anything
     * @return
     */
    RowInfo operator[](Key &&key) const {return get(key);}


    class Iterator: public GenIterator<_ValueDef> {
    public:
        Iterator(_DocStorage &stor,
                std::unique_ptr<leveldb::Iterator> &&iter,
                typename GenIterator<_ValueDef>::Config &&cfg)
        :GenIterator<_ValueDef>(std::move(iter), cfg),_storage(stor) {}


        ///Retrieve associated document's id
        DocID id() const {
            std::string_view key = this->raw_key();
            BasicRowView x = key.substr(key.length()-sizeof(DocID));
            auto [id] = x.get<DocID>();
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
                    FirstRecord::included, LastRecord::excluded
            });
        } else {
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    RawKey(_kid+1),RawKey(_kid),
                    FirstRecord::excluded, LastRecord::included
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
            TempAppend k1(key);
            key.append<DocID>(-1);
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    key,RawKey(_kid),
                    FirstRecord::included, LastRecord::excluded
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
            TempAppend t1(to);
            if (last_record == LastRecord::included) to.append<DocID>(-1);
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    from,to,
                    FirstRecord::included, LastRecord::excluded
            });

        } else {
            TempAppend t1(from);
            TempAppend t2(to);
            from.append<DocID>(-1);
            if (last_record == LastRecord::excluded) to.append<DocID>(-1);
            return Iterator(_storage,_db->make_iterator(false,_snap),{
                    from,to,
                    FirstRecord::included, LastRecord::excluded
            });
        }
    }

protected:
    _DocStorage &_storage;
};

template<DocumentStorageType _DocStorage, DocumentDef _ValueDef = BasicRowDocument>
class DocumentIndex: public DocumentIndexView<_DocStorage, _ValueDef> {
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
            DocID cur_doc,
            KeyspaceID kid,bool deleting)
            :_observers(observers)
            ,_batch(batch)
            ,_cur_doc(cur_doc)
            ,_kid(kid),_deleting(deleting) {}

        void operator()(Key &k, const ValueType &v) {put(k,v);}
        void operator()(Key &&k, const ValueType &v) {put(k,v);}
        operator bool() const {return !_deleting;}

    protected:
        void put(Key &k, const ValueType &v) {
            auto [dummy, ks] = k.get<KeyspaceID, Blob>();
            k.change_kid(_kid);
            k.append(_cur_doc);
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
        DocID _cur_doc;
        KeyspaceID _kid;
        bool _deleting;

        friend class DocumentIndex;
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
    DocumentIndex(_DocStorage &storage, std::string_view name, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage, _ValueDef>(storage, name)
        ,_rev(revision)
    {
        connect_indexer(std::forward<Fn>(indexer));
        check_reindex();
    }

    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentIndex(_DocStorage &storage, KeyspaceID kid, int revision, Fn &&indexer)
        :DocumentIndexView<_DocStorage,_ValueDef>(storage, kid)
         ,_rev(revision)
    {
        connect_indexer(std::forward<Fn>(indexer));
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
        auto iter = this->_storage.scan();
        Batch b;
        while (iter.next()) {
            std::optional<DocType> doc = iter.value();
            const DocType *new_doc =doc.has_value()?&(*doc):nullptr;
            DocID id = iter.id();
            DocID prev_id = iter.prev_id();
            if (prev_id) {
               DocInfo dinfo = this->_storage[prev_id];
               if (dinfo.available) {
                   DocType old_doc = dinfo.doc();
                   _indexer(b, Update {&old_doc,new_doc,dinfo.prev_id,prev_id,id});
                   this->_db->commit_batch(b);
                   continue;
               }
            }
            _indexer(b, Update {nullptr,new_doc,0,prev_id,id});
            this->_db->commit_batch(b);
        }
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
                    Emit emt(_observers, b, update.old_doc_id, this->_kid, true);
                    fn(emt, *update.old_doc, {update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, update.new_doc_id, this->_kid, false);
                    fn(emt, *update.new_doc, {update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(_observers, b, update.old_doc_id, this->_kid, true);
                    fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, update.new_doc_id, this->_kid, false);
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

#endif /* SRC_DOCDB_DOC_INDEX_H_ */


