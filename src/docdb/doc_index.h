#pragma once
#ifndef SRC_DOCDB_DOC_INDEX_H_
#define SRC_DOCDB_DOC_INDEX_H_

#include "database.h"
#include "serialize.h"

#include "doc_storage_concept.h"

namespace docdb {

template<DocumentDef _ValueDef, DocumentStorageViewType _DocStorage>
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


    class Iterator: public GenIterator {
    public:
        Iterator(std::unique_ptr<leveldb::Iterator> &&iter,
            _DocStorage &storage,
            const std::string_view &range_end,
            Direction direction,
            LastRecord last_record)
            :GenIterator(std::move(iter), range_end, direction, last_record)
            ,_storage(storage) {}

        BasicRowView key() const {
            return BasicRowView(GenIterator::key());
        }
        std::string_view bin_data() const {
            return GenIterator::value();
        }
        ///retrieve the row
        ValueType value() const {
            std::string_view data = GenIterator::value();
            return _ValueDef::from_binary(data.begin(), data.end());
        }
        ///retrieve associated document
        /**
         * @return returns DocInfo, which is defined on DocumentStorageView<>::DocInfo
         * @note You need always check available flag before you retrieve the document itself
         */
        DocInfo doc() const {
            std::string_view key = raw_key();
            const char *from = key.data()+key.size()-sizeof(DocID);
            const char *to = from+key.size();
            DocID docId = BasicRowView::deserialize_item<DocID>(from,to);
            return _storage[docId];
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
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return _db->init_iterator<Iterator>(_snap, key, FirstRecord::included, _storage, key.prefix_end(), Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(_snap, key.prefix_end(), FirstRecord::excluded, _storage, key, Direction::backward, LastRecord::included);
        }
    }


    Iterator scan(Direction dir = Direction::normal) {
        if (isForward(_dir)) {
            return _db->init_iterator<Iterator>(_snap, RawKey(_kid), FirstRecord::included, _storage, RawKey(_kid+1), Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(_snap, RawKey(_kid+1), FirstRecord::excluded, _storage, RawKey(_kid), Direction::backward, LastRecord::included);
        }
    }

    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key,dir);}
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(_dir)) {
            return _db->init_iterator<Iterator>(_snap, key, FirstRecord::included, _storage, RawKey(_kid+1), Direction::forward, LastRecord::excluded);
        } else {
            TempAppend k1(key);
            key.append<DocID>(-1);
            return _db->init_iterator<Iterator>(_snap, key, FirstRecord::included, _storage, RawKey(_kid), Direction::backward, LastRecord::included);
        }
    }
    Iterator scan_prefix(Key &&key, Direction dir = Direction::normal) {return scan_prefix(key,dir);}
    Iterator scan_prefix(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        RawKey end = key.prefix_end();
        if (isForward(_dir)) {
            return _db->init_iterator<Iterator>(_snap, key, FirstRecord::included, _storage, end, Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(_snap, end, FirstRecord::excluded, _storage, key, Direction::backward, LastRecord::included);
        }

    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) {return scan_range(from, to, last_record);}
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded) {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            TempAppend t1(to);
            if (last_record == LastRecord::included) to.append<DocID>(-1);
            return _db->init_iterator<Iterator>(_snap, from, FirstRecord::included, _storage, to, Direction::forward, LastRecord::excluded);
        } else {
            TempAppend t1(from);
            TempAppend t2(to);
            from.append<DocID>(-1);
            if (last_record == LastRecord::excluded) to.append<DocID>(-1);
            return _db->init_iterator<Iterator>(_snap, from, FirstRecord::included, _storage, to, Direction::backward, LastRecord::included);
        }
    }

protected:
    _DocStorage &_storage;
};

template<DocumentDef _ValueDef, DocumentStorageType _DocStorage>
class DocumentIndex: public DocumentIndexView<_ValueDef, _DocStorage> {
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
            std::string &buffer,
            DocID cur_doc,
            KeyspaceID kid,bool deleting)
            :_observers(observers)
            ,_batch(batch)
            ,_buffer(buffer)
            ,_cur_doc(cur_doc)
            ,_kid(kid),_deleting(deleting) {}

        void operator()(Key &k, const ValueType &v) {put(k,v);}
        void operator()(Key &&k, const ValueType &v) {put(k,v);}
        operator bool() const {return !_deleting;}

    protected:
        void put(Key &k, const ValueType &v) {
            std::string_view ks(k);
            k.change_kid(_kid);
            k.append(_cur_doc);
            if (_deleting) {
                _batch.Delete(k);
            } else {
                _ValueDef::to_binary(v, std::back_inserter(_buffer));
                _batch.Put(k, {_buffer.data(), _buffer.size()});
                _buffer.clear();
            }
            _observers.call(_batch, BasicRowView(ks));
        }

        ObserverList<UpdateObserver> &_observers;
        Batch &_batch;
        std::string &_buffer;
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

    void register_observer(UpdateObserver &&observer) {
        _ptr->register_observer(std::move(observer));
    }



    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentIndex(_DocStorage &storage, std::string_view name, int revision, Fn &&indexer)
        :DocumentIndexView<_ValueDef, _DocStorage>(storage, name)
    {
        _ptr = std::make_shared<Indexer<Fn> >(this->_db, this->_kid, std::forward<Fn>(indexer), revision);
        check_reindex();
        register_storage_observer();
    }

    template<typename Fn>
    CXX20_REQUIRES(std::invocable<Fn, Emit &, const DocType &> || std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>)
    DocumentIndex(_DocStorage &storage, KeyspaceID kid, int revision, Fn &&indexer)
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
            std::string buffer;
            if constexpr(std::invocable<Fn, Emit &, const DocType &, const DocMetadata &>) {
                if (update.old_doc) {
                    Emit emt(_observers, b, buffer, update.old_doc_id, _kid, true);
                    _fn(emt, *update.old_doc, {update.old_doc_id, update.old_old_doc_id, true});
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, buffer,update.old_doc_id, _kid, false);
                    _fn(emt, *update.new_doc, {update.new_doc_id, update.old_doc_id, false});
                }
            } else {
                if (update.old_doc) {
                    Emit emt(_observers, b, buffer,update.old_doc_id,_kid, true);
                    _fn(emt, *update.old_doc);
                }
                if (update.new_doc) {
                    Emit emt(_observers, b, buffer,update.old_doc_id, _kid, false);
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

#endif /* SRC_DOCDB_DOC_INDEX_H_ */


