/*
 * index.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_INDEX_H_
#define SRC_DOCDB_INDEX_H_
#include "keyvalue.h"
#include "view.h"
#include "concepts.h"
#include <string_view>
#include "storage.h"


namespace docdb {

class Storage;



class EmitFn;

using Indexer = std::function<void(std::string_view, const EmitFn &)>;





class IndexView {
public:

    IndexView(PDatabase db, std::string_view name, Direction dir = Direction::forward, PSnapshot snap = {})
        :_db(std::move(db)),_snap(std::move(snap)),_kid(_db->open_table(name)), _dir(dir) {}
    IndexView(PDatabase db, KeyspaceID kid, Direction dir = Direction::forward, PSnapshot snap = {})
        :_db(std::move(db)), _snap(std::move(snap)),_kid(kid), _dir(dir) {}

    using DocID = Storage::DocID;

    IndexView make_snapshot() const {
        if (_snap != nullptr) return *this;
        return IndexView(_db, _kid, _dir, _db->make_snapshot());
    }

    IndexView open_snapshot(const PSnapshot &snap) const {
        return IndexView(_db, _kid, _dir, snap);
    }


    using _Iterator = Iterator;
    class Iterator: public _Iterator {
    public:
        using _Iterator::_Iterator;

        std::string_view key() const {
            return _iter->Valid()?to_string(_iter->key()).substr(sizeof(KeyspaceID)+1):std::string_view();
        }
    };

    ///Find a key
    /**
     * @param key specifies key to find
     * @return return iterator, which processes all values for matching key
     *
     * @note the function can be used to search prefixes. To search string prefix,
     * you need to avoid adding terminating zero after last string, this can be done using
     * casting the last string to StringPrefix
     */
    Iterator find(Key &&key) {return find(key);}
    ///Find a key
    /**
     * @param key specifies key to find
     * @return return iterator, which processes all values for matching key
     *
     * @note the function can be used to search prefixes. To search string prefix,
     * you need to avoid adding terminating zero after last string, this can be done using
     * casting the last string to StringPrefix
     */
    Iterator find(Key &key) {
        key.change_kid(_kid);
        RawKey endk = key.prefix_end();
        if (isForward(_dir)) {
            return _db->init_iterator<Iterator>(false, _snap, key, false, endk, Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(false, _snap, key, true, endk, Direction::backward, LastRecord::included);
        }
    }

    static constexpr DocID first_doc = 0;
    static constexpr DocID last_doc = -1;


    ///Scan index from given key in direction
    /**
     * @param key key to start. The key can also contain document id to specify
     * exact document where to start (for example in case of paging).
     * @param dir direction to process
     * @return iterator
     */
    Iterator scan_from(Key &&key, Direction dir = Direction::normal) {return scan_from(key, dir);}
    ///Scan index from given key in direction
    /**
     * @param key key to start. The key can also contain document id to specify
     * exact document where to start (for example in case of paging).
     * @param dir direction to process
     * @return iterator
     */
    Iterator scan_from(Key &key, Direction dir = Direction::normal) {
        key.change_kid(_kid);
        if (isForward(changeDirection(_dir, dir))) {
            RawKey endk(_kid+1);
            return _db->init_iterator<Iterator>(false, _snap, key, false, endk, Direction::forward, LastRecord::excluded);
        } else {
            TempAppend _(key);
            key.add(last_doc);
            RawKey endk(_kid);
            return _db->init_iterator<Iterator>(false, _snap, key, false, endk, Direction::backward, LastRecord::excluded);
        }
    }


    ///Scan given range
    /**
     * @param from starting key, it can contain starting document id
     * @param to ending key, it can contain ending document id
     * @param last_record whether to include 'to' key into the range.
     * @return iterator
     */
    Iterator scan_range(Key &&from, Key &&to, LastRecord last_record = LastRecord::excluded) {
        return scan_range(from, to, last_record);
    }
    ///Scan given range
    /**
     * @param from starting key, it can contain starting document id
     * @param to ending key, it can contain ending document id
     * @param last_record whether to include 'to' key into the range.
     * @return iterator
     */
    Iterator scan_range(Key &from, Key &&to, LastRecord last_record = LastRecord::excluded) {
        return scan_range(from, to, last_record);
    }
    ///Scan given range
    /**
     * @param from starting key, it can contain starting document id
     * @param to ending key, it can contain ending document id
     * @param last_record whether to include 'to' key into the range.
     * @return iterator
     */
    Iterator scan_range(Key &&from, Key &to, LastRecord last_record = LastRecord::excluded) {
        return scan_range(from, to, last_record);
    }
    ///Scan given range
    /**
     * @param from starting key, it can contain starting document id
     * @param to ending key, it can contain ending document id
     * @param last_record whether to include 'to' key into the range.
     * @return iterator
     */
    Iterator scan_range(Key &from, Key &to, LastRecord last_record = LastRecord::excluded) {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            TempAppend _gt(to);
            if (last_record == LastRecord::included) to.add(last_doc);
            return  _db->init_iterator<Iterator>(false, _snap, from, false, to, Direction::forward, LastRecord::excluded);
        } else {
            TempAppend _gt(to);
            TempAppend _gf(from);
            if (last_record == LastRecord::excluded) to.add(last_doc);
            from.add(last_doc);
            return _db->init_iterator<Iterator>(false, _snap, from, false, to, Direction::backward, LastRecord::excluded);
        }
    }



protected:
    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
    Direction _dir;


};


class Index: public IndexView {
public:

    using UpdateCallback = std::function<void(Batch &, const std::vector<std::string_view> &, const PSnapshot &)>;

    ///Create index
    /**
     * @param name name of index
     * @param source source storage
     * @param revision indexer revision
     * @param indexFn indexer function
     * @param dir direction when index is read
     */
    Index(std::string_view name,
          Storage &source,
          std::size_t revision,
          Indexer &&indexFn,
          Direction dir = Direction::forward);



    void register_callback(UpdateCallback &&cb);


protected:

    class Instance: public IndexView {
    public:
        Instance(KeyspaceID kid,
              Storage &source,
              std::size_t revision,
              Indexer &&indexFn);

        void update(const PSnapshot &snapshot);
        void init();
        DocID get_start_id() const {return _start_id;}
    protected:
        Storage &_source;
        Indexer _indexFn;
        std::size_t _revision;
        DocID _start_id = 0;

        std::mutex _mx;
        std::vector<UpdateCallback> _cblist;

    };

    std::shared_ptr<Instance> _inst;


    void init_from_db();


    friend class EmitFn;

};

class EmitFn {
public:
    using KeySet = Value;

    EmitFn(Batch &b, KeySet &ks, KeyspaceID kid, Storage::DocID docId, bool del);

    void operator()(Key &key, Value &value) const;
    void operator()(Key &&key, Value &value) const {operator()(key,value);}
    void operator()(Key &key, Value &&value) const {operator()(key,value);}
    void operator()(Key &&key, Value &&value) const {operator()(key,value);}
protected:
    Batch &_b;
    KeySet &_ks;
    KeyspaceID _kid;
    Storage::DocID _docid;
    bool _del;

};



}
#endif /* SRC_DOCDB_INDEX_H_ */
