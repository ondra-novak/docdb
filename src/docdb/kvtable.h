#pragma once
#ifndef SRC_DOCDB_KVTABLE_H_
#define SRC_DOCDB_KVTABLE_H_
#include "database.h"
#include <leveldb/write_batch.h>


namespace docdb {

class KeyValueTableView {
public:
    KeyValueTableView(const PDatabase &db, const std::string_view &name, Direction dir = Direction::forward, const PSnapshot &snap = {});
    KeyValueTableView(const PDatabase &db, KeyspaceID kid, Direction dir = Direction::forward, const PSnapshot &snap = {});

    bool lookup(Key &&key, std::string &value)  {return lookup(key, value);}
    bool lookup(Key &key, std::string &value) {
        key.change_kid(_kid);
        return _db->get(key, value);
    }

    using Iterator = GenIterator;

    Iterator scan(Direction dir = Direction::normal) {
        if (isForward(changeDirection(_dir, dir))) {
            return _db->init_iterator<Iterator>(false, _snap, RawKey(_kid), false, RawKey(_kid+1), Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(false, _snap, RawKey(_kid+1), true, RawKey(_kid), Direction::backward, LastRecord::included);
        }
    }

    Iterator scan_prefix(Key &&prefix_key, Direction dir = Direction::normal) {
        return scan_prefix(prefix_key, dir);
    }
    Iterator scan_prefix(Key &prefix_key, Direction dir = Direction::normal) {
        prefix_key.change_kid(_kid);
        if (isForward(changeDirection(_dir, dir))) {
            return _db->init_iterator<Iterator>(false, _snap, prefix_key, false, prefix_key.prefix_end(), Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(false, _snap, prefix_key.prefix_end(), true, prefix_key,  Direction::forward, LastRecord::included);
        }
    }

    Iterator scan_from(Key &&start, Direction dir = Direction::normal) {
        return scan_from(start, dir);
    }
    Iterator scan_from(Key &start, Direction dir = Direction::normal) {
        start.change_kid(_kid);
        if (isForward(changeDirection(_dir, dir))) {
            return _db->init_iterator<Iterator>(false, _snap, start, false, Key(_kid+1), Direction::forward, LastRecord::excluded);
        } else {
            return _db->init_iterator<Iterator>(false, _snap, start, false, Key(_kid),  Direction::backward, LastRecord::included);
        }
    }

    Iterator scan_range(Key &&from, Key &&to, LastRecord lr = LastRecord::excluded) {return scan_range(from, to, lr);}
    Iterator scan_range(Key &from, Key &&to, LastRecord lr = LastRecord::excluded) {return scan_range(from, to, lr);}
    Iterator scan_range(Key &&from, Key &to, LastRecord lr = LastRecord::excluded) {return scan_range(from, to, lr);}
    Iterator scan_range(Key &from, Key &to, LastRecord lr = LastRecord::excluded) {
        from.change_kid(_kid);
        to.change_kid(_kid);
        if (from <= to) {
            return _db->init_iterator<Iterator>(false, _snap, from, false, to, Direction::forward, lr);
        } else {
            return _db->init_iterator<Iterator>(false, _snap, from, false, to,  Direction::backward, lr);
        }
    }


protected:
    PDatabase _db;
    KeyspaceID _kid;
    Direction _dir;
    PSnapshot _snap;
};

class KeyValueTable: public KeyValueTableView {
public:
    using KeyValueTableView::KeyValueTableView;

    using UpdateCallback = std::function<void(Batch &, const std::vector<std::string_view> &, const PSnapshot &)>;

    class WriteBatch {
    public:
        WriteBatch(KeyValueTable &owner);
        ~WriteBatch();

        void commit();
        void rollback();
        void put(Key &&key, const Value &&value);
        void put(Key &&key, const std::string_view &value);
        void put(Key &key, const Value &&value);
        void put(Key &key, const std::string_view &value);
        void erase(Key &&key);
        void erase(Key &key);

    protected:
        Batch b;
        KeyValueTable &_owner;
    };



protected:
    std::mutex _mx;
    std::vector<Key> _change_key_list;
    std::vector<std::string_view> _change_unique_key_list;

};

}


#endif /* SRC_DOCDB_KVTABLE_H_ */
