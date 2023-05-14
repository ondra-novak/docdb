#pragma once
#ifndef SRC_DOCDB_VIEW_H_
#define SRC_DOCDB_VIEW_H_
#include "database.h"

#include "key.h"

#include "iterator.h"
namespace docdb {




class View {
public:

    ///Construct the view
    /**
     * @param db reference to database
     * @param name name of table
     * @param dir default direction to iterate this view (default is forward)
     * @param snap reference to snapshot (optional)
     */
    View(PDatabase db, std::string_view name, Direction dir = Direction::forward, PSnapshot snap = {})
        :_db(std::move(db)),_kid(_db->open_table(name)), _dir(dir), _snap(std::move(snap)) {}

    ///Construct the view
    /**
     * @param db reference to database
     * @param kid keyspace id
     * @param dir default direction to iterate this view (default is forward)
     * @param snap reference to snapshot (optional)
     */
    View(PDatabase db, KeyspaceID kid, Direction dir = Direction::forward, PSnapshot snap = {})
        :_db(std::move(db)),_kid(kid), _dir(dir), _snap(std::move(snap)) {}

    ///Retrieve snapshot of this view
    /**
     * @return copy of view as snapshot
     */
    View get_snapshot() const {
        if (_snap) return *this;
        else return View(_db, _kid, _dir, _db->make_snapshot());
    }

    ///Retrieve key instance (to be filled with search data);
    template<typename ... Args>
    Key key(const Args & ... args) const {
        if (_subindex.has_value())
            return Key(_kid, *_subindex, args...);
        else
            return Key(_kid, args...);
    }

    ///Lookup for a key
    /**
     * @param key key to lookup
     * @return value found. If the key doesn't exists, result is no-value state
     */
    std::optional<std::string_view> lookup(const Key &key) const {
        std::optional<std::string> out;
        out.emplace();
        if (!_db->get(key, *out, _snap)) out.reset();
        return out;
    }

    ///Create iterator to scan whole view
    /**
     * @param dir you can specify default direction
     * @return iterator
     */
    Iterator scan(Direction dir = Direction::normal) const {
        Key from(key());
        Key to(from.prefix_end());
        if (isForward(changeDirection(_dir, dir))) {
            return scan(from, to, LastRecord::excluded);
        } else {
            Iterator r = scan(to,from, LastRecord::included);
            if (r.is_key(to)) r.next();
            return r;
        }
    }

    ///Scan from key to the end of view,
    /**
     * @param key key where start
     * @param dir direction
     * @return iterator
     */
    Iterator scan_from(const Key &key, Direction dir = Direction::normal) const {
        if (isForward(changeDirection(_dir, dir))) {
            return scan(key, this->key().prefix_end(), LastRecord::excluded);
        } else {
            return scan(key, this->key(), LastRecord::included);
        }
    }


    ///Scan from key to key
    /**
     * @param from starting key
     * @param to ending key
     * @param last_record whether to include last item (the item which key equals to
     *  'to' key
     * @return iterator
     */
    Iterator scan(const Key &from, const Key &to,  LastRecord last_record = LastRecord::excluded) const {
        auto iter = _db->make_iterator(false, _snap);
        iter->Seek(from);
        if (from <= to) {
            return Iterator(std::move(iter), to, Direction::forward, last_record);
        } else {
            return Iterator(std::move(iter), to, Direction::backward, last_record);
        }
    }

    ///Scan for key prefix
    /**
     * @param pfx prefix. It will iterateo for all keys starting by the prefix
     * @param dir allows to change direction
     * @return iterator
     */
    Iterator scan_prefix(const Key &pfx, Direction dir  = Direction::normal) {
        if (isForward(changeDirection(_dir, dir))) {
            return scan(pfx, pfx.prefix_end());
        } else {
            return scan(pfx.prefix_end(), pfx);
        }
    }

    ///Calculate index size between two keys
    /**
     * @param from from key
     * @param to to key
     * @return return value is not in particula unit, it is just number, not items.
     * You can use this number to compare multiple indexes to order index processing
     * from smallest to largest index
     */
    auto get_index_size(const Key &from, const Key &to) const {
        if (from >= to) return _db->get_index_size(to, from);
        return _db->get_index_size(from, to);
    }
    auto get_index_size(const Key &key, Direction dir = Direction::normal) const {
        return get_index_size(key, isForward(changeDirection(_dir, dir))?this->key().prefix_end():this->key());
    }
    auto get_index_size(const Key &key) const {
        return get_index_size(key, key.prefix_end());
    }
    auto get_index_size() const {
        return get_index_size(this->key(), this->key().prefix_end());
    }




protected:
    PDatabase _db;
    PSnapshot _snap;
    KeyspaceID _kid;
    Direction _dir;
    std::optional<KeyspaceID> _subindex;
};


}



#endif /* SRC_DOCDB_VIEW_H_ */
