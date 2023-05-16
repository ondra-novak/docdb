/*
 * iterator.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_ITERATOR_H_
#define SRC_DOCDB_ITERATOR_H_
#include "leveldb_adapters.h"

#include "keyvalue.h"
#include <leveldb/iterator.h>
#include <memory>
#include <utility>
#include <functional>


namespace docdb {

enum class Direction {
    ///iterate forward
    /** Iterates in order of alphanumeric ordering, from 'a' to 'z', from '0' to '9' */
    forward,
    ///iterate backward
    /** Iterates in order of reversed alphanumeric ordering, from 'z' to 'a', from '9' to '0' */
    backward,
    ///Used to specify, that ordering doesn't change relative to default
    /** if there is no default ordering, it is equal to forward */
    normal,
    ///Used to specify, that ordering is reversed relative to default
    /** if there is no default ordering, it is equal to reversed */
    reversed
};

enum class LastRecord {
    included,
    excluded
};

static constexpr Direction changeDirection(Direction initial, Direction change) {
    switch (initial) {
        case Direction::forward: switch (change) {
            default:
            case Direction::normal: return initial;
            case Direction::backward:
            case Direction::forward: return change;
            case Direction::reversed: return Direction::backward;
        };
        case Direction::backward: switch (change) {
            default:
            case Direction::normal: return initial;
            case Direction::backward:
            case Direction::forward: return change;
            case Direction::reversed: return Direction::forward;
        };
        default:
        case Direction::normal: switch (change) {
            default:
            case Direction::normal: return initial;
            case Direction::backward:
            case Direction::forward: return change;
            case Direction::reversed: return Direction::reversed;
        };
        case Direction::reversed: switch (change) {
            default:
            case Direction::normal: return initial;
            case Direction::backward:
            case Direction::forward: return change;
            case Direction::reversed: return Direction::normal;
        };
    }
}

static constexpr bool isForward(Direction dir) {
    return dir == Direction::forward || dir == Direction::normal;
}


class Iterator {
public:

    ///Transform function
    /**
     * Transform function allows to perform additional transformation or calculation
     * above the value.
     *
     * The function accepts three arguments
     *
     * @p @b 1 Iterator as pointer to iterator instance
     * @p @b 2 string_view instance contains a value to transform
     * @p @b 3 string reference to buffer, where to put result
     * @return bool whether transform has been done, or not
     */
    using ValueTransform = std::function<bool(const Iterator *, const std::string_view &, std::string &)>;
    ///Filter function
    /** The filter function returns true to accept value, or false to skip value */
    using Filter = std::function<bool(const Iterator *)>;


    ///Construct the iterator
    /**
     * @param iter underlying iterator instance
     * @param range_end end of the range
     * @param direction direction
     * @param last_record whether to include last record
     */
    Iterator(std::unique_ptr<leveldb::Iterator> &&iter,
            const std::string_view &range_end,
            Direction direction = Direction::forward,
            LastRecord last_record = LastRecord::excluded)
        :_iter(std::move(iter))
        ,_range_end(range_end)
        ,_direction(direction)
        ,_last_record(last_record) {
    }

    ///Construct the iterator
    /**
     * @param iter underlying iterator instance
     * @param range_end end of the range
     * @param transform transform function (can be nullptr)
     * @param filter filter function (can be nullptr)
     * @param direction direction
     * @param last_record whether to include last record
     */
    Iterator(std::unique_ptr<leveldb::Iterator> &&iter,
            const std::string_view &range_end,
            ValueTransform &&transform,
            Filter &&filter,
            Direction direction = Direction::forward,
            LastRecord last_record = LastRecord::excluded)
    :_iter(std::move(iter))
    ,_range_end(range_end)
    ,_direction(direction)
    ,_last_record(last_record)
    ,_transform(std::move(transform))
    ,_filter(std::move(filter)) {
    }

    bool is_key(const std::string_view &key) const {
        return _iter->Valid() && to_string(_iter->key()) == key;
    }
    ///Retrieve key in raw form
    std::string_view raw_key() const {
        return _iter->Valid()?to_string(_iter->key()):std::string_view();
    }

    ///Retrieve key, strip keyspace id (so you can directly parse the key)
    std::string_view key() const {
        return _iter->Valid()?to_string(_iter->key()).substr(sizeof(KeyspaceID)):std::string_view();
    }

    std::string_view value() const {
        std::string_view val = to_string(_iter->value());
        if (_transform) {
            bool b;
            switch (_trn_state) {
                case not_transformed_yet:
                   _value_buff.clear();
                   b = _transform(this, val, _value_buff);
                   if (b) {
                       _trn_state = transformed;
                       return _value_buff;
                   } else {
                       _trn_state = keep_original;
                       return val;
                   }
                case transformed:
                    return _value_buff;
                default:
                    return val;
            }
        } else {
            return val;
        }
    }

    ///Set transform
    ValueTransform set_transform(ValueTransform &&fn) {
        fn.swap(_transform);
        return fn;
    }

    ///Set transform
    Filter set_filter(Filter &&fn) {
        fn.swap(_filter);
        _first_move = true;
        return fn;
    }

    ///add transform
    /** Adds transform to the current transform chain
     *
     * @param trn new transform function
     * @param before
     */
    void add_transform(ValueTransform &&trn, bool before = false) {
        if (!_transform) {
            set_transform(std::move(trn));
        } else {
            if (before) set_transform(combine(std::move(trn),set_transform({})));
            else set_transform(combine(set_transform({}), std::move(trn)));
        }
    }

    void add_filter(Filter &&flt) {
        if (!_filter) {
            set_filter(std::move(flt));
        } else {
            set_filter(combine(set_filter({}), std::move(flt)));
        }
    }



    ///move to next item
    /** @retval true next item is available
     *  @retval false found end;
     */
    bool next() {
        if (!_iter->Valid()) return false;
        do {
            clear_state();
            switch(_direction) {
                default:
                case Direction::forward:
                case Direction::normal:
                    if (!_first_move) _iter->Next();
                    if (!_iter->Valid()) return false;
                    switch (_last_record){
                        case LastRecord::included:
                            if (to_string(_iter->key()) > _range_end) return false;
                            break;
                        default:
                            if (to_string(_iter->key()) >= _range_end) return false;
                            break;
                    };
                    break;
                case Direction::backward:
                case Direction::reversed:
                    if (!_first_move) _iter->Prev();
                    if (!_iter->Valid()) return false;
                    switch (_last_record){
                        case LastRecord::included:
                            if (to_string(_iter->key()) < _range_end) return false;
                            break;
                        default:
                            if (to_string(_iter->key()) <= _range_end) return false;
                            break;
                    };
                    break;
            };
            _first_move = false;
            if (!_filter || _filter(this)) return true;
        } while (true);
    }

    ///move to previous item
    /** @retval true next item is available
     *  @retval false found end;
     *  @note Moving before the starting point is UB
     */
    bool previous() {
        if (!_iter->Valid()) return false;
        do {
            clear_state();
            switch(_direction) {
                default:
                case Direction::normal:
                case Direction::forward:
                    _iter->Prev();
                    if (!_iter->Valid()) return false;
                    break;
                case Direction::reversed:
                case Direction::backward:
                    _iter->Next();
                    if (!_iter->Valid()) return false;
                    break;
            };
            _first_move = false;
            if (!_filter || _filter(this)) return true;
        } while (true);

    }


protected:
    enum ValueTransformState {
        ///value was not transformed yet, call transform on access
        not_transformed_yet,
        ///value has been transformed, return transformed value
        transformed,
        ///transformation failed, return original value
        keep_original
    };

    std::unique_ptr<leveldb::Iterator> _iter;
    std::string _range_end;
    Direction _direction;
    LastRecord _last_record;
    ValueTransform _transform;
    Filter _filter;
    mutable std::string _value_buff;
    mutable ValueTransformState _trn_state = not_transformed_yet;
    bool _first_move = true;

    void clear_state() {
        _trn_state = not_transformed_yet;
    }

    static ValueTransform combine(ValueTransform &&a, ValueTransform &&b) {
        return [a = std::move(a), b = std::move(b)](const Iterator *iter, const std::string_view &src, std::string &buff)->bool{
            std::string tmp;
            if (a(iter, src, tmp)) {
                if (!b(iter,tmp, buff)) buff = tmp;
                return true;
            }
            return b(iter, src, buff);
        };
    }

    static Filter combine(Filter &&a, Filter &&b) {
        return [a = std::move(a), b = std::move(b)](const Iterator *iter){
            return a(iter) && b(iter);
        };
    }


};


}


#endif /* SRC_DOCDB_ITERATOR_H_ */
