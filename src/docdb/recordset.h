#pragma once
#ifndef SRC_DOCDB_RECORDSET_H_
#define SRC_DOCDB_RECORDSET_H_
#include "key.h"
#include "leveldb_adapters.h"

#include <leveldb/iterator.h>
#include <memory>
#include <utility>
#include <functional>
#include <optional>


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

using FirstRecord =  LastRecord;


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



template<typename RecordSet, typename ValueType>
class RecordSetIterator: public std::iterator<std::forward_iterator_tag, ValueType> {
public:


    RecordSetIterator () = default;
    RecordSetIterator (RecordSet *coll, bool is_end):_coll(coll), _is_end(is_end || _coll->is_at_end()) {}
    RecordSetIterator (const RecordSetIterator &other):_coll(other._coll),_is_end(other._is_end) {}
    RecordSetIterator &operator=(const RecordSetIterator &other) {
        if (this != &other) {
            _coll = other._coll;
            _is_end = other._is_end;
            _val.reset();
        }
    }

    bool operator==(const RecordSetIterator &other) const {
        return _coll == other._coll && _is_end == other._is_end;
    }
    RecordSetIterator &operator++() {
        _is_end = !_coll->next();
        _val.reset();
        return *this;
    }

    RecordSetIterator operator++(int) {
        RecordSetIterator ret = *this;
        _is_end = !_coll->next();
        _val.reset();
        return ret;
    }

    const ValueType &operator *() const {
        return get_value();
    }

    const ValueType *operator->() const {
        return &get_value();
    }

protected:
    RecordSet *_coll = nullptr;
    bool _is_end = true;
    mutable std::optional<ValueType> _val;
    const ValueType &get_value() const {
        if (!_val.has_value()) {
            _val.emplace(_coll->get_item());
        }
        return *_val;
    }

};



class RecordSetBase {


public:

    ///Filter function - returns false to skip record
    using Filter = std::function<bool(const RecordSetBase &)>;

    struct Config {
        std::string_view range_start;
        std::string_view range_end;
        FirstRecord first_record;
        LastRecord last_record;
        Filter filter;
    };

    RecordSetBase(std::unique_ptr<leveldb::Iterator> &&iter, const Config &config)
        :_iter(std::move(iter))
        ,_range_beg(config.range_start)
        ,_range_end(config.range_end)
        ,_direction(config.range_start <= config.range_end?Direction::forward:Direction::backward)
        ,_first_record(config.first_record)
        ,_last_record(config.last_record)
        ,_filter(config.filter)
    {

        reset();

    }

    bool is_key(const std::string_view &key) const {
        return _iter->Valid() && to_string(_iter->key()) == key;
    }
    ///Retrieve key in raw form
    std::string_view raw_key() const {
        return to_string(_iter->key());
    }
    std::string_view raw_value() const {
        return to_string(_iter->value());
    }

    bool is_at_end() const {
        return _is_at_end;
    }

    ///move to next item
    /** @retval true next item is available
     *  @retval false found end;
     */
    bool next() {
        if (_is_at_end) return false;
        do {
            switch(_direction) {
                default:
                case Direction::forward:
                    _iter->Next();
                    check_end_fw(_range_end, _last_record);
                    break;
                case Direction::backward:
                    _iter->Prev();
                    check_end_bw(_range_end, _last_record);
            };
            if (_is_at_end) return false;
            if (!_filter || _filter(*this)) return true;
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
            switch(_direction) {
                default:
                case Direction::forward:
                    _iter->Prev();
                    check_end_bw(_range_beg, _first_record);
                    break;
                case Direction::backward:
                    _iter->Next();
                    check_end_fw(_range_beg, _first_record);
                    break;
            };
            if (_is_at_end) return false;
            if (!_filter || _filter(*this)) return true;
        } while (true);

    }

    bool reset() {
        _iter->Seek(_range_beg);

        switch (_direction) {
            case Direction::forward:
                check_end_fw(_range_end, _last_record);
                if (!_is_at_end &&_first_record == FirstRecord::excluded && is_key(_range_beg)) {
                    _iter->Next();
                    check_end_fw(_range_end, _last_record);
                }
                break;
            case Direction::backward:
                if (!_iter->Valid()) {
                    _iter->SeekToLast();
                    check_end_bw(_range_end, _last_record);
                } else {
                    if (_first_record == FirstRecord::excluded && !is_key(_range_beg)) {
                        _iter->Prev();
                        check_end_bw(_range_end, _last_record);
                    }
                }
                break;
            default:
                throw;
        }
        if (!_is_at_end && _filter && !_filter(*this)) {
            return next();
        }
        return !_is_at_end;

    }

    template<typename Fn>
    CXX20_REQUIRES(std::convertible_to<std::invoke_result_t<Fn, const RecordSetBase &>, bool>)
    void add_filter(Fn &&fn) {
        if (_filter) {
            _filter = [a = std::move(_filter), b = std::move(fn)](const RecordSetBase &rc){
                return a(rc) && b(rc);
            };
        } else {
            _filter = fn;
        }
    }

protected:

    using MyBuffer = Buffer<char, 128>;

    void check_end_fw(const MyBuffer &b, LastRecord lr) {
        if (_iter->Valid()) {
            auto ck = _iter->key();
            _is_at_end = (lr == LastRecord::excluded && ck == b)
                    || to_string(ck) > static_cast<std::string_view>(b);
        } else {
            _is_at_end = true;
        }
    }
    void check_end_bw(const MyBuffer &b, LastRecord lr) {
        if (_iter->Valid()) {
            auto ck = _iter->key();
            _is_at_end = (lr == LastRecord::excluded && ck == b)
                    || to_string(ck) < static_cast<std::string_view>(b);
        } else {
            _is_at_end = true;
        }
    }

    std::unique_ptr<leveldb::Iterator> _iter;
    MyBuffer _range_beg;
    MyBuffer _range_end;
    Direction _direction;
    FirstRecord _first_record;
    LastRecord _last_record;
    Filter _filter;
    bool _is_at_end = false;

};

template<typename _DocDef>
class RecordSetBaseT: public RecordSetBase {
public:

    using RecordSetBase::RecordSetBase;

    template<typename Fn>
    CXX20_REQUIRES(std::convertible_to<std::invoke_result_t<Fn, Key, const typename _DocDef::Type &>, bool>)
    void add_filter(Fn &&fn) {
        RecordSetBase::add_filter([fn = std::move(fn)](RecordSetBase &rc){
            auto s = rc.raw_value();
            return fn(Key(RowView(rc.raw_key())), _DocDef::from_binary(s.begin(), s.end()));
        });
    }
    template<typename Fn>
    CXX20_REQUIRES(std::convertible_to<std::invoke_result_t<Fn, Key>, bool>)
    void add_filter(Fn &&fn) {
        RecordSetBase::add_filter([fn = std::move(fn)](RecordSetBase &rc){
            return fn(Key(RowView(rc.raw_key())));
        });
    }

};



}


#endif /* SRC_DOCDB_RECORDSET_H_ */
