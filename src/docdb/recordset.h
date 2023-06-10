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
class RecordSetIterator{
public:

    using iterator_category = std::forward_iterator_tag;
    using value_type = ValueType; // crap
    using difference_type = ptrdiff_t;
    using pointer = void;
    using reference = void;

    RecordSetIterator () = default;
    RecordSetIterator (RecordSet *coll, bool is_end):_coll(coll), _is_end(is_end || _coll->empty()) {}
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
        Filter filter = {};
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


    ///Returns true, if recordset is empty
    /**
     * @retval true recordset is empty
     * @retval false recordset contains values
     *
     * @note the function returns state relative to current reading position.
     * You need to call reset() to restore recordset state
     */
    bool empty() const {return _is_at_end;}

    ///move to next item
    /** @retval true next item is available
     *  @retval false found end;
     */
    bool next() {
        if (_is_at_end) return false;
        do {
            ++_count;
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
            --_count;
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

    ///Resets recordsed state
    /**
     * Moves cursor to the first record allowing to read the recordset again
     *
     * @return returns same value as empty() called after reset();
     */
    bool reset() {
        _iter->Seek(_range_beg);
        _count = 0;

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

    ///Add the record filter
    /**
     *
     * @note this function adds filter which must be able to read raw versions of keys and values.
     * @param fn filter function, it receives reference to instance to recordset, which's cursor
     * is set to current record, and function must return true, to allow the record, or false to
     * move to next record;
     *
     * More filters can be added
     */
    template<typename Fn>
    DOCDB_CXX20_REQUIRES(std::convertible_to<std::invoke_result_t<Fn, const RecordSetBase &>, bool>)
    void add_filter(Fn &&fn) {
        if (_filter) {
            _filter = [a = std::move(_filter), b = std::move(fn)](const RecordSetBase &rc){
                return a(rc) && b(rc);
            };
        } else {
            _filter = fn;
        }
    }


    ///calculates count of remaining items
    /**
     * @return count of remainig items
     * @note function seeks back to current position, once the count is calculated
     *
     * @note function calculates the count by moving the cursor until end of the recordset is
     * reached
     */
    std::size_t count()  {
        if (this->_is_at_end) return 0;
        auto c = _count;
        std::string cur_key (raw_key());
        std::size_t n = 0;
        while (!this->_is_at_end) {
            n++;
            next();
        }
        _iter->Seek(cur_key);
        _is_at_end = false;
        _count = c;
        return n;
    }

    ///calculates aproximate count of records
    /**
     * @param db database object - the function needs database object to calculate aproximation
     * @param limit maximum count of rows to be calculate accurately. If there is more
     * rows, the function calculates an aproximation.
     * The function counts records and when the counter reaches a specified limit, it
     * aproximates count of records. The result don't need to be accurate.
     * @return count of records aproximated
     */
    template<typename PDatabase>
    std::size_t count_aprox(const PDatabase &db, std::size_t limit=30) {
        if (this->_is_at_end) return 0;
        auto c = _count;
        std::uint64_t sz_bytes = db->get_index_size(raw_key(), _range_end);;
        if (sz_bytes == 0) return count();

        std::string cur_key (raw_key());
        std::size_t n = 0;
        std::size_t step = limit;
        while (!this->_is_at_end) {
            if (n >= limit) {
                std::uint64_t sz_range = db->get_index_size(cur_key, raw_key());
                //returned range must be more then 0 (it is still aproximation)
                //and must be less than total size (otherwise range is small and can be calculated)
                if (sz_range > 0 && sz_range < sz_bytes) {
                    //n * sz_range / sz_butes
                    n =  static_cast<std::size_t>(
                            static_cast<double>(n) * sz_bytes / sz_range);
                    break;
                }
                limit = limit + step;
            }
            n++;
            next();
        }
        _iter->Seek(cur_key);
        _count = c;
        _is_at_end = false;
        return n;
    }

    ///Calculate aproximate size of the recordset in bytes.
    /** This function can be used to determine index size for comparison, but not count of
     * records. For example, if you need to know, which recordset is larger then other
     *
     * @param db database object
     * @return aproximation size in bytes
     */
    template<typename PDatabase>
    std::uint64_t aprox_size_in_bytes(const PDatabase &db)  const{
        return db->get_index_size(_range_beg, _range_end);
    }

    ///Calculate aproximate size of the recordset in bytes.
    /** This function can be used to determine index size for comparison, but not count of
     * records. For example, if you need to know, which recordset is larger then other
     *
     * @param db database object
     * @return aproximation size in bytes
     */
    template<typename PDatabase>
    std::uint64_t aprox_procesed_bytes(const PDatabase &db)  const{
        if (_is_at_end) return aprox_size_in_bytes(db);
        else return db->get_index_size(_range_beg, to_string(_iter->key()));
    }

    template<typename PDatabase>
    std::uint64_t aprox_remain_bytes(const PDatabase &db)  const{
        if (_is_at_end) return aprox_size_in_bytes(db);
        else return db->get_index_size(to_string(_iter->key()),_range_end);
    }

    ///Current record offset
    auto get_offset() const {
        return _count;
    }

    operator bool() const {
        return !empty();
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
    std::size_t _count = 0;
    bool _is_at_end = false;

};




}


#endif /* SRC_DOCDB_RECORDSET_H_ */
