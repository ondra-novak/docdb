/*
 * iterator.h
 *
 *  Created on: 14. 5. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_ITERATOR_H_
#define SRC_DOCDB_ITERATOR_H_
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



template<typename Collection, typename ValueType>
class RecordSetIteratorT: public std::iterator<std::forward_iterator_tag, ValueType> {
public:


    RecordSetIteratorT () = default;
    RecordSetIteratorT (Collection &coll, bool is_end):_coll(&coll), _is_end(is_end) {}
    RecordSetIteratorT (const RecordSetIteratorT &other):_coll(other._coll),_is_end(other._is_end) {}
    RecordSetIteratorT &operator=(const RecordSetIteratorT &other) {
        if (this != &other) {
            _coll = other._coll;
            _is_end = other._is_end;
            _val.reset();
        }
    }

    bool operator==(const RecordSetIteratorT &other) const {
        return _coll == other._coll && _is_end == other._is_end;
    }
    RecordSetIteratorT &operator++() {
        _is_end = !_coll->next();
        _val.reset();
        return *this;
    }

    RecordSetIteratorT operator++(int) {
        RecordSetIteratorT ret = *this;
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
    Collection *_coll = nullptr;
    bool _is_end = true;
    mutable std::optional<ValueType> _val;
    const ValueType &get_value() const {
        if (!_val.has_value()) {
            _val.emplace(_coll->get_item());
        }
        return *_val;
    }

};



template<DocumentDef _ValueType>
class RecordSetT {

    enum class DirAndState {
        next_first,
        next,
        previous_first,
        previous
    };

public:
    using ValueType = typename _ValueType::Type;

    class AbstractFilter {
    public:
        virtual ~AbstractFilter() = default;
        virtual void release() {delete this;}
        virtual bool filter(const RecordSetT &iter) {return true;}
        virtual ValueType transform(const std::string_view &data) {
            return _ValueType::from_binary(data.begin(), data.end());
        }
        virtual ValueType transform(ValueType &&val) {
            return val;
        }

        struct Deleter {
            void operator()(AbstractFilter *x) const {x->release();}
        };
    };


    using PFilter = std::unique_ptr<AbstractFilter, typename AbstractFilter::Deleter>;

    class FilterChain {
    public:
        FilterChain(PFilter &&first,PFilter &&second)
            :_first(std::move(first)),_second(std::move(second)) {}

        virtual bool filter(const RecordSetT &iter) {
            return _first->filter(iter) && _second->filter(iter);;
        }
        virtual ValueType transform(const std::string_view &data) {
            return _second->transform(_first->transform(data));
        }
        virtual ValueType transform(ValueType &&val) {
            return _second->transform(_first->transform(val));
        }

        PFilter _first;
        PFilter _second;
    };


    struct Config {
        std::string_view range_start;
        std::string_view range_end;
        FirstRecord first_record;
        LastRecord last_record;
        PFilter filter;
    };

    RecordSetT(std::unique_ptr<leveldb::Iterator> &&iter, Config &&config)
        : RecordSetT(std::move(iter), config) {}
    RecordSetT(std::unique_ptr<leveldb::Iterator> &&iter, Config &config)
        :_iter(std::move(iter))
        ,_range_end(config.range_end)
        ,_direction(config.range_start > config.range_end?DirAndState::previous_first:DirAndState::next_first)
        ,_last_record(config.last_record)
        ,_filter(std::move(config.filter)) {

        _iter->Seek(to_slice(config.range_start));

        switch (_direction) {
            case DirAndState::next_first:
                if (config.first_record == FirstRecord::excluded && is_key(config.range_start)) {
                    _direction = DirAndState::next;
                }
                break;
            case DirAndState::previous_first:
                if (config.first_record == FirstRecord::excluded || !is_key(config.range_start)) {
                    _direction = DirAndState::previous;
                }
                break;
            default:
                throw;
        }

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

    ///Retrieve key as BasicRowView (to be parsed, without keyspaceid)
    Key key() const {
        return Key(RowView(raw_key()));
    }

    ValueType value() const {
        std::string_view val = to_string(_iter->value());
        if (_filter) {
            return _filter->transform(val);
        } else {
            return _ValueType::from_binary(val.begin(), val.end());
        }
    }

    void add_filter(PFilter &&flt) {
        if (_filter) {
            _filter = PFilter(new FilterChain(std::move(_filter), std::move(flt)));
        } else {
            _filter = std::move(flt);
        }
    }
    void remove_filter() {
        if (_filter) {
            auto f = dynamic_cast<FilterChain *>(_filter.get());
            if (f) {
                _filter = std::move(f->_first);
            } else{
                _filter.reset();
            }
        }
    }

    ///move to next item
    /** @retval true next item is available
     *  @retval false found end;
     */
    bool next() {
        if (!_iter->Valid()) return false;
        do {
            switch(_direction) {
                default:
                case DirAndState::next: _iter->Next();
                                        [[fallthrough]];
                case DirAndState::next_first:
                    _direction = DirAndState::next;
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
                case DirAndState::previous: _iter->Prev();
                                         [[fallthrough]];
                case DirAndState::previous_first:
                    _direction = DirAndState::previous;
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
            if (!_filter || _filter->filter(*this)) return true;
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
                case DirAndState::next:
                case DirAndState::next_first:
                    _direction = DirAndState::next;
                    _iter->Prev();
                    if (!_iter->Valid()) return false;
                    break;
                case DirAndState::previous:
                case DirAndState::previous_first:
                    _direction = DirAndState::previous;
                    _iter->Next();
                    if (!_iter->Valid()) return false;
                    break;
            };
            if (!_filter || _filter->filter(*this)) return true;
        } while (true);

    }

    struct IteratorValueType {
        Key key;
        ValueType value;
    };

    auto get_item() const {
        return IteratorValueType{key(), value()};
    }

    using Iterator = RecordSetIteratorT<RecordSetT, IteratorValueType>;


    Iterator begin() {
        return Iterator(*this, false);
    }
    Iterator end() {
        return Iterator(*this, true);
    }


protected:

    std::unique_ptr<leveldb::Iterator> _iter;
    std::string _range_end;
    DirAndState _direction;
    LastRecord _last_record;
    PFilter _filter;
    std::size_t _cur_pos = 0;

};

template<DocumentDef _ValueDef>
using GenIterator = RecordSetT<_ValueDef>;



}


#endif /* SRC_DOCDB_ITERATOR_H_ */
