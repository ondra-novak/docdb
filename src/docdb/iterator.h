/*
 * iterator.h
 *
 *  Created on: 6. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_ITERATOR_H_
#define SRC_DOCDB_ITERATOR_H_

#include "key.h"
#include <leveldb/iterator.h>
#include <memory>

namespace docdb {

///Base class for iterators
/**
 * Iterator in docdb are one shot for given range, can be used to bulk scanning
 * You can define key range, whether begin and end is included
 */

template<typename Impl>
class IteratorBase {
public:
    
    
    enum class State {
        created,
        positioned,
        end
        
    };
    
    IteratorBase(std::unique_ptr<leveldb::Iterator>  &&iter, const KeyView &begin, const KeyView &end, bool include_begin, bool include_end)
        :
            _iter(std::move(iter)),
            _begin(std::move(begin)), 
            _end(std::move(end)),
            _include_begin(include_begin),
            _include_end(include_end),
            _backward(end<begin),
            _state(State::created) {
    }
        
    
    bool next() {
        switch (_state) {
            case State::created: 
                _iter->Seek(_begin);
                if (_iter->Valid()) {
                    KeyView k(_iter->key());
                    _state = State::positioned;
                    if (!_include_begin && k == _begin) {
                        return next();
                    } 
                    if (!check_filter()) return next();
                    return true;
                } else {
                    _state = State::end;
                    return false;
                }
            break;
            case State::positioned:
                if (_backward) {
                    _iter->Prev();
                    if (_iter->Valid()) {
                        KeyView k(_iter->key());
                        int cmp = k.compare(_end);
                        if ((cmp > 0) ||  (cmp == 0 && _include_end)) {
                            if (!check_filter())  return next();
                            return true;
                        }
                        _state = State::end; 
                        return false;
                    } else {
                        _state = State::end; 
                        return false;
                    }
                } else {
                    _iter->Next();
                    if (_iter->Valid()) {
                        KeyView k(_iter->key());
                        int cmp = k.compare(_end);
                        if ((cmp < 0) || (cmp == 0 && _include_end))  {
                            if (!check_filter())  return next();
                            return true;
                        }
                        _state = State::end; 
                        return false;
                    } else {
                        _state = State::end;
                        return false;
                    }
                }
            default:
                    return false;
        }
    }
    
    void swap_dir() {
        std::swap(_begin, _end);
        std::swap(_include_begin, _include_end);
        _backward = !_backward;
    }
    
    bool forward() const {return !_backward;}
    bool backward() const {return _backward;}
    
    KeyView key() const {
        return KeyView(_iter->key());
    }
    std::string_view value() const {
        leveldb::Slice v = _iter->value();
        return std::string_view(v.data(), v.size());
    }
    

    
    
    
protected:
    
    std::unique_ptr<leveldb::Iterator> _iter;
    KeyView _begin;
    KeyView _end;
    bool _include_begin;
    bool _include_end;
    bool _backward;
    State _state;
    
    bool check_filter() const {
        return static_cast<const Impl *>(this)->filter();
    }
};

class IteratorRaw: public IteratorBase<IteratorRaw> {
public:
    using IteratorBase<IteratorRaw>::IteratorBase;

    static constexpr bool filter() {return true;} 
   
};


}



#endif /* SRC_DOCDB_ITERATOR_H_ */
