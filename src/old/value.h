/*
 * value.h
 *
 *  Created on: 15. 11. 2022
 *      Author: ondra
 */

#ifndef SRC_DOCDB_VALUE_H_
#define SRC_DOCDB_VALUE_H_
#include <leveldb/slice.h>

namespace docdb {

using KeyspaceID = std::uint8_t;


template<typename Container>
class Value_Base {
public:
    Value_Base()=default;
    explicit Value_Base(Container c):_k(std::move(c)) {}
    Value_Base(const leveldb::Slice slice):_k(slice.data(), slice.size()) {}

    
    std::size_t size() const;
    
    bool operator==(const Value_Base &other) const;
    bool operator!=(const Value_Base &other) const;
    bool operator>=(const Value_Base &other) const;
    bool operator<=(const Value_Base &other) const;
    bool operator>(const Value_Base &other) const;
    bool operator<(const Value_Base &other) const;
    
    int compare(const Value_Base &other) const {
        return _k.compare(other._k);
    }

    const Container &get_raw() const {
        return _k;
    }
    Container &get_raw() {
        return _k;
    }
    std::string_view get_key_content() const {
        return std::string_view(_k).substr(1);
    }
    
    
    operator leveldb::Slice() const {
        return leveldb::Slice(_k.data(), _k.size());
    }    
    
    using Iterator = typename Container::const_iterator;
    
    ///extract fields from the key if the fields was stored with tags
    /**
     * @param args list of variables, must be in correct type. Use nullptr
     * to skip certain field
     * @return iterator where reading stops
     */
    template<typename ... Args> Iterator extract(Args ... args);


    ///Extract from specified point
    template<typename ... Args> Iterator extract_from(Iterator from, Args ... args);

    ///extract fields if they were stored untagged
    /** Untagged fields must be extracted as the same time as 
     * they were stored, because tags are not stored and there is no
     * other way to determine type of fields
     * 
     * @param args
     */
    template<typename ... Args> Iterator extract_untagged(Args ... args);
    
    
    ///Extract from specified point
    template<typename ... Args> Iterator extract_untagged_from(Iterator from, Args ... args);

    Iterator begin() const {return _k.begin();}
    Iterator end() const {return _k.end();}


protected:
    Container _k;
    
    
};



using ValueView = Value_Base<std::string_view>;



class Value: public Value_Base<std::string> {
public:
    using Value_Base<std::string>::Value_Base;
    Value(std::string_view content):Value_Base<std::string>(std::string(content)) {} 
    
    void clear() {_k.clear();}
    
    ValueView view() const {
        return ValueView (std::string_view(_k));
    }
    operator ValueView() const {
        return view();
    }
    
    void append(const std::string_view &v) {
        _k.append(v);
    }
    void push_back(char c) {
        _k.push_back(c);
    }

    void resize(std::size_t s) {
        _k.resize(s);
    }

    ///Set multicolumn key
    template<typename ... Args> void set(Args ... args);
    ///append more columns
    template<typename ... Args> void append(Args ... args);
    
    ///Set multicolumn key
    template<typename ... Args> void set_untagged(Args ... args);
    ///append more columns
    template<typename ... Args> void append_untagged(Args ... args);

};



template<typename Container>
inline std::size_t Value_Base<Container>::size() const {
    return _k.size();
}

template<typename Container>
inline bool Value_Base<Container>::operator ==(const Value_Base &other) const {
    return _k == other._k;
}

template<typename Container>
inline bool Value_Base<Container>::operator !=(const Value_Base &other) const {
    return _k != other._k;
}

template<typename Container>
inline bool Value_Base<Container>::operator >=(const Value_Base &other) const {
    return _k >= other._k;
}

template<typename Container>
inline bool Value_Base<Container>::operator <=(const Value_Base &other) const {
    return _k <= other._k;
}

template<typename Container>
inline bool Value_Base<Container>::operator >(const Value_Base &other) const {
    return _k > other._k;
}

template<typename Container>
inline bool Value_Base<Container>::operator <(const Value_Base &other) const {
    return _k < other._k;
}



template<typename Container>
template<typename ... Args>
inline typename Value_Base<Container>::Iterator Value_Base<Container>::extract(Args ... args) {
    return structured::extract(begin(), end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Value_Base<Container>::Iterator Value_Base<Container>::extract_from(Iterator from, Args ... args) {
    return structured::extract(from, end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Value_Base<Container>::Iterator Value_Base<Container>::extract_untagged( Args ... args) {
    return structured::extract_untagged(begin(), end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Value_Base<Container>::Iterator Value_Base<Container>::extract_untagged_from(
        Iterator from, Args ... args) {
    return structured::extract_untagged(from, end(), std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Value::set_untagged(Args ... args) {
    clear();
    append_untagged(std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Value::append_untagged(Args ... args) {
    structured::build(std::back_inserter(_k), std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Value::set(Args ... args) {
    clear();
    append(std::forward<Args>(args)...);
}
template<typename ... Args>
inline void Value::append(Args ... args) {
    structured::build_untagged(std::back_inserter(_k), std::forward<Args>(args)...);
}






}



#endif /* SRC_DOCDB_VALUE_H_ */
