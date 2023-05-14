#pragma once
#ifndef SRC_DOCDB_KEY_H_
#define SRC_DOCDB_KEY_H_
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include <leveldb/slice.h>
#include <typeinfo>


#include "types.h"

namespace docdb {

using KeyspaceID = std::uint8_t;


template<typename Container>
class Key_Base {
public:
    Key_Base()=default;
    explicit Key_Base(Container c):_k(std::move(c)) {}
    Key_Base(const leveldb::Slice slice):_k(slice.data(), slice.size()) {}

    
    KeyspaceID keyspace() const;
    
    std::size_t size() const;
    
    bool operator==(const Key_Base &other) const;
    bool operator!=(const Key_Base &other) const;
    bool operator>=(const Key_Base &other) const;
    bool operator<=(const Key_Base &other) const;
    bool operator>(const Key_Base &other) const;
    bool operator<(const Key_Base &other) const;
    
    int compare(const Key_Base &other) const {
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

    Iterator begin() const {auto iter = _k.begin(); ++iter; return iter;}
    Iterator end() const {return _k.end();}


protected:
    Container _k;
    
    
};



using KeyView = Key_Base<std::string_view>;



class Key: public Key_Base<std::string> {
public:
    using Key_Base<std::string>::Key_Base;
    
    Key();
    Key(KeyspaceID id);
    Key(KeyspaceID id, std::string_view content);
    
    void clear() {_k.resize(1);}
    
    
    void keyspace(KeyspaceID id);
    using Key_Base<std::string>::keyspace;

    KeyView view() const {
        return KeyView(std::string_view(_k));
    }
    operator KeyView() const {
        return view();
    }
    
    void append(const std::string_view &v) {
        _k.append(v);
    }
    void push_back(char c) {
        _k.push_back(c);
    }

    void resize(std::size_t s) {
        _k.resize(s+1);
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
inline KeyspaceID Key_Base<Container>::keyspace() const {
    
    return static_cast<KeyspaceID>(_k[0]);
}


template<typename Container>
inline std::size_t Key_Base<Container>::size() const {
    return _k.size()-1;
}

template<typename Container>
inline bool Key_Base<Container>::operator ==(const Key_Base &other) const {
    return _k == other._k;
}

template<typename Container>
inline bool Key_Base<Container>::operator !=(const Key_Base &other) const {
    return _k != other._k;
}

template<typename Container>
inline bool Key_Base<Container>::operator >=(const Key_Base &other) const {
    return _k >= other._k;
}

template<typename Container>
inline bool Key_Base<Container>::operator <=(const Key_Base &other) const {
    return _k <= other._k;
}

template<typename Container>
inline bool Key_Base<Container>::operator >(const Key_Base &other) const {
    return _k > other._k;
}

template<typename Container>
inline bool Key_Base<Container>::operator <(const Key_Base &other) const {
    return _k < other._k;
}



template<typename Container>
template<typename ... Args>
inline typename Key_Base<Container>::Iterator Key_Base<Container>::extract(Args ... args) {
    return structured::extract(begin(), end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Key_Base<Container>::Iterator Key_Base<Container>::extract_from(Iterator from, Args ... args) {
    return structured::extract(from, end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Key_Base<Container>::Iterator Key_Base<Container>::extract_untagged( Args ... args) {
    return structured::extract_untagged(begin(), end(), std::forward<Args>(args)...);
}

template<typename Container>
template<typename ... Args>
inline typename Key_Base<Container>::Iterator Key_Base<Container>::extract_untagged_from(
        Iterator from, Args ... args) {
    return structured::extract_untagged(from, end(), std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Key::set_untagged(Args ... args) {
    clear();
    append_untagged(std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Key::append_untagged(Args ... args) {
    structured::build(std::back_inserter(_k), std::forward<Args>(args)...);
}

template<typename ... Args>
inline void Key::set(Args ... args) {
    clear();
    append(std::forward<Args>(args)...);
}
template<typename ... Args>
inline void Key::append(Args ... args) {
    structured::build_untagged(std::back_inserter(_k), std::forward<Args>(args)...);
}


inline Key::Key(KeyspaceID id) {
    _k.push_back(id);
}
inline Key::Key():Key(0xFF) {}

inline Key::Key(KeyspaceID id, std::string_view content) {
    _k.reserve(content.size()+1);
    _k.push_back(id);
    _k.append(content);
}



inline void Key::keyspace(KeyspaceID id) {
    _k[0] = static_cast<char>(id);
}




}
#endif /* SRC_DOCDB_KEY_H_ */
