#pragma once
#ifndef SRC_DOCDB_KEY_H_
#define SRC_DOCDB_KEY_H_
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

#include <leveldb/slice.h>
#include <typeinfo>


namespace docdb {

using KeyspaceID = std::uint8_t;


namespace keycontent {


using iterator = const char *; 

std::uint8_t read_uint8(iterator &iter);
std::uint16_t read_uint16(iterator &iter);
std::uint32_t read_uint32(iterator &iter);
std::uint64_t read_uint64(iterator &iter);
void read_string(std::string &out, iterator &iter);

}
template<typename Container>
class Key_Base {
public:
    Key_Base()=default;
    explicit Key_Base(Container c):_k(std::move(c)) {}
    Key_Base(const leveldb::Slice slice):_k(slice.data(), slice.size()) {}

    using iterator = keycontent::iterator;
    
    KeyspaceID keyspace() const;
    iterator begin() const;
    iterator end() const;
    
    static std::uint8_t read_uint8(iterator &iter) {return keycontent::read_uint8(iter);}
    static std::uint16_t read_uint16(iterator &iter)  {return keycontent::read_uint16(iter);}
    static std::uint32_t read_uint32(iterator &iter)  {return keycontent::read_uint32(iter);}
    static std::uint64_t read_uint64(iterator &iter)  {return keycontent::read_uint64(iter);}
    static void read_string_append(std::string &out, iterator &iter) {return keycontent::read_string(out, iter);}
    static void read_string(iterator &iter, std::string &out) {out.clear();return keycontent::read_string(out, iter);}
    
    template<typename T>
    static T read(iterator &iter) {
        if constexpr(sizeof(T) == 1) {
            return static_cast<T>(read_uint8(iter));
        } else if constexpr(sizeof(T) == 2) {
            return static_cast<T>(read_uint16(iter));
        } else if constexpr(sizeof(T) == 4) {
            return static_cast<T>(read_uint32(iter));
        } else if constexpr(sizeof(T) == 8) {
            return static_cast<T>(read_uint64(iter));
        }
    }
    
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
    
    ///extract fields from the key if the key is structured
    /**
     * @param args list of variables, must be in correct type. Use nullptr
     * to skip certain field
     */
    template<typename ... Args> void extract(Args ... args);

    


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
    
    void clear();
    
    void add_raw(std::string_view content);
    void add(std::uint8_t x);
    void add(std::uint16_t x);
    void add(std::uint32_t x);
    void add(std::uint64_t x);
    void add(std::string_view text);
    
    void keyspace(KeyspaceID id);
    using Key_Base<std::string>::keyspace;

    KeyView view() const {
        return KeyView(std::string_view(_k));
    }
    operator KeyView() const {
        return view();
    }

    ///Set key content as structured key
    template<typename ... Args> void set(Args ... args);
    

};


template<typename Container>
inline KeyspaceID Key_Base<Container>::keyspace() const {
    
    return static_cast<KeyspaceID>(_k[0]);
}

template<typename Container>
inline typename Key_Base<Container>::iterator Key_Base<Container>::begin() const {
    return _k.data()+1;
}

template<typename Container>
inline typename Key_Base<Container>::iterator Key_Base<Container>::end() const {
    return _k.data()+_k.size();
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
inline void Key_Base<Container>::extract(Args ... args) {    
}

template<typename ... Args>
inline void Key::set(Args ... args) {
}


}

#endif /* SRC_DOCDB_KEY_H_ */
