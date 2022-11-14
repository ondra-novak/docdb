#pragma once
#ifndef SRC_DOCDB_TYPES_H_
#define SRC_DOCDB_TYPES_H_
#include "number.h"

#include <algorithm>
#include <string>
#include <type_traits>
#include <variant>

namespace docdb {


///Pass this variable to extract to determine, whether field is null
class IsNull {
public:
    IsNull():res(true) {}
    IsNull(bool r):res(r) {}       
    
    operator bool() const {return res;}
    
protected:
    bool res;
};

template<typename T>
union BinHelper {
    T val;
    char bin[sizeof(T)];
};

template<typename Iter, typename T> Iter load_bigendian(BinHelper<T> &hlp, Iter iter) {
    for (unsigned int i = 0; i < sizeof(T); i++) {
        hlp.bin[sizeof(T)-i-1] = *iter;
        ++iter;
    }
    return iter;
}


template<typename UInt> struct UIntHelper { 
template<typename Iter, typename T> static Iter extract(Iter beg, Iter end, T&& t) {
    if (std::distance(beg, end) < sizeof(UInt)) throw std::bad_cast();
    auto iter = beg+sizeof(UInt);
    if constexpr(std::is_same_v<T, IsNull &>) {
        t = IsNull(false);return iter;
    } else if constexpr(std::is_null_pointer_v<T>) {
        return iter;
    } else if constexpr(std::is_same_v<T, UInt &>) {
        BinHelper<UInt> hlp;
        beg = load_bigendian(hlp, beg);
        t = hlp.val;
        return beg;
    } else if constexpr(std::is_assignable_v<T , std::string>) {
        UInt v;
        auto res = extract(beg, end, v);
        t = std::to_string(v);
        return res;        
    } else if constexpr(std::is_assignable_v<T , UInt>) {
        UInt v;
        auto res = extract<Iter, UInt &>(beg, end, v);
        t = v;
        return res;
    } else {
        throw std::bad_cast();
    }
}
template<typename Iter> static Iter build(Iter where, UInt t) {
    BinHelper<UInt> hlp;
    hlp.val = t;
    for (int i = 0; i < sizeof(UInt); i++) {
        *where = hlp.bin[sizeof(UInt)-i-1];
        where++;
    }
    return where;
}
};




enum class FieldType: std::uint8_t {
    null,
    bool_true,
    bool_false,
    number_float,
    number_double,
    number_uint8,
    number_uint16,
    number_uint32,
    number_uint64,    
    stringz,
    small_blob,
    blob,
    large_blob,
    huge_blob
};


template<typename SizeType, FieldType codepoint> class BlobT: public std::basic_string<char> {
public:
    using std::basic_string<char>::basic_string;

    template<typename Iter> 
    static Iter skip(Iter beg, Iter end) {
        SizeType sz;
        beg = UIntHelper<SizeType>::extract(beg, end, sz);
        sz = std::min(sz, std::distance(beg, end));
        beg = beg + sz;
        return beg;
    }
    
    template<typename Iter, typename T> 
    static Iter extract(Iter beg, Iter end, T&& t) {
        if constexpr(std::is_same_v<T, IsNull &>) {            
            t = IsNull(false);
            return skip(beg, end);
        }
        else if constexpr(std::is_null_pointer_v<T>) {
            return skip(beg,end);
        }
        else if constexpr(std::is_base_of_v<std::string, std::remove_reference_t<T> >) {
            SizeType sz;            
            beg = UIntHelper<SizeType>::template extract<Iter,SizeType &>(beg, end, sz);
            t.clear();
            t.reserve(sz);
            for (SizeType i = 0; i < sz; i++) {
                if (beg == end) break;
                t.push_back(*beg);
                ++beg;                
            }
            return beg;
        }
        else if constexpr(std::is_assignable_v<T, std::string>) {
            std::string s;
            auto res = extract(beg, end, s);
            t = s;
            return res;
        }
        else {
            throw std::bad_cast();
        }
    }
    template<typename Iter>
    static Iter build(Iter where, const std::string &blob) {
        SizeType sz = static_cast<SizeType>(std::min<std::string::size_type>(SizeType(-1),blob.size()));
        where = UIntHelper<SizeType>::build(where, sz);
        for (SizeType i = 0; i < sz; i++) {
            *where = blob[i];
            ++where;
        }
        return where;
    }
    
    template<typename Arch>
    static void serialize(BlobT &b, Arch &arch) {
        if (*arch.iterator != codepoint) throw std::bad_cast();
        ++arch.iterator;
        SizeType sz;
        ;
        arch.iterator = _numbers::read_unsigned_int(arch.iterator, sz);
        b.clear();
        b.reserve(sz);
        for (SizeType i = 0; i <sz; i++) {
            b.push_back(static_casg<char>(*arch.iterator));
            ++arch.iterator;
        }
    }
    
    template<typename Arch>
    static void serialize(const BlobT &b, Arch &arch) {
        *arch.iterator = codepoint;
        ++arch.iterator;
        SizeType sz = static_cast<SizeType>(b.length());
        _numbers::write_unsigned_int(arch.iterator, sz);
        for (SizeType i = 0; i < sz; i++) {
            *arch.iterator = b[i];
            ++arch.iterator;
        }
    }    
};

using SmallBlob = BlobT<std::uint8_t, FieldType::small_blob>;
using Blob = BlobT<std::uint16_t, FieldType::blob>;
using LargeBlob = BlobT<std::uint32_t, FieldType::large_blob>;
using HugeBlob = BlobT<std::uint64_t, FieldType::huge_blob>;


namespace structured {

template<typename T> void extract_bool(T&& t, bool val) {
    if constexpr(std::is_same_v<T, IsNull &>) t = IsNull(false);
    else if constexpr(std::is_null_pointer_v<T>) return; 
    else if constexpr(std::is_assignable_v<T,std::string>) t = val?std::string("true"):std::string("false"); 
    else if constexpr(std::is_assignable_v<T,bool>) t = val; 
    else throw std::bad_cast();
}

template<typename T> void extract_null(T&& t) {
    if constexpr(std::is_same_v<T, IsNull &>) t = IsNull(true);
    else if constexpr(std::is_null_pointer_v<T>) return; 
    else if constexpr(std::is_assignable_v< T,std::string>) t = "null"; 
    else throw std::bad_cast();
}

template<typename Iter> Iter skip_stringz(Iter beg, Iter end) {
    while (beg != end) {
        char c = *beg;
        ++beg;
        if (c == 0) break;
    }
    return beg;
}
template<typename Iter, typename T> Iter extract_stringz(Iter beg, Iter end, T&& t) {
    if constexpr(std::is_same_v<T, IsNull &>) {
        t = IsNull(false); return skip_stringz(beg, end);
    } else if constexpr(std::is_null_pointer_v<T>) { 
        return skip_stringz(beg, end);
    } else if constexpr(std::is_same_v<T, std::string &>) {
        t.clear();
        while (beg != end) {
            char c = *beg;
            ++beg;
            if (c == 0) break;
            t.push_back(c);
        }
        return beg;
    } else if constexpr(std::is_assignable_v<T,std::string>) {
        std::string s;
        auto res = extract_stringz(beg, end, s);
        t = s;
        return res;
    } else throw std::bad_cast();
}

template<typename Iter> Iter build_stringz(Iter where, const char *c) {
    while (*c) {
        *where = *c;
        ++where;
        ++c;
    }
    *where = '\0';
    ++where;
    return where;
    
}

template<typename Iter> Iter build_stringz(Iter where, const std::string_view s) {
    for (char c: s) {
        if (!c) break;
        *where = c;
        ++where;        
    }
    *where = '\0';
    ++where;
    return where;
    
}

template<typename Real> struct NumberHelper { 
template<typename Iter, typename T> static Iter extract(Iter beg, Iter end, T &&t) {
    if (std::distance(beg, end) < sizeof(Real)) throw std::bad_cast();
    auto iter = beg+sizeof(Real);
    if constexpr(std::is_same_v<T, IsNull &>) {
        t = IsNull(false);return iter;
    } else if constexpr(std::is_null_pointer_v<T>) {
        return iter;
    } else if constexpr(std::is_same_v<T, Real &>) {
        BinHelper<Real> hlp;
        beg = load_bigendian(hlp, beg);
        t = -hlp.val;
        return beg;
    } else if constexpr(std::is_assignable_v< T,std::string>) {
        Real v;
        auto res = extract(beg, end, v);
        t = std::to_string(v);
        return res;        
    } else if constexpr(std::is_assignable_v<T,Real>) {
        Real v;
        auto res = extract<Iter, Real &>(beg, end, v);
        t = v;
        return res;
    } else {
        throw std::bad_cast();
    }
}
template<typename Iter> static Iter build(Iter where, Real t) {
    BinHelper<Real> hlp;
    hlp.val = -t;
    for (int i = 0; i < sizeof(Real); i++) {
        *where = hlp.bin[sizeof(Real)-i-1];
        where++;
    }
    return where;
}

};


template<typename Iter>
Iter extract(Iter beg, Iter end) {
    return beg;
}


template<typename Iter, typename T, typename ... Args>
Iter extract(Iter beg, Iter end, T &&var, Args ... args) {
    if (beg == end) throw std::bad_cast();
    FieldType f = static_cast<FieldType>(*beg);
    ++beg;
    Iter iter = beg;
    switch(f) {
        case FieldType::blob: iter = Blob::extract(beg, end, var);break;
        case FieldType::small_blob: iter = SmallBlob::extract(beg, end, var);break;
        case FieldType::huge_blob: iter = HugeBlob::extract(beg, end, var);break;
        case FieldType::large_blob: iter = LargeBlob::extract(beg, end, var);break;
        case FieldType::bool_false: extract_bool(var, false);break;
        case FieldType::bool_true: extract_bool(var, true);break;
        case FieldType::null: extract_null(var); break;
        case FieldType::number_double: iter=NumberHelper<double>::extract(beg, end, var);break; 
        case FieldType::number_float: iter=NumberHelper<float>::extract(beg, end, var);break; 
        case FieldType::number_uint8: iter=UIntHelper<std::uint8_t>::extract(beg, end, var);break; 
        case FieldType::number_uint16: iter=UIntHelper<std::uint16_t>::extract(beg, end, var);break;
        case FieldType::number_uint32: iter=UIntHelper<std::uint32_t>::extract(beg, end, var);break;
        case FieldType::number_uint64: iter=UIntHelper<std::uint64_t>::extract(beg, end, var);break;
        case FieldType::stringz: iter=extract_stringz(beg, end, var);break;
        default: throw std::bad_cast();
    }
    return extract(iter, end, std::forward<Args>(args)...);
}

template<typename T> class undefined;

template<typename Iter>
Iter build(Iter where) {
    return where;
}

template<typename Iter, typename T, typename ... Args>
Iter build(Iter where, T &&var, Args &&... args) {
    using Type = std::remove_reference_t<T>;
    if constexpr(std::is_null_pointer_v<Type>) {
        *where = static_cast<std::uint8_t>(FieldType::null);
        ++where;
    } 
    else if constexpr(std::is_same_v<Type, bool>) {
        *where = static_cast<std::uint8_t>(var?FieldType::bool_true:FieldType::bool_false);
        ++where;
    }
    else if constexpr(std::is_same_v<Type, int>) {
        *where = static_cast<std::uint8_t>(FieldType::number_double);
        ++where;
        where = NumberHelper<double>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, long>) {
        *where = static_cast<std::uint8_t>(FieldType::number_double);
        ++where;
        where = NumberHelper<double>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, long long>) {
        *where = static_cast<std::uint8_t>(FieldType::number_double);
        ++where;
        where = NumberHelper<double>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, short>) {
        *where = static_cast<std::uint8_t>(FieldType::number_double);
        ++where;
        where = NumberHelper<double>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, std::uint8_t>) {
        *where = static_cast<std::uint8_t>(FieldType::number_uint8);
        ++where;
        where = UIntHelper<std::uint8_t>::build(where, var);
    }
    else if constexpr(std::is_same_v<Type, std::uint16_t>) {
        *where = static_cast<std::uint8_t>(FieldType::number_uint16);
        ++where;
        where = UIntHelper<std::uint16_t>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, std::uint32_t>) {
        *where = static_cast<std::uint8_t>(FieldType::number_uint32);
        ++where;
        where = UIntHelper<std::uint32_t>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, std::uint64_t>) {
        *where = static_cast<std::uint8_t>(FieldType::number_uint64);
        ++where;
        where = UIntHelper<std::uint64_t>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<SmallBlob, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::small_blob);
        ++where;
        where = SmallBlob::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<Blob, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::blob);
        ++where;
        where = Blob::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<LargeBlob, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::large_blob);
        ++where;
        where = LargeBlob::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<HugeBlob, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::huge_blob);
        ++where;
        where = HugeBlob::build(where, std::forward<Type>(var));
    } 
    else if constexpr(std::is_convertible_v<Type, const char *>) {
        *where = static_cast<std::uint8_t>(FieldType::stringz);
        ++where;
        where = build_stringz(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, double>) {
        *where = static_cast<std::uint8_t>(FieldType::number_double);
        ++where;
        where = NumberHelper<double>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_same_v<Type, float>) {
        *where = static_cast<std::uint8_t>(FieldType::number_float);
        ++where;
        where = NumberHelper<float>::build(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<std::string, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::stringz);
        ++where;
        where = build_stringz(where, std::forward<Type>(var));
    }
    else if constexpr(std::is_base_of_v<std::string_view, Type>) {
        *where = static_cast<std::uint8_t>(FieldType::stringz);
        ++where;
        where = build_stringz(where, std::forward<Type>(var));
    }
    else {
        throw undefined<Type>::undefined();        
    }
    return build(where, std::forward<Args>(args)...);
    
    
}
template<typename ... Args>
void build_string_append(std::string &s, Args && ... args)  {
    build(std::back_inserter(s), std::forward<Args>(args)...);    
}

template<typename ... Args>
void build_string(std::string &s, Args && ... args)  {
    s.clear();
    build_string_append(s, std::forward<Args>(args)...);
}



template<typename Fn>
void to_strings(const std::string_view &content, Fn &&fn) {
    auto beg = content.begin();
    auto end = content.end();
    std::string s;
    while (beg != end) {        
        beg = extract(beg, end, s);
        fn(s);
    }
}

}

}




#endif /* SRC_DOCDB_TYPES_H_ */
