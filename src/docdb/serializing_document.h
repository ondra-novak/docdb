#ifndef SRC_DOCDB_SERIALIZING_DOCUMENT_H_
#define SRC_DOCDB_SERIALIZING_DOCUMENT_H_

#include "concepts.h"

namespace docdb {

template<typename Obj, typename Ser>
DOCDB_CXX20_CONCEPT(IsSerializableMethod, requires(Obj &obj, Ser &ser){
    Obj::serialize(obj,ser);
});

template<typename Obj, typename Ser>
DOCDB_CXX20_CONCEPT(IsSerializableGlobal, requires(Obj &obj, Ser &ser){
    {serialize(obj,ser)};
});



template<typename Iter>
class ToBinarySerializer {
public:

    ToBinarySerializer(Iter &insert):iter(insert) {}

    void write_binary(char r) {
        *iter = r;
        ++iter;
    }

    void write_binary(unsigned char r) {
        *iter = static_cast<char>(r);
        ++iter;
    }

    template<typename X>
    void write_binary(const X &r) {
        const char *ptr = reinterpret_cast<const char *>(&r);
        iter = std::copy(ptr, ptr+sizeof(X), iter);
    }

    auto operator()() {
        return true;
    }

    template<typename X, typename ... Args>
    auto operator()(const X &item, const Args &... args) {
        if constexpr(IsSerializableMethod<const X, ToBinarySerializer>) {
            bool r1 = X::serialize(item, *this);
            return r1 && this->operator ()(args...);
        } else if constexpr(IsSerializableGlobal<const X, ToBinarySerializer>) {
            bool r1 = serialize(item, *this);
            return r1 && this->operator ()(args...);
        } else {
            static_assert(std::is_trivially_copy_constructible_v<X>, "Type must be trivially copy constructible, or it must define public static method serialize(_this,arch)");
            write_binary(item);
            return true;
        }
    }



protected:
    Iter &iter;
};

template<typename Iter>
class FromBinarySerializer {
public:

    FromBinarySerializer(Iter &at, Iter end):iter(at), end(end) {}

    auto read_binary(char &r) {
        if (iter == end) return false;
        r = *iter;
        ++iter;
        return true;
    }

    auto read_binary(unsigned char &r) {
        if (iter == end) return false;
        r = *iter;
        ++iter;
        return true;
    }

    template<typename X>
    auto read_binary(X &r) {
        if (std::distance(iter,end) < static_cast<std::intptr_t>(sizeof(X))) {
            return false;
        }
        char *ptr = reinterpret_cast<char *>(&r);
        auto endcpy = iter;
        std::advance(endcpy, sizeof(X));
        iter =std::copy(iter,endcpy, ptr);
        return true;
    }

    auto operator()() {
        return true;
    }

    template<typename X, typename ... Args>
    auto operator()(X &item, Args & ... args) {
        if constexpr(IsSerializableMethod<X, FromBinarySerializer>) {
            bool r1 = X::serialize(item, *this);
            return r1 && this->operator ()(args...);
        } else if constexpr(IsSerializableGlobal<X, FromBinarySerializer>) {
            bool r1 = serialize(item, *this);
            return r1 && this->operator ()(args...);
        } else {
            static_assert(std::is_trivially_copy_constructible_v<X>, "Type must be trivially copy constructible, or it must define public static method serialize(_this,arch)");
            return read_binary(item);
        }
    }




protected:
    Iter &iter;
    Iter end;

};

template<typename T>
DOCDB_CXX20_CONCEPT(ReadingSerializer, requires(T arch){
    arch.read_binary(std::declval<int &>());
});

template<typename T>
DOCDB_CXX20_CONCEPT(WritingSerializer, requires(T arch){
    arch.write_binary(std::declval<int>());
});



template<typename T>
struct SerializingDocument {
    using Type = T;

    template<typename Iter>
    static Iter to_binary(const Type &src, Iter insert) {
        ToBinarySerializer<Iter> arch(insert);
        arch(src);
        return insert;
    }

    template<typename Iter>
    static Type from_binary(Iter beg, Iter end) {
        Type out;
        FromBinarySerializer<Iter> arch(beg, end);
        arch(out);
        return out;
    }
};

template<IsContainer Cont, WritingSerializer Arch>
bool serialize(const Cont &s, Arch &arch) {
    std::size_t sz = s.size();
    unsigned char bytes = 0;
    while (sz) {
        bytes++;
        sz >>= 8;
    }
    arch.write_binary(bytes);
    sz = s.size();
    for (unsigned char i = 0; i < bytes; ++i) {
        arch.write_binary(static_cast<unsigned char>(sz & 0xFF));
        sz >>= 8;
    }
    for (const auto &c: s) {
        arch(c);
    }
    return true;
}

template<IsContainer Cont, ReadingSerializer Arch>
bool serialize(Cont &s, Arch &arch) {
    unsigned char bytes;
    if (!arch.read_binary(bytes)) return false;
    std::size_t sz = 0;
    for (unsigned char i = 0; i < bytes; ++i) {
        unsigned char c;
        if (!arch.read_binary(c)) return false;
        sz = (sz << 8) | c;
    }
    s.resize(sz);
    for (auto &c :s) {
        if (!arch.read_binary(c)) return false;
    }
    return true;
}


}



#endif /* SRC_DOCDB_SERIALIZING_DOCUMENT_H_ */
