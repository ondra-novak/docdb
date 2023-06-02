#pragma once
#ifndef SRC_DOCDB_CONCEPTS_H_
#define SRC_DOCDB_CONCEPTS_H_

#include <concepts>
#include <variant>
#include <type_traits>
#include <utility>
#include <string>


namespace docdb {


/*
 Some IDEs (Eclipse for example) fails to parse concepts
 So we introduce a macro as workaround
 Please add any other macros emited by IDEs' parsers if you
 find self in the same situation
*/

#if defined( __CDT_PARSER__)
#define DOCDB_CXX20_REQUIRES(...)
#define DOCDB_CXX20_CONCEPT(type, ...) struct type {}
#else
#define DOCDB_CXX20_REQUIRES(...) requires (__VA_ARGS__)
#define DOCDB_CXX20_CONCEPT(type, ...) concept type = __VA_ARGS__
#endif

template<typename T, typename U>
concept same_or_reference_of = std::is_same_v<std::remove_reference_t<T>, U>;

struct WriteIteratorConcept {
    char &operator *();
    WriteIteratorConcept &operator++();
    bool operator=(const WriteIteratorConcept &other);
};

struct ReadIteratorConcept {
    const char &operator *() const;
    ReadIteratorConcept &operator++();
    bool operator=(const ReadIteratorConcept &other);
};


template<typename T>
DOCDB_CXX20_CONCEPT(ToBinaryConvertible, requires(const typename T::Type& val, WriteIteratorConcept iter) {
    { T::to_binary(val, iter) } -> std::same_as<WriteIteratorConcept>;
});

template<typename T>
DOCDB_CXX20_CONCEPT(FromBinaryConvertible,requires(ReadIteratorConcept b, ReadIteratorConcept e) {
    { T::from_binary(b, e) } -> std::same_as<typename T::Type>;
});

template<typename T>
DOCDB_CXX20_CONCEPT(DocumentDef,ToBinaryConvertible<T> && FromBinaryConvertible<T>);

template<typename T>
DOCDB_CXX20_CONCEPT(DocumentCustomDeleted, requires(const typename T::Type &x){
    {T::is_deleted(x)} -> std::convertible_to<bool>;
});



template<typename T>
DOCDB_CXX20_CONCEPT(IsTuple, requires {
  typename std::tuple_size<T>::type;
  typename std::tuple_element<0, T>::type;
});

template<typename T>
DOCDB_CXX20_CONCEPT(IsContainer, requires(T x) {
  typename T::value_type;
  {x.begin() } -> std::input_iterator;
  {x.end() } -> std::input_iterator;
  {x.push_back(std::declval<typename T::value_type>())};
});

template<typename T>
DOCDB_CXX20_CONCEPT(HasReserveFunction, requires(T x) {
    {x.reserve(std::declval<std::size_t>())};
});


template<typename T>
DOCDB_CXX20_CONCEPT(IsVariant,requires {
    typename std::variant_size<T>::type;
});
///DocumentWrapper
/**
 * Object responsible to store document and keep its binary data. There is
 * one class which has these requirements : Document<DocumentDef>. The class
 * must have a constructor, which accepts function which accepts std::string reference and
 * returns boolean. The string value is filled with binary data, and return value is
 * true when operation succeed or false if failed (so content of buffer is invalid)
 * @tparam T
 */
template<typename T>
DOCDB_CXX20_CONCEPT(DocumentWrapper, std::is_constructible_v<T, decltype([](std::string &)->bool{return true;})>);


template<typename T>
DOCDB_CXX20_CONCEPT(DocumentDefHasConstructType, requires {
    typename T::ConstructType;
});

template<typename T, bool>
struct DocConstructType {
    using Type = typename T::Type;
};

template<typename T>
struct DocConstructType<T,true> {
    using Type = typename T::ConstructType;
};

template<typename T>
using DocConstructType_t = typename DocConstructType<T, DocumentDefHasConstructType<T> >::Type;

template<template<int> class X>
struct ByteToIntegralType {

    template<int idx, typename Fn>
    static auto finalize(Fn && fn) {
        return fn(X<idx>());
    }

    template<int group, typename Fn>
    static auto jump_table_1(std::uint8_t index, Fn &&fn) {
        constexpr auto base = group << 4;
        switch (index) {
            case base+0: return finalize<base+0>(std::forward<Fn>(fn));
            case base+1: return finalize<base+1>(std::forward<Fn>(fn));
            case base+2: return finalize<base+2>(std::forward<Fn>(fn));
            case base+3: return finalize<base+3>(std::forward<Fn>(fn));
            case base+4: return finalize<base+4>(std::forward<Fn>(fn));
            case base+5: return finalize<base+5>(std::forward<Fn>(fn));
            case base+6: return finalize<base+6>(std::forward<Fn>(fn));
            case base+7: return finalize<base+7>(std::forward<Fn>(fn));
            case base+8: return finalize<base+8>(std::forward<Fn>(fn));
            case base+9: return finalize<base+9>(std::forward<Fn>(fn));
            case base+10: return finalize<base+10>(std::forward<Fn>(fn));
            case base+11: return finalize<base+11>(std::forward<Fn>(fn));
            case base+12: return finalize<base+12>(std::forward<Fn>(fn));
            case base+13: return finalize<base+13>(std::forward<Fn>(fn));
            case base+14: return finalize<base+14>(std::forward<Fn>(fn));
            case base+15: return finalize<base+15>(std::forward<Fn>(fn));
            default: throw;
        }
    }


    template<typename Fn>
    auto visit(Fn &&visitor, std::uint8_t index) {
        switch (index >> 4) {
            case 0: return jump_table_1<0>(index, std::forward<Fn>(visitor));
            case 1: return jump_table_1<1>(index, std::forward<Fn>(visitor));
            case 2: return jump_table_1<2>(index, std::forward<Fn>(visitor));
            case 3: return jump_table_1<3>(index, std::forward<Fn>(visitor));
            case 4: return jump_table_1<4>(index, std::forward<Fn>(visitor));
            case 5: return jump_table_1<5>(index, std::forward<Fn>(visitor));
            case 6: return jump_table_1<6>(index, std::forward<Fn>(visitor));
            case 7: return jump_table_1<7>(index, std::forward<Fn>(visitor));
            case 8: return jump_table_1<8>(index, std::forward<Fn>(visitor));
            case 9: return jump_table_1<9>(index, std::forward<Fn>(visitor));
            case 10: return jump_table_1<10>(index, std::forward<Fn>(visitor));
            case 11: return jump_table_1<11>(index, std::forward<Fn>(visitor));
            case 12: return jump_table_1<12>(index, std::forward<Fn>(visitor));
            case 13: return jump_table_1<13>(index, std::forward<Fn>(visitor));
            case 14: return jump_table_1<14>(index, std::forward<Fn>(visitor));
            case 15: return jump_table_1<15>(index, std::forward<Fn>(visitor));
            default: throw;
        }
    }
};

template<typename ... T>
struct DeferFalse {
    static constexpr bool val = false;
};

template<typename ... T>
constexpr bool defer_false = DeferFalse<T...>::val;

}



#endif /* SRC_DOCDB_CONCEPTS_H_ */
