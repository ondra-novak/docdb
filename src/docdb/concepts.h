#pragma once
#ifndef SRC_DOCDB_CONCEPTS_H_
#define SRC_DOCDB_CONCEPTS_H_

#include <concepts>
#include <type_traits>
#include <utility>


namespace docdb {


/*
 Some IDEs (Eclipse for example) fails to parse concepts
 So we introduce a macro as workaround
 Please add any other macros emited by IDEs' parsers if you
 find self in the same situation
*/

#if defined( __CDT_PARSER__)
#define CXX20_REQUIRES(...)
#define CXX20_CONCEPT(type, ...) struct type {}
#else
#define CXX20_REQUIRES(...) requires (__VA_ARGS__)
#define CXX20_CONCEPT(type, ...) concept type = __VA_ARGS__
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
CXX20_CONCEPT(ToBinaryConvertible, requires(const typename T::Type& val, WriteIteratorConcept iter) {
    { T::to_binary(val, iter) } -> std::same_as<void>;
});

template<typename T>
CXX20_CONCEPT(FromBinaryConvertible,requires(ReadIteratorConcept b, ReadIteratorConcept e) {
    { T::from_binary(b, e) } -> std::same_as<typename T::Type>;
});

template<typename T>
CXX20_CONCEPT(DocumentDef,ToBinaryConvertible<T> && FromBinaryConvertible<T>);

template<typename T>
CXX20_CONCEPT(IsTuple, requires {
  typename std::tuple_size<T>::type;
  typename std::tuple_element<0, T>::type;
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
CXX20_CONCEPT(DocumentWrapper, std::is_constructible_v<T, decltype([](std::string &)->bool{})>);


}



#endif /* SRC_DOCDB_CONCEPTS_H_ */
