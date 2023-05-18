#pragma once
#ifndef SRC_DOCDB_CONCEPTS_H_
#define SRC_DOCDB_CONCEPTS_H_

#include <concepts>
#include <type_traits>
#include <utility>


namespace docdb {


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
concept ToBinaryConvertible = requires(const typename T::Type& val, WriteIteratorConcept iter) {
    { T::to_binary(val, iter) } -> std::same_as<void>;
};

template<typename T>
concept FromBinaryConvertible = requires(ReadIteratorConcept b, ReadIteratorConcept e) {
    { T::from_binary(b, e) } -> std::same_as<typename T::Type>;
};

template<typename T>
concept DocumentDef = ToBinaryConvertible<T> && FromBinaryConvertible<T>;

template <typename T>
concept IsTuple = requires {
  typename std::tuple_size<T>::type;
  typename std::tuple_element<0, T>::type;
};

}



#endif /* SRC_DOCDB_CONCEPTS_H_ */
