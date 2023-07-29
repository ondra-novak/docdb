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
 Please add any other macros emitted by IDEs' parsers if you
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

};

struct ReadIteratorConcept {
    const char &operator *() const;
    ReadIteratorConcept &operator++();
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



template<int i>
struct ConstInteger {
    static constexpr int value = i;
    static constexpr bool valid = true;
};

struct ConstError {
    static constexpr bool valid = false;
};

template<typename Fn, int min, int max>
class JumpTable {
public:
    static_assert(min <= max);

    using Ret = decltype(std::declval<Fn>()(std::declval<ConstInteger<min> >()));
    static constexpr unsigned int size = max - min + 1;

    constexpr JumpTable() {
        init_jump_table<min>(_jumpTable);
    }

    constexpr Ret visit(int value, Fn &&fn) const  {
        auto index = static_cast<unsigned int>(value - min);
        if (index >= size) fn(ConstError{});
        return _jumpTable[index](std::forward<Fn>(fn));
    }

protected:

    template<int i>
    static Ret call_fn(Fn &&fn) {return fn(ConstInteger<i>());}

    using FnPtr = Ret (*)(Fn &&fn);
    FnPtr _jumpTable[size] = {};

    template<int i>
    constexpr void init_jump_table(FnPtr *ptr) {
        if constexpr(i <= max) {
            *ptr = &call_fn<i>;
            init_jump_table<i+1>(ptr+1);
        }
    }
};

template<int min, int max, typename Fn>
auto number_to_constant(int number, Fn &&fn) {
    static constexpr JumpTable<Fn, min, max> jptable;
    return jptable.visit(number, std::forward<Fn>(fn));
}


template<typename ... T>
struct DeferFalse {
    static constexpr bool val = false;
};

template<typename ... T>
constexpr bool defer_false = DeferFalse<T...>::val;

}

template<typename X>
DOCDB_CXX20_CONCEPT(AggregateFunction, requires (X fn, const typename X::InputType &input) {
    {fn(input)} -> std::convertible_to<typename X::ResultType>;
});





#endif /* SRC_DOCDB_CONCEPTS_H_ */
