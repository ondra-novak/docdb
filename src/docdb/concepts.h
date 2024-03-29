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
    WriteIteratorConcept operator++(int);

};

struct ReadIteratorConcept {
    const char &operator *() const;
    ReadIteratorConcept &operator++();
    ReadIteratorConcept operator++(int);
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

template<typename _Type>
struct TypeToDocument {
    using Type = _Type;
    template<typename Iter>
    static _Type from_binary(Iter &at, Iter end);
    template<typename Iter>
    static Iter to_binary(const _Type &doc, Iter pb);
};


template<typename T>
DOCDB_CXX20_CONCEPT(IsTuple, requires {
  typename std::tuple_size<T>::type;
  typename std::tuple_element<0, T>::type;
});

///Tests, whether Args ... contains 1 argument, which is std::tuple<...>
template<typename ... Args>
DOCDB_CXX20_CONCEPT(IsTuple1Arg, sizeof...(Args) == 1 && (IsTuple<Args> || ...));

///Tests, whether Args ... contains more then 1 argument or if it is one argument, it is not std::tuple<...>
template<typename ... Args>
DOCDB_CXX20_CONCEPT(IsNotTuple1Arg, !IsTuple1Arg<Args...>);


template<typename T>
DOCDB_CXX20_CONCEPT(IsContainer, requires(T x) {
  typename T::value_type;
  {x.begin() } -> std::forward_iterator;
  {x.end() } -> std::forward_iterator;
  {x.push_back(std::declval<typename T::value_type>())};
  {x.reserve(std::distance(x.begin(),x.end()))};
});

template<typename T>
DOCDB_CXX20_CONCEPT(IsOptional, requires(T x) {
   typename T::value_type;
   T();
   T(std::declval<typename T::value_type>());
   {x.has_value()}->std::same_as<bool>;
   x.value();
   x.emplace(std::declval<typename T::value_type>());
});

template<typename T>
DOCDB_CXX20_CONCEPT(HasReserveFunction, requires(T x) {
    {x.reserve(std::declval<std::size_t>())};
});


template<typename T>
DOCDB_CXX20_CONCEPT(IsVariant,requires {
    typename std::variant_size<T>::type;
});
struct DocumentWrapperConstructor {
    bool operator()(std::string &);
};

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
DOCDB_CXX20_CONCEPT(DocumentWrapper, std::is_constructible_v<T, DocumentWrapperConstructor>);



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


enum class AggrOperation {
    //include to aggregation
    include,
    //exclude from aggregation
    exclude
};


template<typename X>
DOCDB_CXX20_CONCEPT(NonincrementalAggregateFunction, requires (X fn, const typename X::InputType &input) {
    {fn(input)} -> std::convertible_to<typename X::ResultType>;
});
template<typename X>
DOCDB_CXX20_CONCEPT(IncrementalAggregateFunction, requires (X fn, const typename X::InputType &input, AggrOperation op) {
    {fn(input, op)} -> std::convertible_to<typename X::ResultType>;
});

template<typename X>
DOCDB_CXX20_CONCEPT(AggregateFunction, (IncrementalAggregateFunction<X> || NonincrementalAggregateFunction<X>));




}
template<typename T, typename To>
DOCDB_CXX20_CONCEPT(HasCastOperatorTo, requires(T x){
    x.operator To();
});





#endif /* SRC_DOCDB_CONCEPTS_H_ */

