/**
 * @file aggregate_rows.h contains various aggregation functions that can be used above RowDocuments
 *
 * - AggregateRows<> - contains aggregation function for the whole row. You specify
 *                     aggregation functions for each column as template arguments
 * - Count           - count rows
 * - Sum             - sum values
 * - Sum2            - perform sum of square roots
 * - Avg             - calculate average
 * - Min             - get minimum item
 * - Max             - get maximum item
 * - First           - get first item
 * - Last            - get last item
 * - Composite<>     - run multiple aggregation above single column
 *
 *
 */

#pragma once
#ifndef SRC_DOCDB_AGGREGATE_ROWS_H_
#define SRC_DOCDB_AGGREGATE_ROWS_H_

#include "serialize.h"

#include <cmath>
namespace docdb {

template<typename X>
DOCDB_CXX20_CONCEPT(RowAggregateFunction, requires (X fn, typename X::AccumType &acc, const typename X::InputType &input) {
    {fn(acc,input)};
});

template<typename X, typename ... Args>
struct AggregatedRevision {
    static constexpr std::size_t revision = AggregatedRevision <Args...>::revision + 0x9e3779b9 + (X::revision<<6) + (X::revision>>2);
};
template<typename X>
struct AggregatedRevision<X> {
    static constexpr std::size_t revision = X::revision;
};

template<typename ... Args>
inline constexpr std::size_t aggregated_revision = AggregatedRevision<Args...>::revision;


template<RowAggregateFunction ... AgrTypes>
struct AggregateRows {
    using Accumulator = std::tuple<typename AgrTypes::AccumType ...>;
    using ParsedRow = std::tuple<typename AgrTypes::InputType ...>;
    static constexpr std::size_t revision = AggregatedRevision<AgrTypes...>::revision;
    std::tuple<AgrTypes ...> _state = {};


    template<std::size_t ... Is>
    void accumulate(const ParsedRow &in, std::index_sequence<Is...>) {
        (std::get<Is>(_state).add(std::get<Is>(in)),...);
    }
    template<std::size_t ... Is>
    void get_result(Accumulator &acc, std::index_sequence<Is...>) const {
        ((std::get<Is>(acc) = std::get<Is>(_state).get_result()),...);
    }

    template<std::size_t ... Is>
    void do_accumulate(Accumulator &acc, const ParsedRow &in, std::index_sequence<Is...>) {
        (std::get<Is>(_state).operator()(std::get<Is>(acc), std::get<Is>(in)),...);
    }


    void operator()(Accumulator &accum, const Row &row) {
        ParsedRow data = row.get<ParsedRow>();
        do_accumulate(accum, data, std::index_sequence_for<AgrTypes...>{});
    }



};


template<typename Type = std::nullptr_t>
struct Count {
    static constexpr std::size_t revision = 1;
    using AccumType = std::size_t;
    using InputType = Type;

    void operator()(std::size_t &count, const Type &x) {
        ++count;
    }
};

template<typename Type>
struct Sum {
    static constexpr std::size_t revision = 2;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &sum, const Type &x) {
        sum = sum + x;
    }
};

template<typename Type>
struct Avg {
    static constexpr std::size_t revision = 3;
    using AccumType = Type;
    using InputType = Type;
    Sum<Type> _sum = {};
    Count<Type> _count = {};
    typename Sum<Type>::AccumType _sum_state = {};
    typename Count<Type>::AccumType _count_state = {};

    void operator()(Type &avg, const Type &x) {
        _sum(_sum_state, x);
        _count(_count_state, x);
        avg = static_cast<Type>(_sum/_count);
    }

};

template<typename Type>
struct Sum2 {
    static constexpr std::size_t revision = 4;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &sum, const Type &x) {
        sum = sum + x*x;
    }
};

template<typename Type>
struct Max {
    static constexpr std::size_t revision = 5;
    using AccumType = Type;
    using InputType = Type;
    bool _is_first = true;

    void operator()(Type &a, const Type &x) {
        a = (_is_first | a < x) ? x: a;
        _is_first = false;
    }

};

template<typename Type>
struct Min {
    static constexpr std::size_t revision = 6;
    using AccumType = Type;
    using InputType = Type;
    bool _is_first = true;

    void operator()(Type &a, const Type &x) {
        a = (_is_first | a > x) ? x: a;
        _is_first = false;
    }
};

template<typename Type>
struct First {
    static constexpr std::size_t revision = 7;
    using AccumType = Type;
    using InputType = Type;
    bool _is_first = true;

    void operator()(Type &a, const Type &x) {
        a = _is_first ? x: a;
        _is_first = false;
    }
};

template<typename Type>
struct Last {
    static constexpr std::size_t revision = 8;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &a, const Type &x) {
        a = x;
    }
};



inline constexpr std::string_view group_concat_default_delimiter{","};
///Creates list of strings separated by a separator
/**
 * @tparam Type type of string, recommended type is std::string_view. You can also
 * use docdb::Blob if you need to group strings with zeroes. However, result is also
 * stored as Blob. You can also group-concat std::wstring_view
 * @tparam delimiter delimiter - it must be specified as refernece to constexpr
 * object of appropriate type
 */
template<typename Type = std::string_view, const std::basic_string_view<typename Type::value_type> *delimiter = &group_concat_default_delimiter>
struct GroupConcat {
    static constexpr std::size_t revision = 9;
    using AccumType = Type;
    using InputType = Type;
    using CharType = typename Type::value_type;
    using StrView = std::basic_string_view<CharType>;
    std::basic_string<CharType> state;

    void operator()(Type &a, const Type &x) {
        if (!state.empty()) {
            state.append(*delimiter);
        }
        state.append(StrView(x));
        a = Type(state);
    }


};

template<typename T, typename ... AgrFns>
struct Composite {
    static constexpr std::size_t revision = AggregatedRevision<AgrFns...>::revision;

    using States = std::tuple<AgrFns...>;
    using AccumType = std::tuple<typename AgrFns::AccumType ...>;
    using InputTypes = std::tuple<typename AgrFns::InputType ...>;
    States _states = {};
    using InputType = T;

    template<std::size_t ... Is>
    void do_accum(AccumType &acc, const InputType &val, std::index_sequence<Is...>) {
        (std::get<Is>(_states).operator()(std::get<Is>(acc), static_cast<std::tuple_element_t<Is,InputTypes> >(val)),...);
    }

    void operator()(AccumType &accum, const InputType &x) {
        do_accum(accum, x, std::index_sequence_for<AgrFns...>{});
    }


};



}


#endif /* SRC_DOCDB_AGGREGATE_ROWS_H_ */
