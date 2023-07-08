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


namespace _details {

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

}
///Construct aggregate function for RowDocument
/**
 * @tparam AgrTypes list of aggregate functions per column in the row. If you
 * need multiple aggregate function for single column, use Composite aggregation
 * function. If you need to skip column, use Skip aggregation function
 *
 */
template<RowAggregateFunction ... AgrTypes>
struct AggregateRows {
    using Accumulator = std::tuple<typename AgrTypes::AccumType ...>;
    using ParsedRow = std::tuple<typename AgrTypes::InputType ...>;
    static constexpr std::size_t revision = _details::AggregatedRevision<AgrTypes...>::revision;
    std::tuple<AgrTypes ...> _state = {};


    template<std::size_t ... Is>
    void do_accumulate(Accumulator &acc, const ParsedRow &in, std::index_sequence<Is...>) {
        (std::get<Is>(_state).operator()(std::get<Is>(acc), std::get<Is>(in)),...);
    }


    void operator()(Accumulator &accum, const Row &row) {
        ParsedRow data = row.get<ParsedRow>();
        do_accumulate(accum, data, std::index_sequence_for<AgrTypes...>{});
    }



};

///Count rows in aggregated results
/**
 * @tparam Type Specifies type of column, you can use std::nullptr_t (default) to
 * avoid claiming any column. However if Count is part of Composite, you need
 * to specify compatibile type of the column
 *
 * - Count<> - equivalent of COUNT(*)
 * - Count<int> - treat column as integer and count rows while this column is skipped
 */
template<typename Type = std::nullptr_t>
struct Count {
    static constexpr std::size_t revision = 1;
    using AccumType = std::size_t;
    using InputType = Type;

    void operator()(std::size_t &count, const Type &x) {
        ++count;
    }
};

///Sum values
/**
 * @tparam Type type of column to sum. Result is of the same type
 */
template<typename Type>
struct Sum {
    static constexpr std::size_t revision = 2;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &sum, const Type &x) {
        sum = sum + x;
    }
};


///Calculate average
/**
 * @tparam Type type of column to calculate average
 * @tparam SumType type of sum value. You need to set different type if you
 * need decimal numbers for average if integers
 */
template<typename Type, typename SumType = Type>
struct Avg {
    static constexpr std::size_t revision = 3;
    using AccumType = Type;
    using InputType = Type;
    Sum<SumType> _sum = {};
    Count<> _count = {};
    typename Sum<SumType>::AccumType _sum_state = {};
    typename Count<>::AccumType _count_state = {};

    void operator()(Type &avg, const Type &x) {
        _sum(_sum_state, x);
        _count(_count_state, x);
        avg = static_cast<Type>(_sum/_count);
    }

};

///Calculate sum of squares. This is need to calculate a standard deviation
/**
 * @tparam Type column type, Result is stored as the same type
 */
template<typename Type>
struct Sum2 {
    static constexpr std::size_t revision = 4;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &sum, const Type &x) {
        sum = sum + x*x;
    }
};

///Find maximum value
/**
 * @tparam Type column type, Result is stored as the same type
 */
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

///Find minimum value
/**
 * @tparam Type column type, Result is stored as the same type
 */
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

///Store first value in the aggregation
/**
 * @tparam Type column type, Result is stored as the same type
 */
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

///Store last value in the aggregation
/**
 * @tparam Type column type. Result is stored as the same type
 */
template<typename Type>
struct Last {
    static constexpr std::size_t revision = 8;
    using AccumType = Type;
    using InputType = Type;

    void operator()(Type &a, const Type &x) {
        a = x;
    }
};

///Skip the column
/**
 * @tparam Type of column to skip. This column is not aggregated and
 * should not appear in final result
 */
template<typename Type>
struct Skip {
    static constexpr std::size_t revision = 9;
    using AccumType = std::nullptr_t;
    using InputType = Type;

    void operator()(std::nullptr_t &, const Type &x) {
        //empty
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

///Scale input value before it is aggregated
/**
 *
 * This transforms input value by multiplying it by a factor. This can
 *zbe useful when you need to work with percents, or if you need to calculate
 * average of integers
 *
 * @tparam val factor of scale. It can be integral constant or pointer to
 * constexpr value (for pass double type value)
 * @tparam Agr aggregation function (or any other transformation)
 */
template<auto val, typename Agr>
struct Scale {
    static constexpr std::size_t revision = 10;
    using AccumType = typename Agr::AccumType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    void operator()(AccumType &a, const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            _state(a, *val * x);
        } else {
            _state(a, val * x);
        }
    }
};

///Offset the input value by a constant
/**
 * Calculates (x + val). where x is input value
 *
 * @tparam val offset as integral constant or pointer to constexpr value
 * @tparam Agr aggregation function
 */
template<auto val, typename Agr>
struct Offset {
    static constexpr std::size_t revision = 11;
    using AccumType = typename Agr::AccumType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    void operator()(AccumType &a, const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            _state(a, x + *val);
        } else {
            _state(a, x + val);
        }
    }
};

///Calculate invert value before aggregation
/**
 * Calculates (val - x), where x is input value
 *
 * @tparam val pointer to constant or integral constant
 * @tparam Agr aggregation function
 */
template<auto val, typename Agr>
struct Invert {
    static constexpr std::size_t revision = 11;
    using AccumType = typename Agr::AccumType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    void operator()(AccumType &a, const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            _state(a, *val - x);
        } else {
            _state(a, *val - x);
        }
    }
};


///Transform
template<typename Agr, typename Agr::InputType (*fn)(const typename Agr::InputType &), std::size_t rev=13>
struct Transform {
    static constexpr std::size_t revision = rev;
    using AccumType = typename Agr::AccumType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    void operator()(AccumType &a, const InputType &x) {
        _state(a, fn(x));
    }
};

template<typename From, typename Agr>
struct Convert {
    static constexpr std::size_t revision = 12;
    using AccumType = typename Agr::AccumType;
    using InputType = From;
    using TargetType = typename Agr::InputType;
    Agr _state = {};

    void operator()(AccumType &a, const InputType &x) {
        _state(a, static_cast<TargetType>(x));
    }
};


///Performs multiple aggregations above single column
/**
 * @tparam T type of column
 * @tparam AgrFns Aggregation functions called on value of the column.
 *  Note that all aggregated function must accept the same type or accept
 *  compatible type.
 *
 *  @code
 *  Composite<int, Count<int>, Sum<int>, Min<int>, Max<int>, Avg<int, double> >
 *  @endcode
 *
 *  Above example generates 5 columns count,sum, min, max, avg, from single column
 *  value
 */
template<typename T, typename ... AgrFns>
struct Composite {
    static constexpr std::size_t revision = _details::AggregatedRevision<AgrFns...>::revision;

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
