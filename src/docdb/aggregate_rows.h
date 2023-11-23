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
template<AggregateFunction ... AgrTypes>
struct AggregateRows {
    using InputType = Row;
    using ResultType = std::tuple<typename AgrTypes::ResultType ...>;
    using ParsedRow = std::tuple<typename AgrTypes::InputType ...>;
    static constexpr std::size_t revision = _details::AggregatedRevision<AgrTypes...>::revision;
    std::tuple<AgrTypes ...> _state = {};

    template<std::size_t ... Is>
    auto do_accumulate(const ParsedRow &in, std::index_sequence<Is...>) {
        return ResultType(std::get<Is>(_state)(std::get<Is>(in)) ...);
    }

    ResultType operator()(const Row &row) {
        return do_accumulate(row.get<ParsedRow>(), std::index_sequence_for<AgrTypes...>{});
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
    using ResultType = std::size_t;
    using InputType = Type;
    std::size_t _state = 0;

    std::size_t operator()(const Type &) {
        return ++_state;
    }
};

///Sum values
/**
 * @tparam Type type of column to sum. Result is of the same type
 */
template<typename Type>
struct Sum {
    static constexpr std::size_t revision = 2;
    using ResultType = Type;
    using InputType = Type;
    Type _state = {};

    const Type &operator()(const Type &x) {
        return _state += x;
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
    using ResultType = Type;
    using InputType = Type;
    Sum<SumType>  _sum =  {};
    Count<> _count = {};

    Type operator()(Type &avg, const Type &x) {
        _sum(static_cast<SumType>(x));
        _count(std::nullptr_t());
        return static_cast<Type>(_sum/_count);
    }

};

///Calculate sum of squares. This is need to calculate a standard deviation
/**
 * @tparam Type column type, Result is stored as the same type
 */
template<typename Type>
struct Sum2 {
    static constexpr std::size_t revision = 4;
    using ResulType = Type;
    using InputType = Type;
    Type _state = {};

    const Type &operator()(const Type &x) {
        return _state += x*x;
    }
};

///Find maximum value
/**
 * @tparam Type column type, Result is stored as the same type
 */
template<typename Type>
struct Max {
    static constexpr std::size_t revision = 5;
    using ResultType = Type;
    using InputType = Type;
    Type _state = {};
    bool _has_value = false;


    const Type &operator()( const Type &x) {
        _state = (_has_value & _state > x) ? _state : x;
        _has_value = true;
        return _state;
    }

};

///Find minimum value
/**
 * @tparam Type column type, Result is stored as the same type
 */
template<typename Type>
struct Min {
    static constexpr std::size_t revision = 6;
    using ResultType = Type;
    using InputType = Type;
    Type _state = {};
    bool _has_value = false;


    const Type &operator()( const Type &x) {
        _state = (_has_value & _state < x) ? _state : x;
        _has_value = true;
        return _state;
    }
};

///Store first value in the aggregation
/**
 * @tparam Type column type, Result is stored as the same type
 */
template<typename Type>
struct First {
    static constexpr std::size_t revision = 7;
    using ResultType = Type;
    using InputType = Type;
    Type _state = {};
    bool _has_value = false;


    const Type& operator()(Type &a, const Type &x) {
        _state = _has_value  ? _state : x;
        _has_value = true;
        return _state;

    }
};

///Store last value in the aggregation
/**
 * @tparam Type column type. Result is stored as the same type
 */
template<typename Type>
struct Last {
    static constexpr std::size_t revision = 8;
    using ResultType = Type;
    using InputType = Type;
    Type _state = {};

    const Type &operator()(Type &a, const Type &x) {
        _state = x;
        return _state;
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
    using ResultType = std::nullptr_t;
    using InputType = Type;

    ResultType operator()(const Type &) {
        //empty
        return nullptr;
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
    using ResultType = Type;
    using InputType = Type;
    using CharType = typename Type::value_type;
    using StrView = std::basic_string_view<CharType>;
    std::basic_string<CharType> _state;

    ResultType operator()(const Type &x) {
        if (!_state.empty()) {
            _state.append(*delimiter);
        }
        _state.append(StrView(x));
        return Type(_state);
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
template<auto val, AggregateFunction Agr>
struct Scale {
    static constexpr std::size_t revision = 10;
    using ResultType = typename Agr::ResultType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    decltype(auto) operator()(const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            return _state(*val * x);
        } else {
            return _state(val * x);
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
template<auto val, AggregateFunction Agr>
struct Offset {
    static constexpr std::size_t revision = 11;
    using ResultType = typename Agr::ResultType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    decltype(auto) operator()(const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            return _state(x + *val);
        } else {
            return _state(x + val);
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
template<auto val, AggregateFunction Agr>
struct Invert {
    static constexpr std::size_t revision = 11;
    using ResultType = typename Agr::ResultType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    decltype(auto) operator()(const InputType &x) {
        if constexpr(std::is_pointer_v<decltype(val)>) {
            return _state(*val - x);
        } else {
            return _state(*val - x);
        }
    }
};


///Transform
template<AggregateFunction Agr, typename Agr::InputType (*fn)(const typename Agr::InputType &), std::size_t rev=13>
struct Transform {
    static constexpr std::size_t revision = rev;
    using ResultType = typename Agr::AccumType;
    using InputType = typename Agr::InputType;
    Agr _state = {};

    decltype(auto) operator()( const InputType &x) {
        return _state(fn(x));
    }
};

template<typename From, AggregateFunction Agr>
struct Convert {
    static constexpr std::size_t revision = 12;
    using ResultType = typename Agr::ResultType;
    using InputType = From;
    using TargetType = typename Agr::InputType;
    Agr _state = {};

    decltype(auto) operator()(const InputType &x) {
        return _state(static_cast<TargetType>(x));
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
template<typename T, AggregateFunction ... AgrFns>
struct Composite {
    static constexpr std::size_t revision = _details::AggregatedRevision<AgrFns...>::revision;

    using States = std::tuple<AgrFns...>;
    using ResultType = std::tuple<typename AgrFns::ResultType ...>;
    using InputTypes = std::tuple<typename AgrFns::InputType ...>;
    States _states = {};
    using InputType = T;

    template<std::size_t ... Is>
    ResultType do_accum(const InputType &val, std::index_sequence<Is...>) {
        return ResultType(std::get<Is>(_states).operator()(static_cast<std::tuple_element_t<Is,InputTypes> >(val))...);
    }

    ResultType operator()(const InputType &x) {
        return do_accum(x, std::index_sequence_for<AgrFns...>{});
    }


};



}


#endif /* SRC_DOCDB_AGGREGATE_ROWS_H_ */
