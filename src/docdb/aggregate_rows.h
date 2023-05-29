#pragma once
#ifndef SRC_DOCDB_AGGREGATE_ROWS_H_
#define SRC_DOCDB_AGGREGATE_ROWS_H_

#include "serialize.h"

#include <cmath>
namespace docdb {

template<typename ... AgrTypes>
struct AggregateState{

    using States = std::tuple<AgrTypes...>;

    template<int index = 0>
    void accumulate(const Row &row) {
        if constexpr(index >= std::tuple_size_v<States>) {
            return;
        } else {
            if (row.empty()) return;
            auto &st = std::get<index>(_state);
            using ValueType = typename std::tuple_element_t<index, States>::ValueType;
            auto [x, next_row] = row.get<ValueType, Row >();
            st.add(x);
            accumulate<index+1>(next_row);
        }
    }

    template<int index = 0>
    void build_result(Row &row) {
        if constexpr(index >= std::tuple_size_v<States>) {
            return;
        } else {
            const auto &st = std::get<index>(_state);
            row.append(st.get_result());
            build_result<index+1>(row);
        }
    }

    States _state;
};

template<typename X>
DOCDB_CXX20_CONCEPT(RowAggregateFunction, requires (X fn) {
    typename X::ValueType;
    {fn.add(std::declval<typename X::ValueType>())} ->std::same_as<void>;
    {fn.get_result()}->std::convertible_to<Row>;
});

///Aggregate rows into single Row object
/**
 * The function expects that that index returns values in type Row;
 *
 * @tparam AgrTypes list of aggregation function to execute on each column. For example
 * Sum<type>, Count<type>, etc. Each type means one columnt (expect Count<type>, which
 * doesn't consume any column). If you need multiple aggregations for a single column, use
 * Composite aggregation.
 *
 * @param index source index where search values to aggregate
 * @param key contains prefix key to aggregate
 * @return Row object containing aggregated result
 */
template<RowAggregateFunction ... ArgTypes>
constexpr auto aggregate_rows =  [](const auto &index, Key key) -> AggregatedResult<Row> {
        auto rs = index.select(key);
        static_assert(std::is_convertible_v<decltype(*rs.begin()), const Row &>,
                "The function 'aggregate_rows' supports values returned as the type Row");
        if (rs.begin() == rs.end()) return {};
        AggregateState<ArgTypes ...> _state;

        for (const auto &row: rs) {
            _state.accumulate(row.value);
        }
        Row out;
        _state.build_result(out);
        return out;
    };


template<typename Type = double>
struct Count {
    std::size_t _count = 0;
    using ValueType = std::nullptr_t;

    template<typename X>
    void add(X &&) {
        ++_count;
    }
    Type get_result() const {
        return Type(_count);
    }
};

template<typename Type>
struct Sum {
    Type _s = 0;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        _s = _s + x;
    }
    auto get_result() const {
        return _s;
    }
};

template<typename Type>
struct Avg {
    Type _s = 0;
    std::size_t _count = 0;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        _s = _s + x;
        ++_count;
    }
    auto get_result() const {
        return _s/_count;
    }
};

template<typename Type>
struct Sum2 {
    Type _s = 0;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        _s = _s + x*x;
    }
    auto get_result() const {
        return _s;
    }
};

template<typename Type>
struct Max {
    Type _s = {};
    bool _first = true;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        if (_first) {
            _first = false;
            _s = x;
        } else {
            _s = std::max<ValueType>(_s,x);
        }
    }
    void build_result(Row &row) const {
        row.append(_s);
    }
    auto get_result() const {
        return _s;
    }
};

template<typename Type>
struct Min {
    Type _s = {};
    bool _first = true;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        if (_first) {
            _first = false;
            _s = x;
        } else {
            _s = std::min<ValueType>(_s,x);
        }
    }
    auto get_result() const {
        return _s;
    }
};

template<typename Type>
struct First {
    Type _s = {};
    bool _first = true;
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        if (_first) {
            _first = false;
            _s = x;
        }
    }
    auto get_result() const {
        return _s;
    }
};

template<typename Type>
struct Last {
    Type _s = {};
    using ValueType = Type;
    template<typename X>
    void add(const X &x) {
        _s = x;
    }
    auto get_result() const {
        return _s;
    }
};

template<typename T, typename ... ArgFns>
struct Composite {

    using States = std::tuple<ArgFns...>;
    using Results = std::tuple<decltype(std::declval<ArgFns>().get_result())...>;
    States _states = {};
    using ValueType = T;

    template<int index = 0>
    void add(const ValueType &x) {
        if constexpr(index >= std::tuple_size_v<States>) {
            return;
        } else {
            auto &st = std::get<index>(_states);
            st.add(x);
            add<index+1>(x);
        }
    }
    auto get_result() const {
        Results res;
        auto assign_values = [&]<std::size_t... Is>(std::index_sequence<Is...>) {
                   ((std::get<Is>(res) = std::get<Is>(_states).get_result()), ...);
        };
        assign_values(std::make_index_sequence<std::tuple_size_v<Results> >{});
        return res;
    }


};



}


#endif /* SRC_DOCDB_AGGREGATE_ROWS_H_ */
