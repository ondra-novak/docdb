#pragma once
#ifndef SRC_DOCDB_RECORDSET_AGGREGATOR_H_
#define SRC_DOCDB_RECORDSET_AGGREGATOR_H_

namespace docdb {

namespace _details {

//aggregate function -> void operator()(T &accum, Value val)

///If T is lambda function, retrieves type of argument
     template<typename T> struct DeduceArg;

     template<typename _Res, typename _Tp, bool _Nx, typename A, typename ... Args>
     struct DeduceArg< _Res (_Tp::*) (A &, Args ...) noexcept(_Nx) > {using type = A;};
     template<typename _Res, typename _Tp, bool _Nx, typename A, typename ... Args>
     struct DeduceArg< _Res (_Tp::*) (A &, Args ...) const noexcept(_Nx) > {using type = A;};


     template<typename X>
     DOCDB_CXX20_CONCEPT(IteratorContainsID, requires(X x) {
        {x->id} -> std::convertible_to<DocID>;
     });
}

///Aggregate tool
/**
 * @tparam KeyColumns types stored in key, up to unique columns. Unspecified extra columns are
 * aggregated. For example if you have key <int,std::string,int>, you can specify <int,std::string>
 * as KeyColumns, so any values matching these two columns are aggregated into single value
 *
 * To use aggregation over recordset, use Aggregate<Collumns...>::Recordset
 */
template<typename ... KeyColumns>
struct Aggregate {

    ///type for key - it is alias for std::tuple
    using KeyObject = std::tuple<KeyColumns...>;


    ///Aggregate recordset
    /**
     * @tparam RC Recordset to aggregate (for example IndexView::Recordset)
     * @tparam AggrFn aggregation function. Function must accept two or three arguments.
     * The first argument must be reference of accumulator, other arguments are index's value
     * and document id (which is optional).
     *
     * @code
     * void fn3(accumulator &a, const ValueType &val, DocID id);
     * void fn2(accumulator &a, const ValueType &val);
     * @endcode
     *
     * The accumulator type is then deduced as resulting aggregated value. Instance
     *  of the accumulator must be constructed using default constructor
     *
     * For convenience you can use automatic deduction, so if you specify object
     * without template arguments, it can deduce recordset type and aggregate function
     */
    template<typename RC, typename AggrFn>
    class Recordset {
    public:

        using AggrValueType = typename _details::DeduceArg<decltype(&AggrFn::operator())>::type;

        ///Inicialize aggregator
        /**
         * @param rc recordset - must be moved in, or you can use view to call appropriate
         * select() function to receive recordset
         * @param aggrFn instance of aggregate function
         */
        Recordset(RC rc, AggrFn aggrFn)
            :_rc(std::move(rc))
            ,_iter(_rc.begin())
            ,_end(_rc.end())
            ,_aggrFn(std::move(aggrFn)) {}

        ///Structure is returned as result of aggregated iteration
        struct ValueType {
            ValueType(const Key &k):key(k.get<KeyColumns...>()) {}

            ///Key - contains tuple of keys specified by KeyColumns...
            KeyObject key;
            ///Value - contains aggregated value
            AggrValueType value = {};
        };

        ///Iterator - allows to read aggregated results (input iterator)
        /** best usage of this iterator is to use ranged-for
         *
         */
        class Iterator {
        public:
            using iterator_category = std::input_iterator_tag;
            using value_type = ValueType; // crap
            using difference_type = ptrdiff_t;
            using pointer = const ValueType *;
            using reference = const ValueType &;


            Iterator() = default;
            Iterator(Recordset *owner, bool end):_owner(owner), _end(end) {}

            Iterator &operator++() {
                _end = !_owner->get_next();
                return *this;
            }
            const ValueType &operator *() const {
                return *_owner->get_value_ptr();
            }
            const ValueType *operator ->() const {
                return _owner->get_value_ptr();
            }
            bool operator == (const Iterator &iter) const {
                return _owner == iter._owner && _end == iter._end;
            }

        protected:
            Recordset *_owner = nullptr;
            bool _end = true;
        };

        ///get begin iterator
        /** as this iterator is single pass input iterator, it returns instance which
         * is marked as "not end". However reading through iterator changes state of underlying
         * recordset
         *
         * @return iterator
         *
         * @note function automatically fetches the first item.
         */
        Iterator begin()  {return Iterator(this,!get_next());}
        ///get end iterator
        /**
         * @return instance of iterator which marks end of iteration
         */
        Iterator end() {return Iterator(this, true);}


    protected:

        using RCValue = decltype(std::declval<typename RC::Iterator>()->value);
        static constexpr bool three_args_fn = _details::IteratorContainsID<typename RC::Iterator> && std::invocable<AggrFn, AggrValueType &,RCValue, DocID>;
        RC _rc;
        typename RC::Iterator _iter;
        typename RC::Iterator _end;
        AggrFn _aggrFn;
        std::optional<ValueType> _result;

        bool get_next() {
            if (_iter == _end) return false;
            _result.reset();
            _result.emplace(_iter->key);
            if constexpr (three_args_fn) {
                _aggrFn(_result->value, _iter->value, _iter->id);
            } else {
                _aggrFn(_result->value, _iter->value);
            }
            ++_iter;
            while (_iter != _end) {
                KeyObject k(_iter->key.template get<KeyColumns...>());
                if (k != _result->key) break;
                if constexpr (three_args_fn) {
                    _aggrFn(_result->value, _iter->value, _iter->id);
                } else {
                    _aggrFn(_result->value, _iter->value);
                }
                ++_iter;
            }
            return true;
        }

        const ValueType *get_value_ptr() const {
            return &(*_result);
        }
    };

    template<typename RC, typename AggrFn>
    Recordset(RC rc, AggrFn aggrFn)->Recordset<RC, AggrFn>;

};




}




#endif /* SRC_DOCDB_RECORDSET_AGGREGATOR_H_ */
