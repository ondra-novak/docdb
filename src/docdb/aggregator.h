#pragma once
#ifndef SRC_DOCDB_AGGREGATOR_H_
#define SRC_DOCDB_AGGREGATOR_H_

#include "concepts.h"
#include "waitable_atomic.h"

#include <thread>

namespace docdb {

using TransactionObserverFunction = std::function<void(Batch &b, const Key& key, bool erase)>;

template<typename T>
DOCDB_CXX20_CONCEPT(AggregatorSource, requires(T x) {
    typename T::ValueType;
    {x.get_db()} -> std::convertible_to<PDatabase>;
    {x.register_transaction_observer(std::declval<TransactionObserverFunction>())};
    {x.rescan_for(std::declval<TransactionObserverFunction>())};
    {x.select(std::declval<Key>()) } -> std::derived_from<RecordsetBase>;
    {x.update() };
});



namespace _details {




     template<typename X>
     DOCDB_CXX20_CONCEPT(IteratorContainsID, requires(X x) {
        {x->id} -> std::convertible_to<DocID>;
     });

     template<typename T> struct GetColumns {
         using TupleType = typename T::TupleType;
     };

     template<typename ... Items> struct GetColumns<std::tuple<Items...> > {
         using TupleType = std::tuple<Items...>;
     };

     template<typename X, typename ... Args>
     struct CombineRevision {
         static constexpr std::size_t revision = CombineRevision <Args...>::revision + 1UL<<X::revision;
     };
     template<typename X>
     struct CombineRevision<X> {
         static constexpr std::size_t revision = 1UL<<X::revision;
     };

     template<typename T, typename X>
     auto wrap_reference(X &&val) {
         if constexpr(std::is_reference_v<T>) {
             return &val;
         } else {
             return T(std::forward<X>(val));
         }
     }

     template<typename T, typename X>
     auto unwrap_refernece(X &&val) {
         if constexpr(std::is_reference_v<T>) {
             return T(*val);
         } else {
             return T(val);
         }
     }

     template<typename AggrFn, typename T, bool b>
     struct AggrResult {
         using type = std::decay_t<T>;
     };

     template<typename AggrFn, typename T>
     struct AggrResult<AggrFn, T, true> {
         using type = typename AggrFn::ResultType;
     };

}


///Aggregate tool
/**
 * @tparam ColumnTuple This template argument should contain std::tuple<> of types matching
 * the format of stored key however it can contain less columns for grouping. For example, if
 * the key has 3 columns, you can specify just 2 column to group by these columns. It is also
 * possible to define own type, which can accept the std::tuple<> and define coparison operator
 * to implement non-standard grouping. In this case, your type must contain `using TupleType`
 * which must contain definition of types in columns declared as tuple
 *
 * @code
 * struct MyAggregatedKey {
 *      using TupleType = std::tuple<std::size_t, double>;
 *      MyAggregatedKey(const TupleType &init);
 *      //...
 * };
 * @endcode
 *
 *
 * To use aggregation over recordset, use Aggregate<Collumns...>::Recordset
 */
template<typename ColumnTuple /*= std::tuple<> */>
struct AggregateBy {

    using TupleType = typename _details::GetColumns<ColumnTuple>::TupleType;


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

        using ResultTypeBase = std::invoke_result_t<AggrFn,decltype(std::declval<RC>().begin()->value)>;
        using ResultType = typename _details::AggrResult<AggrFn, ResultTypeBase, AggregateFunction<AggrFn> >::type;


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
            ,_aggrFn(std::forward<AggrFn>(aggrFn)) {}

        ///Structure is returned as result of aggregated iteration
        struct ValueType {
            ///Key - contains key in type passed as ColumnTuple
            ColumnTuple key;
            ///Value - contains aggregated value
            ResultType value;
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
        RC _rc;
        typename RC::Iterator _iter;
        typename RC::Iterator _end;
        AggrFn _aggrFn;
        std::optional<ValueType> _result;

        bool get_next() {
            if (_iter == _end) return false;
            AggrFn aggrFn(_aggrFn);
            _result.reset();
            ColumnTuple kbase(_iter->key.template get<TupleType>());
            auto cur_val = _details::wrap_reference<ResultTypeBase>(
                    aggrFn(_iter->value)
            );
            ++_iter;
            while (_iter != _end) {
                ColumnTuple k(_iter->key.template get<TupleType>());
                if (k != kbase) break;
                cur_val = _details::wrap_reference<ResultTypeBase>(
                    aggrFn(_iter->value)
                );
                ++_iter;
            }
            _result.emplace(ValueType{
                kbase,
                _details::unwrap_refernece<ResultTypeBase>(cur_val)
            });
            return true;
        }

        const ValueType *get_value_ptr() const {
            return &(*_result);
        }
    };


    template<typename RC, typename AggrFn>
    static auto make_recordset(RC &&rc, AggrFn &&aggrfn) {
        return Recordset<RC, AggrFn>(std::forward<RC>(rc), std::forward<AggrFn>(aggrfn));
    }

    template<DocumentDef _ValueDef>
    using AggregatorView = IndexViewGen<_ValueDef, IndexViewBaseEmpty<_ValueDef> >;

    template<AggregatorSource MapSrc, AggregateFunction AggrFn, DocumentDef _ValueDef = RowDocument, bool auto_update = false>
    class Materialized: public AggregatorView<_ValueDef> {
    public:

        using ResultTypeBase = std::invoke_result_t<AggrFn,decltype(std::declval<MapSrc>().select(std::declval<Key>()).begin()->value)>;
        using ResultType = typename AggrFn::ResultType;


        using DocType = typename MapSrc::ValueType;
        using ValueType = typename _ValueDef::Type;

        using Revision = std::size_t;
        static constexpr Revision revision = AggrFn::revision;

        Materialized(MapSrc &source, std::string_view name)
               :Materialized(source, source.get_db()->open_table(name, Purpose::aggregation)) {}
        Materialized(MapSrc &source, KeyspaceID kid)
               :AggregatorView<_ValueDef>(source.get_db(), kid, Direction::forward, {}, false)
               ,_source(source)
               ,_tcontrol(*this) {

               if (get_revision() != revision) {
                   rebuild();
               }
               this->_source.register_transaction_observer(make_observer());
           }

        Materialized(const Materialized &) = delete;
        Materialized &operator=(const Materialized &) = delete;

        Revision get_revision() const {
            auto k = this->_db->get_private_area_key(this->_kid);
            auto doc = this->_db->template get_as_document<FoundRecord<RowDocument> >(k);
            if (doc.has_value()) {
                auto [cur_rev] = doc->template get<Revision>();
                return cur_rev;
            }
            else return 0;
        }

        void update() {
            _source.update();
            update(false);
        }
        void try_update() {
            _source.try_update();
            update(true);
        }

    protected:



        class TaskControl: public AbstractBatchNotificationListener {
        public:
            TaskControl(Materialized &owner):_owner(owner) {}
            TaskControl(const TaskControl &owner) = delete;
            TaskControl &operator=(const TaskControl &owner) = delete;

            virtual void after_commit(std::size_t) noexcept override {
                _owner.after_commit();
            }
            virtual void before_commit(Batch &) override {
                if (_e) {
                    std::exception_ptr e = std::move(_e);
                    std::rethrow_exception(e);
                }
            }
            virtual void after_rollback(std::size_t ) noexcept override {
                _owner.after_rollback();
            }




        protected:
            Materialized &_owner;
            std::exception_ptr _e;
        };

        MapSrc &_source;
        TaskControl _tcontrol;
        std::shared_mutex _index_lock;
        waitable_atomic<bool>_update_lock;
        unsigned char _bank = 0;
        bool dirty = false;

        void after_commit() {
            dirty = true;
            _index_lock.unlock_shared();
            if constexpr (auto_update) {
                if (!_update_lock.exchange(true)) {
                    std::thread thr([this]{
                        try {
                            while (update_lk());
                        } catch (...) {
                            //todo handle exception
                        }
                        _update_lock.store(false);
                        _update_lock.notify_all();
                    });
                }
            }

        }
        void after_rollback() {
            _index_lock.unlock_shared();
        }

        bool update(bool non_block) {
            if (_update_lock.exchange(true)) {
                if (non_block) return false;
                do {
                    _update_lock.wait(true);
                } while (_update_lock.exchange(true));
            }
            try {
                update_lk();    //we don't need to know, whether dirty is set
                _update_lock.store(false);
                _update_lock.notify_all();
                return true;
            } catch (...) {
                _update_lock.store(false);
                _update_lock.notify_all();
                throw;
            }

        }

        bool update_lk() {
            std::unique_lock ilk(_index_lock);
            if (!dirty) {
                return false;
            }
            dirty = false;
            auto b = _bank;
            _bank = 1-b;
            auto snapshot = this->_db->make_snapshot();
            ilk.unlock();
            run_aggregate(snapshot, b,b+1);
            return true;
        }

        void run_aggregate(PSnapshot snapshot, unsigned char b1, unsigned char b2) {
            auto snapview = _source.get_snapshot(snapshot);
            RecordsetBase rs(this->_db->make_iterator(snapshot, true),{
                    Database::get_private_area_key(this->_kid, b1),
                    Database::get_private_area_key(this->_kid, b2),
                    FirstRecord::included,
                    LastRecord::excluded
            });
            Batch b;
            while (!rs.empty()) {
                auto k = rs.raw_key();
                auto [kid, bank, keydata] = Key::extract<KeyspaceID, unsigned char, Row>(k);
                RawKey key(this->_kid, keydata);
                Key agrkey(Blob(rs.raw_value()));
                auto rc = snapview.select(agrkey);
                auto iter = rc.begin();
                auto iterend = rc.end();
                if (iter != iterend) {
                    AggrFn aggrFn = {};
                    auto cur_val = _details::wrap_reference<ResultTypeBase>(
                            aggrFn(iter->value)
                    );
                    ++iter;
                    while (iter != iterend) {
                        cur_val = _details::wrap_reference<ResultTypeBase>(
                            aggrFn(iter->value)
                        );
                        ++iter;
                    }
                    ValueType v((ResultType((_details::unwrap_refernece<ResultTypeBase>(cur_val)))));
                    auto &buff = b.get_buffer();
                    _ValueDef::to_binary(v, std::back_inserter(buff));
                    b.Put(key, buff);
                } else {
                    b.Delete(key);
                }
                b.Delete(to_slice(k));
                this->_db->commit_batch(b);
                rs.next();
            }
        }

        auto make_observer() {
             return [&](Batch &b, const Key &k, bool ) {
                 _index_lock.lock_shared();

                 TupleType kt = k.get<TupleType>();
                 ColumnTuple ct(kt);
                 Row vrw(kt);
                 Row krw(ct);
                 auto &buff = b.get_buffer();
                 RowDocument::to_binary(krw, std::back_inserter(buff));
                 auto dk = Database::get_private_area_key(this->_kid, _bank,  krw);
                 b.Put(dk, buff);
                 b.add_listener(&_tcontrol);
             };
         }
        void update_revision() {
            Batch b;
            b.Put(this->_db->get_private_area_key(this->_kid), Row(revision));
            this->_db->commit_batch(b);
        }

        void rebuild() {
            this->_db->clear_table(this->_kid, false);
            this->_db->clear_table(this->_kid, true);
            this->_source.rescan_for(make_observer());
            update_revision();
        }

    };

    template<typename MapSrc, typename AggrFn, DocumentDef _ValueDef = RowDocument>
    class MaterializedAutoUpdate: public Materialized<MapSrc,AggrFn, _ValueDef, true> {
    public:
        using Materialized<MapSrc,AggrFn, _ValueDef, true>::Materialized;
    };

};



}




#endif /* SRC_DOCDB_AGGREGATOR_H_ */
