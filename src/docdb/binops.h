/*
 * binops.h
 *
 *  Created on: 12. 6. 2023
 *      Author: ondra
 */

#ifndef SRC_DOCDB_BINOPS_H_
#define SRC_DOCDB_BINOPS_H_

#include "index_view.h"

#include <stack>
#include <unordered_map>
#include <vector>

namespace docdb {

template<typename T>
DOCDB_CXX20_CONCEPT(RecordsetType, std::is_same_v<decltype(std::declval<T>().begin()->id), DocID>);
template<typename T, typename ValueT, typename RC>
DOCDB_CXX20_CONCEPT(RecordsetStackMapFn, std::is_constructible_v<ValueT, std::invoke_result_t<T, decltype(*std::declval<RC>().begin())> >);

///RecordsetStack servers as binary calculator above sets
/**
 * Each set is result of Recordset iteration. You can combine results using
 * operations AND, OR, XOR, and NOT_AND. This can be useful when you need to
 * perform AND operation between results of an index search.
 *
 * Each item in the set carries two values. The first is DocID, which refers
 * to found document. The second is user defined value. This value can be
 * initialized during push() operation when results of recordset being pushed
 * to the stact. You can specify your Map function to map recordset's row to
 * a value
 *
 * The operations AND and OR requires a function which defines how two
 * values assigned to the same document are merged. Other operations such
 * XOR and NOT_AND do not need such function as no items are merged
 *
 * @tparam ValueT type of user value associated with the document in the result
 */
template<typename DocID, typename ValueT = std::nullptr_t>
class RecordsetStackT {
public:

    ///Definition of item in the set
    /** The item contains DocID and custom value> */
    struct Item {
        DocID id;
        ValueT value;
    };

    class Set : public std::vector<Item> {
    public:
        using std::vector<Item>::vector;

        ///Retrieve inverted meaning of the set
        /** Inverted means, that any item in this set is considered
         * as not included. If this set is empty, then it has meaning all items
         *
         * Note that you should avoid to left such set as result, this
         * can lead to full-row scan of the database. Standard function list
         * cannot handle such a set and returns no items
         *
         * @return
         */
        static Set all_items()  {return Set(nullptr);}
        static Set invert(Set &&x) {return Set(nullptr, std::move(x));}

        bool is_inverted() const {return _inverted;}

        void clear() {
            std::vector<Item>::clear();
            _inverted = false;
        }


    protected:
        bool _inverted = false;
        explicit Set(std::nullptr_t):_inverted(true) {}
        Set(std::nullptr_t, Set &&other):std::vector<Item>(other),_inverted(!other._inverted) {}
    };

    static bool cmpfn(const Item &a, const Item &b) {
        return a.id < b.id;
    }
    static ValueT &&move_first(ValueT &, ValueT &y) {
        return std::move(y);
    }

    struct EmptyMap {
        template<typename ... Args>
        ValueT operator()(Args && ...) const {return ValueT();}
    };

    static constexpr EmptyMap empty_map_fn = {};

    using Ret = RecordsetStackT &&;

    Ret push(Set &&set) {
        _stack.push(std::move(set));
        return std::move(*this);
    }
    Ret push(const Set &set) {
        _stack.push(set);
        return std::move(*this);
    }
    Ret push_unsorted(Set &&set) {
        std::sort(set.begin(), set.end(), cmpfn);
        return push(std::move(set));
    }
    Ret push_unsorted(const Set &set) {
        return push_unsorted(Set(set));
    }
    template<RecordsetType Recordset, RecordsetStackMapFn<ValueT, Recordset> FN = EmptyMap>
    Ret push(Recordset &&rc, FN fn = empty_map_fn) {
        Set out = empty_set();
        for (const auto &rw: rc) {
            out.emplace_back(Item{rw.id, fn(rw)});
        }
        return push_unsorted(std::move(out));
    }
    ///pop last item from the stack
    /** if the last item is null, returns empty set */
    Set pop() {
        if (_stack.empty()) return {};
        auto r = std::move(_stack.top());
        _stack.pop();
        return r;
    }
    Ret pop(Set &out) {
        if (_stack.empty()) {
            out.clear();
        } else {
            std::swap(_stack.top(), out);
            _stack.pop();
        }
        return std::move(*this);
    }
    ///Perform operation NOT with set on top os stack
    /**
     * As we don't know whole set, the NOT operation is implemented as
     * inverting special flag on the set. So if the set contained included
     * items, result of operation is set with excluded items
     *
     * We still can achieve operation A AND NOT B, when B contains excluded
     * items, so we can calculate difference
     *
     * @return
     */
    Ret NOT() {
        Set b = pop();
        push(Set::invert(std::move(b)));
        return std::move(*this);
    }
    ///Perform operation AND
    /**
     * Directly supported operations are A AND B, A AND NOT B and NOT A AND B.
     * Operation NOT A AND NOT B is rewritten using De Morgan rule to NOT(A OR B),
     * so result is set of excluded itens
     *
     * @param fn function which merges values.
     * @return
     */
    template<typename MergeFn = decltype(move_first)>
    Ret AND(MergeFn &&fn = move_first) {
        Set b = pop();
        Set a = pop();
        if (b.is_inverted()) {
            if (a.is_inverted()) {
                do_or(a,b,fn);
                return NOT();
            } else {
                return do_diff(a, b);
            }
        }
        if (a.is_inverted()) {
            return do_diff(b, a);
        }
        return do_and(a, b, fn);
    }

    ///Perform operation OR
    /** Directly supported operation is A OR B. Other operations are rewritten
     * using De Morga rule.
     * - NOT A OR B -> NOT(A AND NOT B)
     * - A OR NOT B -> NOT(B AND NOT A)
     * - NOT A OR NOT B -> NOT (A AND B)
     *
     * @param fn function which merges values.
     * @return
     */
    template<typename MergeFn = decltype(move_first)>
    Ret OR(MergeFn &&fn = move_first) {
        Set b = pop();
        Set a = pop();
        if (b.is_inverted()) {
            if (a.is_inverted()) {
                do_and(a,b,fn);
                return NOT();
            } else {
                do_diff(b,a);
                return NOT();
            }
        }
        if (a.is_inverted()) {
            do_diff(a, b);
            return NOT();
        }
        return do_or(a, b,fn);
    }

    ///Performs operation XOR
    /**
     * Directly supported operation is A XOR B.
     * - NOT A XOR NOT B -> NOT(A XOR B)
     * - NOT A XOR B -> NOT((A OR B) AND NOT(A AND B))
     *
     * @tparam MergeFn
     * @param fn
     * @return
     */
    template<typename MergeFn = decltype(move_first)>
    Ret XOR(MergeFn &&fn = move_first) {
        Set b = pop();
        Set a = pop();
        if (b.is_inverted()){
            if (a.is_inverted()) {
                do_xor(a, b);
                return NOT();
            } else {
                return do_xor_not(a, b, fn);
            }
        }
        if (a.is_inverted()) {
            return do_xor_not(a, b, fn);
        }
        return do_xor(a, b);
    }

    bool empty() const {return _stack.empty();}
    bool is_top_empty() const {
        return !_stack.empty() &&  _stack.top().empty() && !_stack.top().is_inverted();
    }

    template<typename StorageView, typename Fn>
    bool list(const StorageView &view, Fn &&fn) {
        if (_stack.empty()) return false;
        return list(_stack.top(), view, fn);
    }

    template<typename StorageView, typename Fn>
    bool list(const Set &set, const StorageView &view, Fn &&fn) {
        if (!set.is_inverted()) {
            for (const Item &itm: set) {
                fn(itm.id, itm.value, view.find(itm.id));
            }
            return true;
        }
        return false;
    }

    Set empty_set() {
        Set out;
        std::swap(_last_a, out);
        std::swap(_last_b, _last_a);
        out.clear();
        return out;
    }

    Set all_items_set() {
        return Set::invert(empty_set());
    }

    Set &top() {
        return _stack.top();
    }

    const Set &top() const {
        return _stack.top();
    }

    void clear() {
        while (!_stack.empty()) {
            std::swap(_last_b, _last_a);
            _last_a = std::move(_stack.top());
            _stack.pop();
        }
    }

    auto begin() const {
        return top().begin();
    }

    auto end() const {
        return top().end();
    }

protected:
    std::stack<Set> _stack;
    void release_set(Set &a, Set &b) {
        _last_a = std::move(a);
        _last_b = std::move(b);
    }

    template<typename Fn>
    Ret do_and(Set &a, Set &b, Fn &fn) {
        Set s = empty_set();
        s.reserve(std::max(a.size(),b.size()));
        auto iter_a = a.begin();
        auto iter_b = b.begin();
        auto end_a = a.end();
        auto end_b = b.end();
        while (iter_a != end_a && iter_b != end_b) {
            if (iter_a->id < iter_b->id) ++iter_a;
            else if (iter_a->id> iter_b->id) ++iter_b;
            else {
                s.emplace_back(Item{iter_a->id, fn(iter_a->value, iter_b->value)});
                ++iter_a;
                ++iter_b;
            }
        }
        release_set(a, b);
        return push(std::move(s));
    }

    template<typename Fn>
    Ret do_or(Set &a, Set &b, Fn &fn) {
        Set s = empty_set();
        s.reserve(std::max(a.size(),b.size()));
        auto iter_a = a.begin();
        auto iter_b = b.begin();
        auto end_a = a.end();
        auto end_b = b.end();
        while (iter_a != end_a && iter_b != end_b) {
            if (iter_a->id < iter_b->id) {
                s.emplace_back(std::move(*iter_a++));;
            }
            else if (iter_a->id > iter_b->id) {
                s.emplace_back(std::move(*iter_b++));;
            }
            else {
                s.emplace_back(Item{iter_a->id, fn(iter_a->value, iter_b->value)});
                ++iter_a;
                ++iter_b;
            }
        }
        while (iter_a != end_a) {
             s.emplace_back(std::move(*iter_a++));;
        }
        while (iter_b != end_b) {
             s.emplace_back(std::move(*iter_b++));;
        }
        release_set(a, b);
        return push(std::move(s));
    }

    template<typename Fn>
    Ret do_xor_not(Set &a, Set &not_b, Fn &fn) {
        do_or(a, not_b, fn);
        do_and(_last_a, _last_b, move_first);
        Set d = pop();
        Set c = pop();
        do_diff(c, d);
        return NOT();

    }

    Ret do_xor(Set &a, Set &b) {
        Set s = empty_set();
        auto iter_a = a.begin();
        auto iter_b = b.begin();
        auto end_a = a.end();
        auto end_b = b.end();
        while (iter_a != end_a && iter_b != end_b) {
            if (iter_a->id < iter_b->id) {
                s.emplace_back(std::move(*iter_a++));;
            }
            else if (iter_a->id > iter_b->id) {
                s.emplace_back(std::move(*iter_b++));;
            }
            else {
                ++iter_a;
                ++iter_b;
            }
        }
        while (iter_a != end_a) {
             s.emplace_back(std::move(*iter_a++));;
        }
        while (iter_b != end_b) {
             s.emplace_back(std::move(*iter_b++));;
        }
        release_set(a, b);
        return push(std::move(s));
    }

    Ret do_diff(Set &a, Set &b) {
        Set s = empty_set();
        auto iter_a = a.begin();
        auto iter_b = b.begin();
        auto end_a = a.end();
        auto end_b = b.end();
        while (iter_a != end_a && iter_b != end_b) {
            if (iter_a->id< iter_b->id) {
                s.emplace_back(std::move(*iter_a++));;
            }
            else if (iter_a->id > iter_b->id) {
               ++iter_b;;
            }
            else {
                ++iter_a;
                ++iter_b;
            }
        }
        while (iter_a != end_a) {
             s.emplace_back(std::move(*iter_a++));;
        }
        release_set(a, b);
        return push(std::move(s));
    }

    Set _last_a;
    Set _last_b;

};

template<typename ValueT>
using RecordsetCalculator = RecordsetStackT<DocID, ValueT>;


using RecordsetCalculatorNoValue = RecordsetStackT<DocID, std::nullptr_t>;


}






#endif /* SRC_DOCDB_BINOPS_H_ */
