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
#include <vector>

namespace docdb {

template<typename T, typename Derived>
class SetCalculatorBase {
public:

    using Set = std::vector<T>;

    ///Push set to the stack
    Derived &&push(Set x) {
        _stack.push(std::move(x));
        return dref();
    }

    ///Push unsorted set to the stack (it will be sorted)
    Derived &&push_unsorted(Set x) {
        std::sort(x.begin(), x.end());
        _stack.push(std::move(x));
        return dref();
    }

    ///Push unsorted set to the stack (it will be sorted)
    template<typename Cmp>
    Derived &&push_unsorted(Set x, Cmp &&cmp) {
        std::sort(x.begin(), x.end(), std::forward<Cmp>(cmp));
        _stack.push(std::move(x));
        return dref();
    }

    ///Returns true, if stack is empty
    bool empty() const {
        return _stack.empty();
    }

    ///Pop set from the stack
    Set pop() {
        if (_stack.empty()) return {};
        Set r = std::move(_stack.top());
        _stack.pop();
        return r;
    }

    ///Pop set from the stack to a variable
    Derived &&pop(Set &to) {
        Set r = pop();
        std::swap(r, to);
        return dref();
    }

    ///Perform operation AND above last two sets in stack
    Derived &&AND() {
        perform_binop([](Set &a, Set &b, Set &out){
            //if one is empty, result is empty
            if (a.empty() || b.empty()) return;
            std::set_intersection(a.begin(), a.end(), b.begin(),b.end(), std::back_inserter(out));
        });
        return dref();
    }
    ///Perform operation OR above last two sets in stack
    Derived &&OR() {
        perform_binop([](Set &a, Set &b, Set &out){
            if (a.empty()) {
                std::swap(out,b);
            }
            else if (b.empty()) {
                std::swap(out,a);
            }
            else {
                std::set_union(a.begin(), a.end(), b.begin(),b.end(), std::back_inserter(out));
            }
        });
        return dref();
    }
    ///Perform operation XOR above last two sets in stack
    Derived &&XOR() {
        perform_binop([](Set &a, Set &b, Set &out){
            if (a.empty()) {
                std::swap(out,b);
            }
            else if (b.empty()) {
                std::swap(out,a);
            }else {
                std::set_symmetric_difference(a.begin(), a.end(), b.begin(),b.end(), std::back_inserter(out));
            }
        });
        return dref();
    }
    ///Perform operation NOT+AND  above last two sets in stack
    /**
     * Performs operation NOT on top item and then AND which results to removing items from
     * second set which are in first set
     * @return
     */
    Derived &&NOT_AND() {
        perform_binop([](Set &a, Set &b, Set &out){
            if (a.empty()) return;

            if (b.empty()) {
                std::swap(out,a);
            }else {
                std::set_difference(b.begin(), b.end(), a.begin(),a.end(), std::back_inserter(out));
            }
        });
        return dref();
    }

    ///Create empty set
    /**
     * Main benefit of this function is to return empty with already preallocated memory. This
     * memory was allocated for other sets used previously in calculations.
     * The object can hold two no longer used sets in cache. So you can call this function
     * twice and you will receive two empty sets with preallocated memory. Any futher
     * call will return empty set with no prellocation.
     * @return
     */
    Set get_empty_set() {
        Set out = std::move(tmp2);
        tmp2 = std::move(tmp1);
        out.clear();
        return out;
    }

    ///Swap two items on top
    Derived &&SWAP() {
        if (_stack.size()<2) [[unlikely]] return dref();
        Set a = pop();
        std::swap(a,_stack.top());
        push(a);
        return dref();
    }

    ///Delete top argument
    Derived &&DELETE() {
        if (_stack.empty()) [[unlikely]] return dref();
        auto &t = (tmp1.size()<tmp2.size()?tmp1:tmp2);
        if (t.size() < _stack.top().size()) {
            std::swap(t,_stack.top().size());
        }
        _stack.pop();
        return dref();
    }


    ///retrieve top item
    const Set &top() const {
        return _stack.top();
    }

    ///determines, whether top item is empty
    /**
     * Useful especially when you AND sets. When top item is empty, you can stop
     * operation, because any AND combination with empty set is also empty set.
     * @return
     */
    bool top_is_empty() const {
        return !empty() && _stack.top().empty();
    }

protected:
    std::stack<Set> _stack;
    Set tmp1;
    Set tmp2;

    Derived &&dref() {
        return static_cast<Derived &&>(*this);
    }

    template<typename Fn>
    void perform_binop(Fn &&fn) {
        Set a = pop();
        Set b = pop();
        Set out = std::move(tmp1);
        out.clear();
        fn(a,b,out);
        tmp1 = std::move(a);
        tmp2 = std::move(b);
        push(std::move(out));
    }
};

template<typename T>
class SetCalculator: public SetCalculatorBase<T, SetCalculator<T> > {

};

class RecordSetCalculator: public SetCalculatorBase<DocID, RecordSetCalculator> {
public:

    using SetCalculatorBase<DocID, RecordSetCalculator>::push;

    template<typename A, typename B, auto c>
    RecordSetCalculator &&push(typename IndexView<A,B,c>::RecordSet &&rc) {
        return push(rc);
    }
    template<typename A, typename B, auto c>
    RecordSetCalculator &&push(typename IndexView<A,B,c>::RecordSet &rc) {
        Set s= this->get_empty_set();
        std::transform(rc.begin(), rc.end(), std::back_inserter(s), [](const auto &row){
            return row.id;
        });
        this->push_unsorted(std::move(s));
        return std::move(*this);
    }

    ///Loads documents from top set
    /**
     * @param view storage view
     * @param fn function which recives document info. Similar to view.find(id).
     */
    template<typename StorageView, typename Fn>
    void documents(const StorageView &view, Fn &&fn) {
        if (_stack.empty()) return;
        const Set &t = _stack.top();
        for (DocID id: t) {
            fn(view.find(id));
        }
    }


};


}



#endif /* SRC_DOCDB_BINOPS_H_ */
