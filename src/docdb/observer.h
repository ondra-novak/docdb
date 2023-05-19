#pragma once
#ifndef SRC_DOCDB_OBSERVER_H_
#define SRC_DOCDB_OBSERVER_H_
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace docdb {


template<typename T> class Observer;

///Observer
/**
 * Simplified version of std::function, expect it uses shared ptr
 * This allows to keep function valid even if it was removed
 */
template<typename ... Args> class Observer<bool(Args ...)> {

    class Abstract {
    public:
        bool dead = false;
        virtual ~Abstract() = default;
        virtual bool call(Args ... args) const = 0;
    };


public:
    Observer() = default;
    Observer(const Observer &) = default;
    Observer(Observer &&) = default;
    Observer &operator=(const Observer &) = default;
    Observer &operator=(Observer &&) = default;

    template<typename Fn>
    CXX20_REQUIRES(std::same_as<decltype(std::declval<Fn>()(std::declval<Args>()...)), bool>)
    Observer(Fn &&fn) {

        class Inst: public Abstract {
        public:
            Inst(Fn &&fn): _fn(std::forward<Fn>(fn)) {}
            virtual bool call(Args ... args) const  override {
                return _fn(args...);
            }
            std::decay_t<Fn> _fn;
        };
        _ptr = std::make_shared<Inst>(std::forward<Fn>(fn));
    }

    ///Observer was initialized
    bool inited() const {
        return _ptr != nullptr;
    }
    ///Observer was initialized, but it is dead (reported false)
    bool dead() const {
        return inited() && _ptr->dead;
    }
    ///Observer is inactive (dead or not inited)
    bool inactive() const {
        return _ptr == nullptr || _ptr->dead;
    }
    ///call observer
    void operator()(Args ... args) {
        _ptr->dead = _ptr->dead ||  !_ptr->call(args...);
    }

protected:


    std::shared_ptr<Abstract> _ptr;
};




template<typename _Observer>
class ObserverList {
public:

    void register_observer(_Observer obs) {
        std::lock_guard _(_mx);
        auto iter = std::find_if(_list.begin(), _list.end(), [](const _Observer &x){
            return !x.inited();
        });
        if (iter == _list.end()) {
            _list.emplace_back(std::move(obs));
        } else {
            *iter = std::move(obs);
        }
    }

    template<typename ... Args>
    void call(Args && ... args) {
        //collect all dead
        std::vector<_Observer> kickup;
        //lock now
        _mx.lock();
        std::size_t cnt = _list.size();
        for (std::size_t i = 0; i < cnt; i++) {
            _Observer &obs = _list[i];
            if (obs.inited()) {
                if (obs.dead()) {
                    //kick dead
                    kickup.push_back(std::move(obs));
                }
                else {
                    //claim instance
                    _Observer inst = obs;
                    //unlock
                    _mx.unlock();
                    //call the observer unlocked
                    inst(args...);
                    //lock back
                    _mx.lock();
                }
            }
        }
        //unlock now
        _mx.unlock();
    }

protected:
    std::mutex _mx;
    std::vector<_Observer> _list;
};
}

#if 0

///Internal component
/**
 * Allows to manage observers and execute them in multiple threads without
 * blocking while they still can be added and removed in fly. The
 * observer also returns true to continue observing and false to
 * stop observing
 *
 * (however, it need to be written, that observer still can receive
 * an event even if it returned false prevously, because there can
 * be still
 *
 *
 * @tparam Fn
 * @tparam Args
 */
template<typename Fn, typename ... Args>
class ObserverMap {
public:

    using ObserverReg = std::pair<int, Fn>;
    using ObserverList = std::vector<ObserverReg>;
    using PObserverList = std::shared_ptr<ObserverList>;
    using LkObserverList = std::pair<std::size_t,std::shared_ptr<ObserverList> >;

    class FailedList {
    public:
        static const int max_count = 16;
        std::array<int, max_count> _items;
        int _count = 0;
        void push_back(int val) {
            _count = _count + (_count < max_count);
            _items[_count-1] = val;
        }
        auto begin() const {return _items.begin();}
        auto end() const {return begin()+_count;}
    };

    int register_lk(Fn &&fn) {
        int r = ++_h;
        if (_cur_list->capacity() > _cur_list->size() || _cur_list.unique()) {
            _cur_list->push_back(ObserverReg(r, std::forward<Fn>(fn)));
        } else {
            PObserverList nl = std::make_shared<ObserverList>(*_cur_list);
            nl->push_back(ObserverReg(r, std::forward<Fn>(fn)));
            _cur_list = nl;
        }
        return r;
    }

    void unregister_lk(int x) {
        if (_cur_list.unique()) {
            _cur_list->erase(std::remove_if(_cur_list->begin(), _cur_list->end(), [&](const ObserverReg &obs){
                return obs.first == x;
            }), _cur_list->end());
        } else {
            FailedList f;
            f.push_back(x);
            remove_failed_lk(f);
        }
    }

    LkObserverList get_list_lk() const {
        return {_cur_list->size(),_cur_list};
    }

    void remove_failed_lk(const FailedList &f) {
        PObserverList nl = std::make_shared<ObserverList>();
        std::set_difference(_cur_list->begin(),
                _cur_list->end(),
                f.begin(),f.end(),std::back_inserter(*nl),
                [](const auto &a, const auto &b) {
           if constexpr(std::is_convertible_v<decltype(a), int>) {
               return a < b.first;
           } else {
               return a.first < b;
           }
        });
        _cur_list = nl;
    }

    static FailedList call(const LkObserverList &list, Args ... args) {
        FailedList failed;
        for (std::size_t i = 0; i< list.first; i++) {
            const ObserverReg &reg = (*list.second)[i];
            if (!reg.second(args...)) failed.push_back(reg.first);
        }
        return failed;
    }






protected:
    PObserverList _cur_list;
    int _h = 0;

};

}
#endif


#endif /* SRC_DOCDB_OBSERVER_H_ */

