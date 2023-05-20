#pragma once
#ifndef SRC_DOCDB_OBSERVER_H_
#define SRC_DOCDB_OBSERVER_H_
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include "buffer.h"

namespace docdb {

template<typename Fn>
class ObserverList {
public:

    std::size_t register_observer(Fn &&fn) {
        std::unique_lock _(_mx);
        auto id = _next_id++;
        _observers.emplace_back(id, std::forward<Fn>(fn));
        return id;
    }

    void unregister_observer(std::size_t id) {
        std::unique_lock _(_mx);
        _observers.erase(std::remove_if(_observers.begin(), _observers.end(), [&](const auto &x){
            return x.first == id;
        }), _observers.end());
    }

    template<typename ... Args>
    void call(Args && ... args) {
        Buffer<std::size_t, 8> kick;
        std::shared_lock _(_mx);
        for (const auto &c: _observers) {
            if (!c.second(std::forward<Args>(args)...)) {
                kick.push_back(c.first);
            }
        }
        if (!kick.empty()) {
            _.unlock();
            std::unique_lock lk(_mx);
            auto iter = kick.begin();
            auto end = kick.end();
            _observers.erase(std::remove_if(_observers.begin(), _observers.end(),[&](const auto &x){
                if (iter != end && *iter == x.first) {
                    ++iter;
                    return true;
                }
                return false;
            }), _observers.end());
        }
    }

protected:
    std::shared_mutex _mx;
    std::size_t _next_id = 1;
    std::vector<std::pair<std::size_t, Fn> > _observers;


};
#if 0

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
#endif

}

#endif /* SRC_DOCDB_OBSERVER_H_ */

