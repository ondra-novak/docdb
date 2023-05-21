#pragma once
#ifndef SRC_DOCDB_OBSERVER_H_
#define SRC_DOCDB_OBSERVER_H_
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include "buffer.h"

#include <atomic>
namespace docdb {





template<typename Fn>
class ObserverList {
public:

    std::size_t register_observer(Fn &&fn) {
        std::lock_guard _(_mx);
        wait_exclusive();
        auto id = _next_id++;
        _observers.emplace_back(id, std::forward<Fn>(fn));
        return id;
    }

    void unregister_observer(std::size_t id) {
        std::lock_guard  _(_mx);
        wait_exclusive();
        _observers.erase(std::remove_if(_observers.begin(), _observers.end(), [&](const auto &x){
            return x.first == id;
        }), _observers.end());
    }

    template<typename ... Args>
    void call(Args && ... args) {
        std::unique_lock lk(_mx);
        if (_observers.empty()) return;
        _locks.fetch_add(1, std::memory_order_relaxed);
        lk.unlock();

        Buffer<std::size_t, 8> kick;
        for (const auto &c: _observers) {
            if (!c.second(std::forward<Args>(args)...)) {
                kick.push_back(c.first);
            }
        }
        if (_locks.fetch_sub(1, std::memory_order_relaxed) - 1 == 0) {
            _locks.notify_one();
        }

        if (!kick.empty()) {

            std::unique_lock lk(_mx);
            wait_exclusive();
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
    std::mutex _mx;
    std::atomic<int> _locks = {0};
    std::size_t _next_id = 1;
    std::vector<std::pair<std::size_t, Fn> > _observers;
    void wait_exclusive() {
        auto l = _locks.load(std::memory_order_relaxed);
        while (l != 0) {
            _locks.wait(l,std::memory_order_relaxed);
            l = _locks.load(std::memory_order_relaxed);
        }
    }


};

}

#endif /* SRC_DOCDB_OBSERVER_H_ */

