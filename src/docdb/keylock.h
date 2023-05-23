#pragma once
#ifndef SRC_DOCDB_KEYLOCK_H_
#define SRC_DOCDB_KEYLOCK_H_

#include <mutex>
#include <unordered_set>

namespace docdb {

class KeyLock {
public:

    bool lock_key(const std::string_view &key) {
        std::lock_guard _(_mx);
        auto ins = _keys.insert(key);
        return ins.second;
    }
    void unlock_key(const std::string_view &key) {
        std::lock_guard _(_mx);
        _keys.erase(key);
    }

    template<typename Iter>
    void unlock_keys(Iter beg, Iter end) {
        std::lock_guard _(_mx);
        for (Iter i = beg; i != end; ++i) {
            _keys.erase(std::string_view(*i));
        }
    }

protected:
    std::mutex _mx;
    std::unordered_set<std::string_view> _keys;
};
class KeyHolder {
public:

    KeyHolder(const std::string_view &s)
        :_ptr(std::make_unique<char>(s.size())), _sz(s.size()) {
        std::copy(s.begin(), s.end(), _ptr.get());
    }
    operator std::string_view() const {return {_ptr.get(), _sz};};

protected:
    std::unique_ptr<char> _ptr;
    std::size_t _sz;

};

}



#endif /* SRC_DOCDB_KEYLOCK_H_ */
