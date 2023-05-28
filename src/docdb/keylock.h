#pragma once
#ifndef SRC_DOCDB_KEYLOCK_H_
#define SRC_DOCDB_KEYLOCK_H_

#include "key.h"
#include <mutex>
#include <unordered_map>

namespace docdb {

template<typename T>
class KeyLock {
public:

    struct LockStatus {
        bool locked;
        bool replaced;
        T locked_for;
    };

    LockStatus lock_key(std::size_t rev, const RawKey &k, T val, const T &replace_if) {
        std::lock_guard _(_mx);
        auto z = _keys.emplace(k, std::pair(std::move(val), std::move(rev)));
        if (!z.second) {
            if (z.first->second.first != replace_if || z.first->second.second != rev) {
                return {false, false, z.first->second.first};
            } else {
                z.first->second.first = val;
                return {true, true, z.first->second.first};;
            }
        }
        _revs.emplace(rev, RowView(z.first->first));
        return {true, false, z.first->second.first};
    }

    void unlock_keys(std::size_t rev) {
        std::lock_guard _(_mx);
        auto qr = _revs.equal_range(rev);
        auto p = qr.first;
        while (p != qr.second) {
            _keys.erase(p->second);
            p = _revs.erase(p);
        }
    }


protected:

    struct HashKey {
        std::size_t operator()(const RawKey &k) const {
            std::hash<std::string_view> hasher;
            return hasher(k);
        }
    };

    std::mutex _mx;
    std::unordered_map<RawKey, std::pair<T, std::size_t>, HashKey> _keys;
    std::unordered_multimap<std::size_t, RawKey> _revs;
};

}



#endif /* SRC_DOCDB_KEYLOCK_H_ */
