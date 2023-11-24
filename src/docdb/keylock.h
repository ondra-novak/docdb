#pragma once
#ifndef SRC_DOCDB_KEYLOCK_H_
#define SRC_DOCDB_KEYLOCK_H_

#include "key.h"

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace docdb {

///Prevents the thread to continue, if there is pending operation with given key
/**
 * It used to control multi-threading operations, while operations above keys must
 * be serialized. If there is pending operation with a key, the object blocks
 * execution until the key is unlocked.
 *
 * You can lock multiple keys in single batch. Deadlock is checked, so
 * if result of locking is deadlock, the operation fails, you always need to
 * check status of locking
 *
 */
template<typename KeyInfo = std::monostate>
class KeyLock {
public:

    using Key = RawKey;
    using Rev = std::size_t;

    struct KeyHashRev {
        Key kh;
        Rev rev;
        KeyInfo info;
    };

    enum LockState {
        //key is locked ok
        ok,
        //key is already locked for this batch
        already_locked,
        //key is locked for different batch and deadlock happened
        deadlock,
        //key is locked - valid for trylock
        locked
    };

    struct KeyHashRevWait: KeyHashRev {
        bool waiting = false;
    };

    ///Lock the key, check duplicates
    /**
     * @param rev batch revision
     * @param key key
     * @param info associated data
     * @param fn function is called, when key is already locked for this revision,
     * it conatins stored associated data. Function must decide, if this
     * is valid situation. It returns true, when this is valid situation,
     * @return state
     */
    template<typename Fn>
    LockState lock_key(std::size_t rev, const RawKey &key, const KeyInfo &info, Fn &&fn) {
        KeyHashRevWait kk{{key, rev, info}};
        std::unique_lock lk(_mx);
        bool waiting = false;
        while(true) {
            auto iter = std::find_if(_lst.begin(), _lst.end(), [&](const KeyHashRev &item){
                return item.kh== kk.kh;
            });
            if (iter == _lst.end()) {
                _lst.push_back(std::move(kk));
                if (waiting) {
                    _waitings.erase(std::remove_if(_waitings.begin(),_waitings.end(), [&](const KeyHashRev &item){
                        return item.rev == rev;
                    }));
                }
                return ok;
            }
            if (iter->rev == rev) {
                if (fn(iter->info)) {
                    iter->info = info;
                    return ok;
                }
                return already_locked;
            }
            if (!waiting) {
                if (check_deadlock(iter->rev, rev)) return deadlock;
                _waitings.push_back(kk);
                waiting = true;
            }
            iter->waiting = true;
            _cond.wait(lk);
        }
    }

    ///Lock the key
    /**
     * @param rev batch revision
     * @param key key to lock
     * @retval true success
     * @retval false failure, deadlock
     */
    LockState lock_key(std::size_t rev, const RawKey &key) {
        KeyInfo empty = {};
        return lock_key(rev,key,empty,[](auto &&){return false;});
    }

    ///Try lock the key, do not block
    /**
     * @param rev batch revision
     * @param key key to lock
     * @retval true success
     * @retval false failure, somebody already locked the key previously
     */
    LockState try_lock_key(std::size_t rev, const RawKey &key) {
        std::hash<std::string_view> hasher;
        Key kh = hasher(key);
        std::unique_lock lk(_mx);
        auto iter = std::find_if(_lst.begin(), _lst.end(), [&](const KeyHashRev &item){
            return item.kh== kh;
        });
        if (iter == _lst.end()) {
            _lst.push_back(KeyHashRevWait{{kh, rev}});
            return ok;
        }
        if (iter->rev == rev) {
            return already_locked;
        }
        return locked;
    }

    ///Unlock all keys owned by given batch's revision
    /**
     * @param rev revision of the batch
     */
    void unlock_keys(std::size_t rev) {
        bool signal = false;
        std::unique_lock lk(_mx);
        auto iter = std::remove_if(_lst.begin(), _lst.end(), [&](const KeyHashRevWait &item){
            bool hit = item.rev == rev;
            signal = signal | (item.waiting & hit);
            return hit;
        });
        _lst.erase(iter, _lst.end());
        lk.unlock();
        if (signal) _cond.notify_all();
    }


protected:
    using LockedList = std::vector<KeyHashRevWait>;
    using WaitList = std::vector<KeyHashRev>;


    bool check_deadlock(Rev req, Rev owner) const {
        //I am waiting for myself - this is deadlock
        if (req == owner) return true;
        //find what I am owning
        for (const KeyHashRevWait &own: _lst) {
            if (own.rev == req) {
                //found ownership, now we need find who is waiting for it
                for (const KeyHashRev &wt: _waitings) {
                    //found that this key is waited by someone
                    if (wt.kh == own.kh) {
                        //recursively check, whether the waiting owner
                        //does own which is also waited
                        if (check_deadlock(wt.rev, owner)) return true;
                    }
                }
            }
        }
        return false;
    }

    std::mutex _mx;
    std::condition_variable _cond;
    LockedList _lst;
    WaitList _waitings;

};


}



#endif /* SRC_DOCDB_KEYLOCK_H_ */
