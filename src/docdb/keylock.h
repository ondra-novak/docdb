#pragma once
#ifndef SRC_DOCDB_KEYLOCK_H_
#define SRC_DOCDB_KEYLOCK_H_

#include "key.h"

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace docdb {

enum class KeyLockState {
    //key is locked ok
    ok,
    //key is locked and value has been replaced
    replaced,
    //key is locked and condition test failed
    cond_failed,
    //key is locked for different batch and deadlock happened
    deadlock,

};

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
template<typename Value = std::monostate>
class KeyLock {
public:

    using Key = RawKey;
    using Rev = std::size_t;


    struct KeyRec {
        Key key;
        Rev rev;
        Value info;
        bool waiting = false;
    };

    struct Waiting {
        const Key *key = nullptr;
        const Rev *rev = nullptr;
    };


    ///Lock the key and perform compare and swap
    /**
     * @param rev batch revision (or anothe batch identification id)
     * @param key key to lock
     * @param cond condition, used when key is already locked. If the key-info contains the same
     * value, lock is successed, and the value is replaced by the new value. If there is
     * different value, error status is returned and value is replaced with stored value
     * @param new_value new value which is replaced if the lock already held
     * @retval ok key is locked ok
     * @retval replaced key has been already locked, condition successed and value replaced
     * @retval cond_failed key has been already locked, condition failed
     * @retval deadlock key is locked by different batch and deadlock would happen
     */

    KeyLockState lock_key_cas(std::size_t rev, const RawKey &key, Value &cond, const Value &new_val) {
        std::unique_lock lk(_mx);
        bool waiting = false;
        while(true) {
            auto iter = std::find_if(_lst.begin(), _lst.end(), [&](const KeyRec &item){
                return item.key== key;
            });
            if (iter == _lst.end()) {
                _lst.push_back(KeyRec{key, rev, new_val});
                if (waiting) {
                    _waitings.erase(std::remove_if(_waitings.begin(),_waitings.end(), [&](const Waiting &item){
                        return *item.rev == rev;
                    }));
                }
                return KeyLockState::ok;
            }
            if (iter->rev == rev) {
                if (iter->info == cond) {
                    iter->info = new_val;
                    return KeyLockState::replaced;
                } else {
                    cond = iter->info;
                    return KeyLockState::cond_failed;
                }
            }
            if (!waiting) {
                if (check_deadlock(iter->rev, rev)) return KeyLockState::deadlock;
                _waitings.push_back({&key, &rev});
                waiting = true;
            }
            iter->waiting = true;
            _cond.wait(lk);
        }
    }

    ///Lock the key - no condition test. It is expected, that condition is {}
    /**
     * @param rev batch revision
     * @param key key to lock
     * @retval true success
     * @retval false failure, deadlock
     */
    bool lock_key(std::size_t rev, const RawKey &key) {
        Value empty = {};
        auto res = lock_key_cas(rev, key, empty, empty);
        return (res == KeyLockState::ok) | (res == KeyLockState::replaced);

    }

    ///Unlock all keys owned by given batch's revision
    /**
     * @param rev revision of the batch
     */
    void unlock_keys(std::size_t rev) {
        bool signal = false;
        std::unique_lock lk(_mx);
        auto iter = std::remove_if(_lst.begin(), _lst.end(), [&](const KeyRec &item){
            bool hit = item.rev == rev;
            signal = signal | (item.waiting & hit);
            return hit;
        });
        _lst.erase(iter, _lst.end());
        lk.unlock();
        if (signal) _cond.notify_all();
    }


protected:
    using LockedList = std::vector<KeyRec>;
    using WaitList = std::vector<Waiting>;


    bool check_deadlock(Rev req, Rev owner) const {
        //I am waiting for myself - this is deadlock
        if (req == owner) return true;
        //find what I am owning
        for (const KeyRec &own: _lst) {
            if (own.rev == req) {
                //found ownership, now we need find who is waiting for it
                for (const Waiting &wt: _waitings) {
                    //found that this key is waited by someone
                    if (*wt.key == own.key) {
                        //recursively check, whether the waiting owner
                        //does own which is also waited
                        if (check_deadlock(*wt.rev, owner)) return true;
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
