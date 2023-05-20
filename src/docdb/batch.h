#pragma once
#ifndef SRC_DOCDB_BATCH_H_
#define SRC_DOCDB_BATCH_H_

#include <leveldb/write_batch.h>
#include "buffer.h"

namespace docdb {


class Batch: public leveldb::WriteBatch {
public:
    using BufferType = Buffer<char, 32>;
    using leveldb::WriteBatch::WriteBatch;

    Batch() = default;
    Batch(const Batch &other) = delete;
    Batch(Batch &&other):leveldb::WriteBatch(std::move(other)),hooks(other.hooks) {
        other.hooks = nullptr;
    }
    Batch &operator = (const Batch &) = delete;
    Batch &operator = (Batch &&other) {
        if (this != &other) {
            leveldb::WriteBatch::operator =(std::move(other));
            call_commit_hook(false);
            hooks = other.hooks;
            other.hooks = nullptr;
        }
        return *this;
    }


    BufferType &get_buffer() {
        _buffer.clear();
        return _buffer;
    }

    class CommitHook { // @suppress("Miss copy constructor or assignment operator")
    public:
        CommitHook(CommitHook *x):next(x) {}
        CommitHook *next = nullptr;
        virtual void run(bool commit) noexcept = 0;
        virtual ~CommitHook() = default;
    };

    template<typename Fn>
    void add_commit_hook(Fn &&fn) {
        class Inst: public CommitHook {
        public:
            Inst(Fn &&fn, CommitHook *nx):CommitHook(nx),_fn(std::forward<Fn>(fn)) {}
            virtual void run(bool commit) noexcept override {
                _fn(commit);
            }
        protected:
            Fn _fn;
        };
        hooks = new Inst(std::forward<Fn>(fn), hooks);
    }

    void call_commit_hook(bool commit) {
        while (hooks) {
           auto x = hooks;
           hooks = hooks->next;
           x->run(commit);
           delete x;
        }
    }


    void rollback() {
        Clear();
        call_commit_hook(false);
    }

    ~Batch() {
        call_commit_hook(false);
    }

protected:
    BufferType _buffer;
    CommitHook *hooks = nullptr;
};


}




#endif /* SRC_DOCDB_BATCH_H_ */
