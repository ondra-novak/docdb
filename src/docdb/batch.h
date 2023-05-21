#pragma once
#ifndef SRC_DOCDB_BATCH_H_
#define SRC_DOCDB_BATCH_H_

#include <leveldb/write_batch.h>
#include "buffer.h"
#include "simple_function.h"

#include <atomic>
namespace docdb {


class Revision {
public:
    Revision() {_rev = _cur_rev++;}
    std::size_t operator *() const {return _rev;}
    void update() {
        _rev = _cur_rev++;
    }
protected:
    std::size_t _rev;
    static std::atomic<std::size_t> _cur_rev;
};

inline std::atomic<std::size_t> Revision::_cur_rev = {0};

class Batch: public leveldb::WriteBatch {
public:
    static std::size_t max_batch_size;
    using Hook = SimpleFunction<void, bool>;
    using BufferType = Buffer<char, 32>;
    using HookBuffer = Buffer<Hook, 8>;
    using leveldb::WriteBatch::WriteBatch;

    Batch() = default;
    Batch(const Batch &other) = delete;
    Batch(Batch &&other) = default;
    Batch &operator = (const Batch &) = delete;
    Batch &operator = (Batch &&other) = default;


    BufferType &get_buffer() {
        _buffer.clear();
        return _buffer;
    }

    ///Add hook
    /** The hook is called, when batch is commited or rollbacked */
    void add_hook(Hook hk) {
        _hooks.push_back(hk);
    }

    void on_event(bool commit) noexcept {
        for (auto &c: _hooks) c(commit);
        WriteBatch::Clear();
        _hooks.clear();
        _rev.update();
    }

    void rollback() {
        on_event(false);
    }

    ~Batch() {
        for (auto &c: _hooks) c(false);
    }

    ///Returns true if the batch is big, so it should be committed
    bool is_big() const {
        return this->ApproximateSize() >= max_batch_size;
    }

    ///Retrieve batch's revision
    /**
     * Each batch has unique revision number, which allows to distinguish multiple
     * batch instances
     * @return revision id;
     */
    std::size_t get_revision() const {
        return *_rev;
    }

protected:
    BufferType _buffer;
    HookBuffer _hooks;
    Revision _rev;
};

inline std::size_t Batch::max_batch_size = 2*65536;


}




#endif /* SRC_DOCDB_BATCH_H_ */
