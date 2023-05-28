#pragma once
#ifndef SRC_DOCDB_BATCH_H_
#define SRC_DOCDB_BATCH_H_

#include <leveldb/write_batch.h>
#include "buffer.h"

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

class Batch;
class Database;

class AbstractBatchNotificationListener {
public:
    ///Batch is about to be committed
    /**
     * You can include additional data to the batch.
     * @param b batch is being commited
     * @note exception thown from this handler causes that batch is rollbacked, and
     * the function commit_batch() throws this exception
     */
    virtual void before_commit(Batch &b) = 0;
    ///Batch has been committed, all information in the batch are visible
    /**
     * @param rev batch's revision
     */
    virtual void after_commit(std::size_t rev) noexcept = 0;
    ///Batch has been rollbacked
    /**
     * @param rev batch's revision
     */
    virtual void after_rollback(std::size_t rev) noexcept = 0;
    ///Destructor
    virtual ~AbstractBatchNotificationListener() = default;
};


inline std::atomic<std::size_t> Revision::_cur_rev = {0};

class Batch: public leveldb::WriteBatch {
public:
    static std::size_t max_batch_size;
    using BufferType = Buffer<char, 32>;
    using NtfBuffer = Buffer<AbstractBatchNotificationListener *, 32>;
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

    ~Batch() {
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->after_rollback(*_rev);
        }
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

    ///Request synchronous write
    void sync_write() {
        _sync = true;
    }

    void add_listener(AbstractBatchNotificationListener *listener) {
        std::size_t bloom;
        if constexpr(sizeof(AbstractBatchNotificationListener *) == 4) {
            auto bit = ((reinterpret_cast<std::uintptr_t>(listener) * 2654435769ull) >> 28) & 0x1F;
            bloom  = 1<<bit;
        } else {
            auto bit = ((reinterpret_cast<std::uintptr_t>(listener) * 11400714819323198485ull) >> 58) & 0x3F;
            bloom  = 1<<bit;
        }
        if ((bloom & _ntf_bloom) == 0) {
            _ntf.push_back(listener);
            _ntf_bloom |= bloom;
        } else {
            auto iter = std::find(_ntf.begin(), _ntf.end(), listener);
            if (iter == _ntf.end()) {
                _ntf.push_back(listener);
            }
        }
    }


protected:
    void before_commit() {
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->before_commit(*this);
        }
    }

    void after_commit() noexcept {
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->after_commit(*_rev);
        }
        WriteBatch::Clear();
        _ntf.clear();
        _ntf_bloom = 0;
        _rev.update();
    }
    void after_rollback() noexcept {
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->after_rollback(*_rev);
        }
        WriteBatch::Clear();
        _ntf.clear();
        _ntf_bloom = 0;
        _rev.update();
    }

    friend class Database;

    BufferType _buffer;
    NtfBuffer _ntf;
    std::uintptr_t _ntf_bloom = 0;
    Revision _rev;
    bool _sync = false;



};

inline std::size_t Batch::max_batch_size = 2*65536;


}




#endif /* SRC_DOCDB_BATCH_H_ */
