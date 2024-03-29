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
     *
     * @note it is still possible that batch will be rollbacked after this notification
     */
    virtual void before_commit(Batch &b) = 0;
    ///Batch has been committed, all information from the batch are visible in the DB
    /**
     * @param rev batch's revision
     */
    virtual void after_commit(std::size_t rev) noexcept = 0;
    ///Batch has been rollbacked
    /**
     * Any data stored into batch are lost.
     *
     * @param rev batch's revision
     */
    virtual void on_rollback(std::size_t rev) noexcept = 0;
    ///Destructor
    virtual ~AbstractBatchNotificationListener() = default;
};


inline std::atomic<std::size_t> Revision::_cur_rev = {0};

class Batch: public leveldb::WriteBatch {
public:
    static std::size_t max_batch_size;
    using BufferType = Buffer<char>;
    using NtfBuffer = Buffer<AbstractBatchNotificationListener *, 10>;
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
        if (!_done) {
            for (std::size_t i = 0; i < _ntf.size(); ++i) {
                _ntf[i]->on_rollback(*_rev);
            }
        }
    }

    ///Returns true if the batch is big, so it should be committed
    bool is_big() const {
        return this->ApproximateSize() >= max_batch_size;
    }

    ///Determines, whether batch is already closed
    /**
     * @retval false batch is not closed, it was neither commiter nor rollbacked
     * @retval true batch is closed now, you should to destroy it or reset it
     */
    bool closed() const {
        return _done;
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

    ///Adds listener, which receives notification about transaction state
    /**
     * @param listener listener - each listener can be added only once,
     * duplicate attempts are ignoredTransactionObserverFunction
     *
     */
    bool add_listener(AbstractBatchNotificationListener *listener) {
        auto iter = std::find(_ntf.begin(), _ntf.end(), listener);
        if (iter == _ntf.end()) {
            if (_done) throw std::logic_error("Batch is already commited. You need to reset()");
            _ntf.push_back(listener);
            return true;
        }
        return false;
    }

    ///Resets the batch, so it can be reused now (it is mandatory)
    /**
     * @note if the batch is not closed, it is rollbacked
     */
    void reset() {
        if (!_done) {
            for (std::size_t i = 0; i < _ntf.size(); ++i) {
                _ntf[i]->on_rollback(*_rev);
            }
        }
        WriteBatch::Clear();
        _ntf.clear();
        _rev.update();
        _done = false;
        _sync = false;
    }


protected:
    void before_commit() {
        if (_done) throw std::logic_error("Batch is already commited, can't commit twice");
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->before_commit(*this);
        }
    }

    void after_commit() noexcept {
        for (std::size_t i = 0; i < _ntf.size(); ++i) {
            _ntf[i]->after_commit(*_rev);
        }
        _done = true;
    }

    void on_rollback() noexcept {
        if (!_done) {
            for (std::size_t i = 0; i < _ntf.size(); ++i) {
                _ntf[i]->on_rollback(*_rev);
            }
            _done = true;
        }
    }


    friend class Database;

    BufferType _buffer;
    NtfBuffer _ntf;
    Revision _rev;
    bool _sync = false;
    bool _done = false;



};

inline std::size_t Batch::max_batch_size = 2*65536;


}




#endif /* SRC_DOCDB_BATCH_H_ */
