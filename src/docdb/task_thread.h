#pragma once
#ifndef SRC_DOCDB_TASK_THREAD_H_
#define SRC_DOCDB_TASK_THREAD_H_
#include <mutex>
#include <thread>
#include <concepts>
#include <condition_variable>
#include <deque>

namespace docdb {


class TaskThread {
public:

    template<std::invocable<> Fn>
    void run(Fn &&fn);

    TaskThread() = default;
    ~TaskThread();


    static thread_local TaskThread *_this_thread;

protected:
    class AbstractAction {
    public:
        virtual void run() noexcept = 0;
        virtual ~AbstractAction() = default;
    };

    using Action = std::unique_ptr<AbstractAction>;

    bool running = false;

    std::thread _wrk;
    std::mutex _mx;
    std::deque<Action> _q;

    void worker();

};

inline thread_local TaskThread *TaskThread::_this_thread = nullptr;



template<std::invocable<> Fn>
inline void TaskThread::run(Fn &&fn) {

    class Inst : public AbstractAction {
    public:
        Inst(Fn &&fn):_fn(std::move(fn)) {}
        virtual void run() noexcept override{
            _fn();
        }
    protected:
        std::decay_t<Fn> _fn;
    };

    std::unique_lock lk(_mx);
    _q.push_back(std::make_unique<Inst>(std::forward<Fn>(fn)));
    if (!running) {
        running = true;
        if (_wrk.joinable()) _wrk.join();
        _wrk = std::thread([this]{
            worker();
        });
    }
}

inline TaskThread::~TaskThread() {
    std::unique_lock lk(_mx);
    _q.clear();
    lk.unlock();
    if (_wrk.joinable()) {
        if (_wrk.get_id() == std::this_thread::get_id()) {
            _wrk.detach();
            _this_thread = nullptr;
        } else {
            _wrk.join();
        }
    }
}

inline void TaskThread::worker() {
    _this_thread = this;
    std::unique_lock lk(_mx);
    while (!_q.empty()) {
        {
            auto a = std::move(_q.front());
            _q.pop_front();
            lk.unlock();
            a->run();
            if (!_this_thread) return;
        }
        lk.lock();
    }
    running = false;
}

}

#endif /* SRC_DOCDB_TASK_THREAD_H_ */
