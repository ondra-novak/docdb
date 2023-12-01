#ifndef SRC_DOCDB_SCHEDULER_H_
#define SRC_DOCDB_SCHEDULER_H_
#include <thread>
#include <queue>
#include <functional>
#include <mutex>

namespace docdb {


///Contains one shot task
/**
 * One shot task can be called just once. It is movable object
 *
 * Note if you destroy the object, the task is still called
 */
class OneShotTask {

    class _ {
    public:
        virtual ~_() = default;
    };

public:


    template<std::invocable<> Fn>
    OneShotTask(Fn &&fn) {
        class __: public _ {
        public:
            __(Fn &&fn):_fn(std::forward<Fn>(fn)) {}
            virtual ~__() {
                _fn();
            }
        protected:
            std::decay_t<Fn> _fn;
        };
        _t = std::make_unique<__>(std::forward<Fn>(fn));
    }
    void operator()() {
        _t.reset();
    }
protected:
    std::unique_ptr<_> _t;
};


///Schedules and eventually runs a task at background. Only one task can be run at time
/**
 * The class allocates one thread for run. The thread stops, when there are no more tasks.
 * You can destroy this object from inside of context of its own thread, the execution
 * continues in detached thread.
 */
class Scheduler {
public:

    template<std::invocable<> Task>
    void run(Task task) {
        std::lock_guard lk(_mx);
        if (!_running) {
            if (_thr.joinable()) {
                _thr.join();
            }
            _thr = std::thread([this]{
                std::unique_lock lk(_mx);
                current = this;
                while (!_q.empty()) {
                    OneShotTask t = std::move(_q.front());
                    _q.pop();
                    lk.unlock();
                    t();
                    if (current != this) return;
                    lk.lock();
                }
                _running = false;
            });
            _running = true;
        }
        _q.push(OneShotTask(std::forward<Task>(task)));
    }

    void clear() {
        std::lock_guard lk(_mx);
        _q = {};
    }

    ~Scheduler() {
        clear();
        if (_thr.joinable()) {
            if (current == this) {
                _thr.detach();
                current = nullptr;
            } else {
                _thr.join();
            }
        }
    }

    static thread_local Scheduler *current;

protected:
    std::thread _thr;
    std::mutex _mx;
    std::queue<OneShotTask> _q;
    bool _running = false;
};

inline thread_local Scheduler *Scheduler::current = nullptr;

}




#endif /* SRC_DOCDB_SCHEDULER_H_ */
