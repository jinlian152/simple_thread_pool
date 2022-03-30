//author jinlian
//2022-01-28

#ifndef __TASK_MANAGER_H__
#define __TASK_MANAGER_H__
#include <vector>
#include <memory>
#include <atomic>
#include <functional>
#include <deque>
#include <condition_variable>
#include <thread>
#include <chrono>
#include <map>
#include <mutex>

extern const int g_default_worker_count;

class Runnable {
public:
    typedef std::function<int(void)> Handler;
    typedef std::function<void(void)> CallBack;
    Runnable() : done_callback_(NULL) {};
    virtual ~Runnable() = default;
    virtual void run() = 0;
    virtual void set_call_back(CallBack callback) {
        done_callback_ = callback;
    }

public:
    CallBack done_callback_;
};

class RunnerImpl : public Runnable{
public:
    RunnerImpl(Runnable::Handler handler, const std::string& name = "") 
        : handler_(handler)
        , task_name_(name)
        , ret_code_(-1) {}
    virtual ~RunnerImpl() {}
    virtual void run() {
        ret_code_ = handler_();
    }

public:
    Runnable::Handler handler_;
    int ret_code_;
    std::string task_name_;
};

class ParallelTask : public Runnable {
public:
    typedef std::function<void (*)()> TaskRunner;
    ParallelTask(std::shared_ptr<Runnable> runnable,
                std::shared_ptr<int> task_count,
                std::shared_ptr<std::condition_variable> cond,
                std::shared_ptr<std::mutex> mutex,
                uint64_t expiration = 0ULL)
        : runnable_(runnable) {
        if (expiration != 0ULL) {
          expireTime_.reset(
              new std::chrono::steady_clock::time_point(
                  std::chrono::steady_clock::now() + std::chrono::milliseconds(expiration)
                )
            );
        }
        task_count_ = task_count;
        cond_ = cond;
        mutex_ = mutex;
    }

    virtual void run() {
        //todo: do expire check.
        if (nullptr !=  runnable_) {
            runnable_->run();
            if (nullptr != runnable_->done_callback_) {
                runnable_->done_callback_();
            }
        }

        std::unique_lock<std::mutex> lock(*mutex_.get());
        *task_count_ -= 1;
        int count = *task_count_ ;
        if(count == 0) {
            cond_->notify_all();
        }
    }

    const std::unique_ptr<std::chrono::steady_clock::time_point> & getExpireTime() const { return expireTime_; }

    // std::shared_ptr<Runnable> getRunnable() { return runnable_; }

private:
    std::shared_ptr<Runnable> runnable_;
    std::shared_ptr<int> task_count_;
    std::shared_ptr<std::condition_variable> cond_;
    std::shared_ptr<std::mutex> mutex_;
    std::unique_ptr<std::chrono::steady_clock::time_point> expireTime_;

private:
    ParallelTask();
    ParallelTask(ParallelTask&);
    ParallelTask& operator=(ParallelTask&);
};

// constexpr std::chrono::duration<long double, std::milli> operator ""ms(long double ms);

class ThreadManager;
typedef std::function<int(void)> WorkerCallback;

class Worker {
public:
    Worker(ThreadManager* thread_mgr,
            std::shared_ptr<std::condition_variable> worker_cv,
            std::shared_ptr<std::mutex> task_mutex,
            std::shared_ptr< std::deque< std::shared_ptr<Runnable> > > tasks) 
        : owner_(thread_mgr)
        , worker_cv_(worker_cv)
        , task_mutex_(task_mutex)
        , tasks_(tasks)
        , need_stop_(false)
        , before_start_(nullptr)
        , before_end_(nullptr) {
    }

    ~Worker() {}

    void start() {
        thread_ = std::thread(&Worker::run, this);
    }

    void run();

    void set_start_callback(WorkerCallback before_start) { before_start_ = before_start; };
    void set_end_callback(WorkerCallback before_end) { before_end_ = before_end; };
    void join() {
        thread_.join();
    }

    void stop() {
        need_stop_ = true;
    }
public:
    static int sleep_count_;
private:
    ThreadManager* owner_;
    std::atomic_bool need_stop_;
    std::thread thread_;
    WorkerCallback before_start_;
    WorkerCallback before_end_;

    std::shared_ptr<std::condition_variable> worker_cv_;
    std::shared_ptr< std::deque< std::shared_ptr<Runnable> > > tasks_;
    std::shared_ptr<std::mutex> task_mutex_;

private:
    Worker();
    Worker(Worker&);
    Worker& operator=(Worker&);
};

class ThreadManager {
public:
    ThreadManager() { }

    ~ThreadManager() { };

    void init(int worker_count,
            WorkerCallback berfor_start,
            WorkerCallback before_end);

    void start ();

    int get_task_count() {
        std::unique_lock<std::mutex> lock(*task_mutex_);
        return tasks_->size();
    }

    void add_task(std::shared_ptr<Runnable> value);

    void stop();

    static std::shared_ptr<ThreadManager> get_instance();

private:
    int worker_count_;
    std::shared_ptr<std::condition_variable> worker_cv_;
    std::vector< std::shared_ptr<Worker> > workers_;
    std::shared_ptr< std::deque< std::shared_ptr<Runnable> > > tasks_;
    std::shared_ptr<std::mutex> task_mutex_;

    static std::shared_ptr<ThreadManager> instance_;
    static std::mutex instance_mutex_;
};

typedef std::shared_ptr<Runnable> RUNNABLE_PTR;

class ParallelTasksRunner {
public:
    void AddTasks(const std::vector<RUNNABLE_PTR>& tasks);
    void Wait(int timeout_ms);
private:
    std::shared_ptr<int> task_count_;
    std::shared_ptr<std::condition_variable> cond_;
    std::shared_ptr<std::mutex> mutex_;
};

void WaitTasks(const std::vector<RUNNABLE_PTR>& tasks, int timeout_ms);

#endif
