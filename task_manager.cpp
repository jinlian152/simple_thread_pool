#include "task_manager.h"
namespace map {

int Worker::sleep_count_ = 0;

std::shared_ptr<ThreadManager> ThreadManager::instance_ = std::make_shared<ThreadManager>();
std::mutex ThreadManager::instance_mutex_;

const int g_default_worker_count = 30;

// constexpr std::chrono::duration<long double, std::milli> operator ""ms(long double ms) {
//     return std::chrono::duration<long double, std::milli>(ms);
// }

void Worker::run() {
    if (nullptr != before_start_) {
        before_start_();
    }

    std::chrono::duration<int, std::milli> timeout_ms(100);
    std::unique_lock<std::mutex> lock(*task_mutex_);
    while (!need_stop_) {
        ++Worker::sleep_count_;
        while (tasks_->empty()
        && !worker_cv_->wait_for(lock,
                std::chrono::milliseconds(100),
                [&] { return !tasks_->empty();})) {
            if (need_stop_) {
                break;
            }
            continue;
        }

        --Worker::sleep_count_;
        if (tasks_->empty()) {
            continue;
        }

        std::shared_ptr<Runnable> task = tasks_->front();
        if (nullptr == task) {
            continue;
        }
        tasks_->pop_front();
        task_mutex_->unlock();
        task->run();
        task_mutex_->lock();
    }

    if (nullptr != before_end_) {
        before_end_();
    }
}

void ThreadManager::init(int worker_count,
                         WorkerCallback berfor_start,
                         WorkerCallback before_end) {
    task_mutex_ = std::make_shared<std::mutex>();
    worker_cv_ = std::make_shared<std::condition_variable>();
    tasks_ = std::make_shared<std::deque< std::shared_ptr<Runnable> > >();

    worker_count_ = worker_count;

    for (int i = 0; i != worker_count; ++i) {
        auto worker = std::make_shared<Worker>(this, worker_cv_, task_mutex_, tasks_);
        worker->set_start_callback(berfor_start);
        worker->set_end_callback(before_end);
        workers_.push_back(worker);
    }
}

void ThreadManager::start () {
    for (auto& iter : workers_) {
        iter->start();
    }
}

void ThreadManager::stop() {
    for (auto& iter : workers_) {
        iter->stop();
    }
    
    worker_cv_->notify_all();

    for (auto& iter : workers_) {
        iter->join();
    }
}

void ThreadManager::add_task(std::shared_ptr<Runnable> value) {
    std::unique_lock<std::mutex> lock(*task_mutex_);
    tasks_->push_back(value);
    if (Worker::sleep_count_ > 0) {
        worker_cv_->notify_one();
    }
}

std::shared_ptr<ThreadManager> ThreadManager::get_instance() {
    if (NULL == ThreadManager::instance_) {
        std::unique_lock<std::mutex>(instance_mutex_);
        if (NULL == ThreadManager::instance_) {
            instance_ = std::make_shared<ThreadManager>();
        }
    }
    return instance_;
}

void ParallelTasksRunner::AddTasks(const std::vector<RUNNABLE_PTR>& tasks) {
    if (0 >= tasks.size()) {
        return ;
    }

    task_count_ = std::make_shared<int>(tasks.size());
    cond_ = std::make_shared<std::condition_variable>();
    mutex_ = std::make_shared<std::mutex>();
    auto thread_mgr = ThreadManager::get_instance();
    for (auto& iter : tasks) {
        std::shared_ptr<ParallelTask> task =
            std::make_shared<ParallelTask>(iter, task_count_, cond_, mutex_);
        auto thread_mgr = ThreadManager::get_instance();
        thread_mgr->add_task(task);
    }
}

void ParallelTasksRunner::Wait(int timeout_ms) {
    if(task_count_.get() != nullptr) {
        std::unique_lock<std::mutex> lock(*mutex_.get());
        if(*task_count_ == 0) {
            return;
        }
        if(timeout_ms < 0) {
            cond_->wait(lock);
        } else {
            auto now = std::chrono::system_clock::now();
            auto expire_ms = std::chrono::milliseconds(timeout_ms);
            cond_->wait_until(lock, now + expire_ms);
        }
    }
}

void WaitTasks(const std::vector<RUNNABLE_PTR>& tasks, int timeout_ms) {
    ParallelTasksRunner async_tasks;
    async_tasks.AddTasks(tasks);
    async_tasks.Wait(timeout_ms);
}

}
