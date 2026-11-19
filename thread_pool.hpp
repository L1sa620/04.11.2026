#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <list>
#include <mutex>
#include <thread>
#include <vector>

namespace matveev
{
  class ThreadPool final
  {
  public:
    using Task = std::function< void() >;
    static constexpr std::size_t default_batch_size = 4;

    explicit ThreadPool(unsigned int count, std::size_t batch_size = default_batch_size);
    ~ThreadPool();
    void submit(Task task);
    void wait();
    void pause();
    void resume();

  private:
    using Queue = std::list< Task >;

    const std::size_t batch_size_;
    Queue tasks_ = {};
    std::deque< Queue > local_queues_ = {};
    std::mutex mutex_ = {};
    std::condition_variable ready_ = {};
    std::condition_variable finished_ = {};
    std::vector< std::thread > workers_ = {};
    std::atomic< std::size_t > unfinished_{0};
    bool stopping_ = false;
    bool paused_ = false;
    std::exception_ptr first_error_ = nullptr;

    Task takeTask(std::size_t index);
    Task stealTask(std::size_t index);
    void execute(std::size_t index);
    void run(Task task);
    void finish();
  };
}

#endif
