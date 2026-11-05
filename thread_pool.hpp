#ifndef THREAD_POOL_HPP
#define THREAD_POOL_HPP

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace matveev
{
  class ThreadPool final
  {
  public:
    using Task = std::function< void() >;

    explicit ThreadPool(unsigned int count);
    ~ThreadPool();
    void submit(Task task);
    void wait();
    void pause();
    void resume();

  private:
    std::queue< Task > tasks_ = {};
    std::mutex mutex_ = {};
    std::condition_variable ready_ = {};
    std::condition_variable finished_ = {};
    std::vector< std::thread > workers_ = {};
    std::size_t active_ = 0;
    bool stopping_ = false;
    bool paused_ = false;
    std::exception_ptr first_error_ = nullptr;

    void execute();
    void finish();
  };
}

#endif
