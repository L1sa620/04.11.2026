#include "thread_pool.hpp"
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

matveev::ThreadPool::ThreadPool(const unsigned int count)
{
  if (count == 0)
  {
    throw std::invalid_argument("Thread count must be positive");
  }
  workers_.reserve(count);
  try
  {
    for (unsigned int i = 0; i < count; ++i)
    {
      workers_.emplace_back(&ThreadPool::execute, this);
    }
  }
  catch (...)
  {
    finish();
    throw;
  }
}

matveev::ThreadPool::~ThreadPool()
{
  finish();
}

void matveev::ThreadPool::submit(Task task)
{
  if (!task)
  {
    throw std::invalid_argument("Task must not be empty");
  }
  {
    const std::lock_guard< std::mutex > lock(mutex_);
    if (stopping_)
    {
      throw std::logic_error("Thread pool is stopping");
    }
    tasks_.push(std::move(task));
  }
  ready_.notify_one();
}

void matveev::ThreadPool::wait()
{
  for (const std::thread &worker : workers_)
  {
    if (worker.get_id() == std::this_thread::get_id())
    {
      throw std::logic_error("A worker cannot wait for its own pool");
    }
  }
  std::unique_lock< std::mutex > lock(mutex_);
  finished_.wait(lock,
      [this]()
      {
        return tasks_.empty() && (active_ == 0);
      });
  const std::exception_ptr error = first_error_;
  lock.unlock();
  if (error)
  {
    std::rethrow_exception(error);
  }
}

void matveev::ThreadPool::execute()
{
  while (true)
  {
    Task task = {};
    {
      std::unique_lock< std::mutex > lock(mutex_);
      ready_.wait(lock,
          [this]()
          {
            return stopping_ || !tasks_.empty();
          });
      if (tasks_.empty())
      {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
      ++active_;
    }
    std::exception_ptr error = nullptr;
    try
    {
      task();
    }
    catch (...)
    {
      error = std::current_exception();
    }
    task = nullptr;
    {
      const std::lock_guard< std::mutex > lock(mutex_);
      if (error && !first_error_)
      {
        first_error_ = error;
      }
      --active_;
      if (tasks_.empty() && (active_ == 0))
      {
        finished_.notify_all();
      }
    }
  }
}

void matveev::ThreadPool::finish()
{
  {
    const std::lock_guard< std::mutex > lock(mutex_);
    stopping_ = true;
  }
  ready_.notify_all();
  for (std::thread &worker : workers_)
  {
    worker.join();
  }
}
