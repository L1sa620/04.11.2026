#include "thread_pool.hpp"
#include <cstddef>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

constexpr std::size_t matveev::ThreadPool::default_batch_size;

matveev::ThreadPool::ThreadPool(const unsigned int count, const std::size_t batch_size):
  batch_size_(batch_size)
{
  if (count == 0)
  {
    throw std::invalid_argument("Thread count must be positive");
  }
  if (batch_size == 0)
  {
    throw std::invalid_argument("Batch size must be positive");
  }
  local_queues_.resize(count);
  workers_.reserve(count);
  try
  {
    for (unsigned int i = 0; i < count; ++i)
    {
      workers_.emplace_back(&ThreadPool::execute, this, i);
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
    tasks_.push_back(std::move(task));
    ++unfinished_;
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
        return unfinished_.load() == 0;
      });
  const std::exception_ptr error = first_error_;
  lock.unlock();
  if (error)
  {
    std::rethrow_exception(error);
  }
}

void matveev::ThreadPool::pause()
{
  const std::lock_guard< std::mutex > lock(mutex_);
  paused_ = true;
}

void matveev::ThreadPool::resume()
{
  {
    const std::lock_guard< std::mutex > lock(mutex_);
    paused_ = false;
  }
  ready_.notify_all();
}

matveev::ThreadPool::Task matveev::ThreadPool::takeTask(const std::size_t index)
{
  Queue &local = local_queues_[index];
  if (local.empty())
  {
    if (tasks_.empty())
    {
      return {};
    }
    auto last = tasks_.begin();
    for (std::size_t i = 0; (i < batch_size_) && (last != tasks_.end()); ++i)
    {
      ++last;
    }
    local.splice(local.end(), tasks_, tasks_.begin(), last);
  }
  Task task = std::move(local.front());
  local.pop_front();
  return task;
}

void matveev::ThreadPool::execute(const std::size_t index)
{
  while (true)
  {
    Task task = {};
    {
      std::unique_lock< std::mutex > lock(mutex_);
      while (!task)
      {
        if (stopping_ || !paused_)
        {
          task = takeTask(index);
        }
        if (!task)
        {
          if (stopping_)
          {
            return;
          }
          ready_.wait(lock);
        }
      }
    }
    run(std::move(task));
  }
}

void matveev::ThreadPool::run(Task task)
{
  try
  {
    task();
  }
  catch (...)
  {
    const std::lock_guard< std::mutex > lock(mutex_);
    if (!first_error_)
    {
      first_error_ = std::current_exception();
    }
  }
  task = nullptr;
  if (--unfinished_ == 0)
  {
    const std::lock_guard< std::mutex > lock(mutex_);
    finished_.notify_all();
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
