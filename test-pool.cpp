#include <atomic>
#include <chrono>
#include <cstddef>
#include <exception>
#include <future>
#include <iostream>
#include <stdexcept>
#include <vector>
#include "thread_pool.hpp"

namespace matveev
{
  namespace tests
  {
    void require(bool condition);
    void testWait();
    void testActiveTask();
    void testDrain();
    void testInvalidInput();
    void testTaskFailure();
    void testWorkerWait();
    void testPausedQueue();
    void testActiveOnPause();
    void testPausedDestruction();
    void testPauseCycles();
  }
}

void matveev::tests::require(const bool condition)
{
  if (!condition)
  {
    throw std::runtime_error("Test condition failed");
  }
}

void matveev::tests::testWait()
{
  constexpr unsigned int thread_count = 4;
  constexpr std::size_t task_count = 128;
  constexpr int batch_count = 2;
  std::vector< int > values(task_count, 0);
  ThreadPool pool(thread_count);
  pool.wait();
  for (int batch = 0; batch < batch_count; ++batch)
  {
    for (std::size_t i = 0; i < values.size(); ++i)
    {
      pool.submit(
          [&values, i]()
          {
            ++values[i];
          });
    }
    pool.wait();
  }
  for (const int value : values)
  {
    require(value == batch_count);
  }
}

void matveev::tests::testActiveTask()
{
  std::promise< void > entered = {};
  std::future< void > started = entered.get_future();
  std::promise< void > release = {};
  const std::shared_future< void > signal = release.get_future().share();
  ThreadPool pool(1);
  pool.submit(
      [&entered, signal]()
      {
        entered.set_value();
        signal.wait();
      });
  started.get();
  std::future< void > first = std::async(std::launch::async,
      [&pool]()
      {
        pool.wait();
      });
  std::future< void > second = std::async(std::launch::async,
      [&pool]()
      {
        pool.wait();
      });
  constexpr int check_ms = 30;
  const bool first_blocked = first.wait_for(std::chrono::milliseconds(check_ms)) == std::future_status::timeout;
  const bool second_blocked = second.wait_for(std::chrono::milliseconds(check_ms)) == std::future_status::timeout;
  release.set_value();
  first.get();
  second.get();
  require(first_blocked && second_blocked);
}

void matveev::tests::testDrain()
{
  constexpr unsigned int task_count = 200;
  std::atomic< unsigned int > completed{0};
  {
    ThreadPool pool(1);
    for (unsigned int i = 0; i < task_count; ++i)
    {
      pool.submit(
          [&completed]()
          {
            ++completed;
          });
    }
  }
  require(completed.load() == task_count);
}

void matveev::tests::testInvalidInput()
{
  bool rejected = false;
  try
  {
    const ThreadPool pool(0);
  }
  catch (const std::invalid_argument &)
  {
    rejected = true;
  }
  require(rejected);
  ThreadPool pool(1);
  rejected = false;
  try
  {
    pool.submit({});
  }
  catch (const std::invalid_argument &)
  {
    rejected = true;
  }
  require(rejected);
  pool.wait();
}

void matveev::tests::testTaskFailure()
{
  int completed = 0;
  ThreadPool pool(1);
  pool.submit(
      []()
      {
        throw std::runtime_error("Task failed");
      });
  pool.submit(
      [&completed]()
      {
        ++completed;
      });
  bool reported = false;
  try
  {
    pool.wait();
  }
  catch (const std::runtime_error &)
  {
    reported = true;
  }
  require(reported && (completed == 1));
}

void matveev::tests::testWorkerWait()
{
  ThreadPool pool(1);
  pool.submit(
      [&pool]()
      {
        pool.wait();
      });
  bool rejected = false;
  try
  {
    pool.wait();
  }
  catch (const std::logic_error &)
  {
    rejected = true;
  }
  require(rejected);
}

void matveev::tests::testPausedQueue()
{
  std::promise< void > entered = {};
  std::future< void > started = entered.get_future();
  ThreadPool pool(1);
  pool.pause();
  pool.pause();
  pool.submit(
      [&entered]()
      {
        entered.set_value();
      });
  std::future< void > waiting = std::async(std::launch::async,
      [&pool]()
      {
        pool.wait();
      });
  constexpr int check_ms = 30;
  const bool blocked = started.wait_for(std::chrono::milliseconds(check_ms)) == std::future_status::timeout;
  const bool waiting_blocked = waiting.wait_for(std::chrono::milliseconds(check_ms)) == std::future_status::timeout;
  pool.resume();
  pool.resume();
  waiting.get();
  started.get();
  require(blocked && waiting_blocked);
}

void matveev::tests::testActiveOnPause()
{
  std::promise< void > entered = {};
  std::future< void > started = entered.get_future();
  std::promise< void > release = {};
  const std::shared_future< void > signal = release.get_future().share();
  std::promise< void > active_finished = {};
  std::future< void > finished = active_finished.get_future();
  std::promise< void > queued_started = {};
  std::future< void > queued = queued_started.get_future();
  ThreadPool pool(1);
  pool.submit(
      [&entered, &active_finished, signal]()
      {
        entered.set_value();
        signal.wait();
        active_finished.set_value();
      });
  started.get();
  pool.submit(
      [&queued_started]()
      {
        queued_started.set_value();
      });
  pool.pause();
  release.set_value();
  finished.get();
  constexpr int check_ms = 30;
  const bool blocked = queued.wait_for(std::chrono::milliseconds(check_ms)) == std::future_status::timeout;
  pool.resume();
  pool.wait();
  queued.get();
  require(blocked);
}

void matveev::tests::testPausedDestruction()
{
  constexpr unsigned int task_count = 50;
  std::atomic< unsigned int > completed{0};
  {
    ThreadPool pool(1);
    pool.pause();
    for (unsigned int i = 0; i < task_count; ++i)
    {
      pool.submit(
          [&completed]()
          {
            ++completed;
          });
    }
  }
  require(completed.load() == task_count);
}

void matveev::tests::testPauseCycles()
{
  constexpr unsigned int thread_count = 4;
  constexpr unsigned int cycle_count = 10;
  constexpr unsigned int batch_size = 25;
  std::atomic< unsigned int > completed{0};
  ThreadPool pool(thread_count);
  for (unsigned int cycle = 0; cycle < cycle_count; ++cycle)
  {
    pool.pause();
    for (unsigned int i = 0; i < batch_size; ++i)
    {
      pool.submit(
          [&completed]()
          {
            ++completed;
          });
    }
    pool.resume();
    pool.wait();
    require(completed.load() == (cycle + 1) * batch_size);
  }
}

int main()
{
  try
  {
    matveev::tests::testWait();
    matveev::tests::testActiveTask();
    matveev::tests::testDrain();
    matveev::tests::testInvalidInput();
    matveev::tests::testTaskFailure();
    matveev::tests::testWorkerWait();
    matveev::tests::testPausedQueue();
    matveev::tests::testActiveOnPause();
    matveev::tests::testPausedDestruction();
    matveev::tests::testPauseCycles();
    std::cout << "Tests passed: 10\n";
  }
  catch (const std::exception &error)
  {
    std::cerr << error.what() << "\n";
    return 1;
  }
  return 0;
}
