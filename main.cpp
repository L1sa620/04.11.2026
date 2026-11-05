#include <cstddef>
#include <exception>
#include <iostream>
#include <numeric>
#include <vector>
#include "thread_pool.hpp"

int main()
{
  try
  {
    constexpr unsigned int thread_count = 2;
    constexpr std::size_t task_count = 8;
    std::vector< int > values(task_count, 0);
    matveev::ThreadPool pool(thread_count);
    for (std::size_t i = 0; i < values.size(); ++i)
    {
      pool.submit(
          [&values, i]()
          {
            values[i] = 1;
          });
    }
    pool.wait();
    std::size_t completed = 0;
    for (const int value : values)
    {
      completed += value;
    }
    std::cout << "Completed: " << completed << "\n";
    pool.pause();
    for (std::size_t i = 0; i < values.size(); ++i)
    {
      pool.submit(
          [&values, i]()
          {
            ++values[i];
          });
    }
    std::cout << "Before resume: " << std::accumulate(values.begin(), values.end(), 0) << "\n";
    pool.resume();
    pool.wait();
    std::cout << "After resume: " << std::accumulate(values.begin(), values.end(), 0) << "\n";
  }
  catch (const std::exception &error)
  {
    std::cerr << error.what() << "\n";
    return 1;
  }
  return 0;
}
