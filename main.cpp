#include <cstddef>
#include <exception>
#include <iostream>
#include <numeric>
#include <vector>
#include "thread_pool.hpp"

namespace matveev
{
  static unsigned int countPrimesBelow(unsigned int limit);
}

unsigned int matveev::countPrimesBelow(const unsigned int limit)
{
  constexpr unsigned int first_prime = 2;
  unsigned int count = 0;
  for (unsigned int number = first_prime; number < limit; ++number)
  {
    bool prime = true;
    for (unsigned int divisor = first_prime; divisor <= number / divisor; ++divisor)
    {
      if ((number % divisor) == 0)
      {
        prime = false;
        break;
      }
    }
    if (prime)
    {
      ++count;
    }
  }
  return count;
}

int main()
{
  try
  {
    constexpr unsigned int thread_count = 2;
    constexpr std::size_t batch_size = 4;
    constexpr unsigned int heavy = 100000;
    constexpr unsigned int light = 100;
    const std::vector< unsigned int > limits = {heavy, heavy, heavy, heavy, light, light, light, light, 0};
    std::vector< unsigned int > counts(limits.size(), 0);
    matveev::ThreadPool pool(thread_count, batch_size);
    pool.pause();
    for (std::size_t i = 0; i < limits.size(); ++i)
    {
      pool.submit(
          [&limits, &counts, i]()
          {
            counts[i] = matveev::countPrimesBelow(limits[i]);
          });
    }
    pool.resume();
    pool.wait();
    for (std::size_t i = 0; i < limits.size(); ++i)
    {
      std::cout << "Primes below " << limits[i] << ": " << counts[i] << "\n";
    }
    std::cout << "Total: " << std::accumulate(counts.begin(), counts.end(), 0U) << "\n";
  }
  catch (const std::exception &error)
  {
    std::cerr << error.what() << "\n";
    return 1;
  }
  return 0;
}
