// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cassert>
#include <chrono>
#include <ratio>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "arrow/util/functional.h"
#include "arrow/util/thread_pool.h"
#include "arrow/util/vector.h"

namespace arrow {
namespace internal {

// A parallelizer that takes a `Status(int)` function and calls it with
// arguments between 0 and `num_tasks - 1`, on an arbitrary number of threads.

template <class FUNCTION>
Status ParallelFor(int num_tasks, FUNCTION&& func,
                   Executor* executor = internal::GetCpuThreadPool()) {
  std::vector<Future<>> futures(num_tasks);

  for (int i = 0; i < num_tasks; ++i) {
    ARROW_ASSIGN_OR_RAISE(futures[i], executor->Submit(func, i));
  }
  auto st = Status::OK();
  for (auto& fut : futures) {
    st &= fut.status();
  }
  return st;
}

template <class FUNCTION, typename T,
          typename R = typename internal::call_traits::return_type<FUNCTION>::ValueType>
Future<std::vector<R>> ParallelForAsync(
    std::vector<T> inputs, FUNCTION&& func,
    Executor* executor = internal::GetCpuThreadPool()) {
  std::vector<Future<R>> futures(inputs.size());
  for (size_t i = 0; i < inputs.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(futures[i], executor->Submit(func, i, std::move(inputs[i])));
  }
  return All(std::move(futures))
      .Then([](const std::vector<Result<R>>& results) -> Result<std::vector<R>> {
        return UnwrapOrRaise(results);
      });
}

// A parallelizer that takes a `Status(int)` function and calls it with
// arguments between 0 and `num_tasks - 1`, in sequence or in parallel,
// depending on the input boolean.

template <class FUNCTION>
Status OptionalParallelFor(bool use_threads, int num_tasks, FUNCTION&& func,
                           Executor* executor = internal::GetCpuThreadPool()) {
  if (use_threads) {
    return ParallelFor(num_tasks, std::forward<FUNCTION>(func), executor);
  } else {
    for (int i = 0; i < num_tasks; ++i) {
      RETURN_NOT_OK(func(i));
    }
    return Status::OK();
  }
}

// A parallelizer that takes a `Result<R>(int index, T item)` function and
// calls it with each item from the input array, in sequence or in parallel,
// depending on the input boolean.

template <class FUNCTION, typename T,
          typename R = typename internal::call_traits::return_type<FUNCTION>::ValueType>
Future<std::vector<R>> OptionalParallelForAsync(
    bool use_threads, std::vector<T> inputs, FUNCTION&& func,
    Executor* executor = internal::GetCpuThreadPool()) {
  if (use_threads) {
    return ParallelForAsync(std::move(inputs), std::forward<FUNCTION>(func), executor);
  } else {
    std::vector<R> result(inputs.size());
    for (size_t i = 0; i < inputs.size(); ++i) {
      ARROW_ASSIGN_OR_RAISE(result[i], func(i, inputs[i]));
    }
    return result;
  }
}

/// \brief Like ParallelFor, but tasks are submitted to the Executor in a way that
/// tries to reduce the number of threads used.
///
/// This is useful when the tasks are not necessarily CPU-bound and the overhead
/// of distributing the work to too many cores is significant (e.g. when the tasks
/// depend on shared memory resources that are contended).
///
/// How tasks are executed:
///
/// Every worker function is called with the first task index it should work on. When done
/// with the fist task, it atomically increments a shared counter to get the next task
/// index to work on. This maximizes the amount of work done by each thread.
///
/// Workers are created in a loop that starts with num_workers=1. On each iteration,
/// workers are submitted to the Executor so that num_workers workers are running
/// concurrently. Then the main thread tries to complete one task (the next available).
/// This gives the workers a chance to make progress on one or more tasks. After
/// this task is performed, we estimate the average wall-clock time per task so far and
/// decide if we should grow the number of workers geometrically (capped by the Executor's
/// capacity).
///
/// With the default GrowthRatio=4/3, num_workers grows according to the following
/// progression:
///
///   1, 2, 3, 4, 6, 8, 11, 15, 20, 27, 36, 48, 64, ...
///
/// \param num_tasks The number of func(i) tasks to run
/// \param func A function that takes an integer index and returns a Status
/// \param small_task_duration_secs More workers are created only if tasks are taking
///                                 longer than this duration on average to complete
/// \return Status OK if all tasks succeeded, or the first error encountered
template <class FUNCTION, class GrowthRatio = std::ratio<4, 3>>
Status ParallelForWithBackOff(int num_tasks, FUNCTION&& func,
                              double small_task_duration_secs,
                              Executor* executor = internal::GetCpuThreadPool()) {
  using steady_clock = std::chrono::steady_clock;
  static_assert(GrowthRatio::num > 0 && GrowthRatio::den > 0,
                "GrowthRatio should be positive");
  static_assert(GrowthRatio::num > GrowthRatio::den, "GrowthRatio should be > 1");

  if (num_tasks <= 0) {
    return Status::OK();
  }
  if (num_tasks == 1) {
    return func(0);
  }

  const auto start_time = steady_clock::now();
  const auto max_workers = static_cast<size_t>(executor->GetCapacity());
  size_t num_workers = 1;
  std::vector<Future<>> workers;

  // Keep next_task on its own 64-byte cache line to avoid false sharing with other
  // variables. This is relevant because next_task is updated by all workers.
  alignas(64) std::atomic<int> next_task{0};
  // precondition: first_task < num_tasks
  auto worker_func = [num_tasks, func = std::move<FUNCTION>(func), &next_task](
                         int first_task, bool keep_working) -> Status {
    assert(first_task < num_tasks);
    auto status = func(first_task);
    if (keep_working) {
      while (status.ok()) {
        const int task = next_task.fetch_add(1, std::memory_order_acq_rel);
        if (task >= num_tasks) {
          break;
        }
        status = func(task);
      }
    }
    // If one func failed, just set next_task to num_tasks to stop the loop on
    // all the workers as soon as they finish their current task.
    if (!status.ok()) {
      next_task.store(num_tasks, std::memory_order_release);
    }
    return status;
  };
  // use worker_func(task, false) instead of func(task) after this point

  auto status = Status::OK();
  for (int task = -1;;) {
    // Make sure enough workers are running.
    if (workers.size() < num_workers) {
      workers.reserve(num_workers);
      do {
        task = next_task.fetch_add(1, std::memory_order_acq_rel);
        if (task >= num_tasks) {
          break;
        }
        ARROW_ASSIGN_OR_RAISE(auto fut,
                              executor->Submit(worker_func, task, /*keep_working=*/true));
        workers.push_back(std::move(fut));
      } while (workers.size() < num_workers);
      if (task >= num_tasks) {
        break;
      }
    }
    // Try to run one task on this thread before checking the workers.
    task = next_task.fetch_add(1, std::memory_order_acq_rel);
    if (task >= num_tasks) {
      break;
    }
    status = worker_func(task, /*keep_working=*/false);
    if (!status.ok()) {
      break;
    }
    if (num_workers < max_workers) {
      auto elapsed_time = steady_clock::now() - start_time;
      // Check on the global progress by conservatively estimating
      // how many tasks were completed since the beginning.
      auto num_tasks_completed = static_cast<double>(
          next_task.load(std::memory_order_relaxed) - static_cast<int>(workers.size()));
      // If the average wall-clock time per task is greater than the
      // small_task_duration_secs, grow the number of workers geometrically.
      if (static_cast<double>(elapsed_time.count() * steady_clock::period::num) >
          (small_task_duration_secs * steady_clock::period::den) * num_tasks_completed) {
        num_workers = std::min(
            ((num_workers * GrowthRatio::num - 1) / GrowthRatio::den) + 1, max_workers);
      }
    }
  }

  for (auto& fut : workers) {
    status &= fut.status();
  }
  return status;
}

}  // namespace internal
}  // namespace arrow
