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
/// How tasks are submitted:
///
/// Tasks are submitted in a loop that tries to keep the number of in-flight tasks
/// equal to num_threads. To start, num_threads=2 tasks are submitted to the Executor, and
/// then we wait for one of them to finish for small_task_duration_secs. Then we check how
/// many tasks are still in flight. If it's less than num_threads, we submit more tasks to
/// the Executor to reach num_threads again, but if no task has finished, we grow
/// num_threads geometrically before continuing the submission loop.
///
/// With the default GrowthRatio=4/3, num_threads grows according to the following
/// progression:
///
///   2, 3, 4, 6, 8, 11, 15, 20, 27, 36, 48, 64, ...
///
/// If tasks take much more than small_task_duration_secs to finish, the number
/// of waits is O(log(C)) where C is the capacity of the Executor (usually 4, 8,
/// or 16). Otherwise, the loop guarantees we don't use all threads from the
/// Executor because it learns that all tasks finish quickly and creating more
/// threads is not necessary.
///
/// \param num_tasks The number of func(i) tasks to run
/// \param func A function that takes an integer index and returns a Status
/// \param small_task_duration_secs Time to wait before trying to submit a new
///                                 batch of tasks to the Executor
/// \return Status OK if all tasks succeeded, or the first error encountered
template <class FUNCTION, class GrowthRatio = std::ratio<4, 3>>
Status ParallelForWithBackOff(int num_tasks, FUNCTION&& func,
                              double small_task_duration_secs,
                              Executor* executor = internal::GetCpuThreadPool()) {
  static_assert(GrowthRatio::num > 0 && GrowthRatio::den > 0,
                "GrowthRatio should be positive");
  static_assert(GrowthRatio::num > GrowthRatio::den, "GrowthRatio should be > 1");
  std::vector<Future<>> futures;
  auto status = Status::OK();

  size_t num_threads = 2;
  for (int next_task = 0; next_task < num_tasks;) {
    // Ensure num_threads tasks are in flight. A task is considered in flight
    // if it has been submitted to the Executor and we *don't know yet* if it has
    // finished, but it could have been. This is what makes this approach not
    // as aggressive as work-stealing, but it works well in practice if
    // small_task_duration_secs is small enough.
    while (futures.size() < num_threads) {
      ARROW_ASSIGN_OR_RAISE(auto fut, executor->Submit(func, next_task));
      futures.push_back(std::move(fut));
      next_task += 1;
      if (next_task >= num_tasks) {
        break;
      }
    }
    if (next_task >= num_tasks) {
      break;
    }
    // Back off for a short duration before trying to submit new tasks again.
    // Allowing the assignment of new tasks to existing threads in the pool.
    {
      auto& fut = futures[futures.size() / 2];
      fut.Wait(small_task_duration_secs);
    }
    // Check all the futures without waiting because they had a chance to
    // make progress while we were waiting for one to finish.
    for (auto it = futures.begin(); it != futures.end();) {
      if (it->is_finished()) {
        status &= it->status();  // wait-free because it's finished
        it = futures.erase(it);
      } else {
        ++it;
      }
    }
    // If after the wait, num_threads are still in flight,
    // grow the number of in-flight tasks geometrically.
    if (futures.size() >= num_threads) {
      num_threads = ((num_threads * GrowthRatio::num - 1) / GrowthRatio::den) + 1;
    }
    // If the num_threads reaches the capacity of the executor, just submit
    // all tasks to the Executor and stop backing off between submitting tasks.
    if (ARROW_PREDICT_FALSE(num_threads >=
                            static_cast<size_t>(executor->GetCapacity()))) {
      num_threads = static_cast<size_t>(num_tasks);
    }
  }

  for (auto& fut : futures) {
    status &= fut.status();
  }
  return status;
}

}  // namespace internal
}  // namespace arrow
