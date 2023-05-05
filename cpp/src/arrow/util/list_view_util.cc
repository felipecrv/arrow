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

#include <cstdint>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/array/list_view.h"
#include "arrow/type.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/list_view_util.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"

namespace arrow {
namespace list_view_util {

namespace internal {

namespace {

/// \pre input.length() > 0 && input.null_count() != input.length()
int64_t MinViewOffset(const ArraySpan& input) {
  using offset_type = typename ListViewArray::offset_type;
  // It's very likely that the first non-null non-empty list-view starts at
  // offset 0 of the child array.
  list_view_util::ListViewArrayIterator it(input);
  while (!it.is_end() && (it.IsNull() || it.size() == 0)) {
    ++it;
  }
  if (it.is_end()) {
    return 0;
  }
  auto min_offset = it.offset();
  if (ARROW_PREDICT_TRUE(min_offset == 0)) {
    // Early exit: offset 0 found already.
    return 0;
  }

  // Slow path: scan the buffers entirely.
  const uint8_t* validity = input.MayHaveNulls() ? input.buffers[0].data : NULLPTR;
  const auto* offsets = reinterpret_cast<const offset_type*>(input.buffers[1].data);
  const auto* sizes = reinterpret_cast<const offset_type*>(input.buffers[2].data);
  arrow::internal::VisitSetBitRunsVoid(
      validity, input.offset + it.index() + 1, input.length - it.index() - 1,
      [&](int64_t i, int64_t run_length) {
        for (int64_t j = 0; j < run_length; j++) {
          const auto offset = offsets[input.offset + i + j];
          if (ARROW_PREDICT_FALSE(offset < min_offset)) {
            if (sizes[input.offset + i + j] > 0) {
              min_offset = offset;
            }
          }
        }
      });
  return min_offset;
}

/// \pre input.length() > 0 && input.null_count() != input.length()
int64_t MaxViewEnd(const ArraySpan& input) {
  using offset_type = typename ListViewArray::offset_type;
  // It's very likely that the first non-null non-empty list-view starts at
  // offset zero, so we check that first and potentially early-return a 0.
  const auto begin = list_view_util::ListViewArrayIterator(input);
  auto it = list_view_util::ListViewArrayIterator::end(input);
  --it;  // safe because input.length() > 0
  while (it != begin && (it.IsNull() || it.size() == 0)) {
    --it;
  }
  if (it == begin) {
    return (it.IsNull() || it.size() == 0)
               ? 0
               : static_cast<int64_t>(it.offset()) + it.size();
  }
  int64_t max_end = static_cast<int64_t>(it.offset()) + it.size();
  if (max_end == input.child_data[0].length) {
    // Early-exit: maximum possible view-end found already.
    return max_end;
  }

  // Slow path: scan the buffers entirely.
  const uint8_t* validity = input.MayHaveNulls() ? input.buffers[0].data : NULLPTR;
  const auto* offsets = reinterpret_cast<const offset_type*>(input.buffers[1].data);
  const auto* sizes = reinterpret_cast<const offset_type*>(input.buffers[2].data);
  arrow::internal::VisitSetBitRunsVoid(
      validity, input.offset, /*length=*/it.index() + 1,
      [&](int64_t i, int64_t run_length) {
        for (int64_t j = 0; j < run_length; ++j) {
          const auto offset = offsets[input.offset + i + j];
          const auto size = sizes[input.offset + i + j];
          const int64_t end = static_cast<int64_t>(offset) + size;
          max_end = std::max(max_end, end * (size > 0));
        }
      });
  return max_end;
}

}  // namespace

std::pair<int64_t, int64_t> RangeOfValuesUsed(const ArraySpan& list_view_array) {
  DCHECK(list_view_array.type->id() == Type::LIST_VIEW);
  if (list_view_array.length == 0 ||
      list_view_array.null_count == list_view_array.length) {
    return {0, 0};
  }
  const int64_t min_offset = MinViewOffset(list_view_array);
  const int64_t max_end = MaxViewEnd(list_view_array);
  return {min_offset, max_end - min_offset};
}

}  // namespace internal

}  // namespace list_view_util
}  // namespace arrow
