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

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/chunk_resolver.h"
#include "arrow/chunked_array.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/gather_internal.h"
#include "arrow/util/logging.h"

namespace arrow::internal {

GatherFromChunksState::GatherFromChunksState(const DataType& type,
                                             const ArrayVector& chunks,
                                             bool chunks_have_nulls,
                                             uint8_t* out) noexcept
    : chunks_(chunks),
      chunks_have_nulls_(chunks_have_nulls),
      chunk_values_are_byte_sized_(util::FixedWidthInBytes(type) >= 0),
      out_(out) {
  if (chunk_values_are_byte_sized_) {
    src_residual_bit_offsets_.resize(chunks.size());
  }
  src_chunks_.resize(chunks.size());

  for (size_t i = 0; i < chunks.size(); ++i) {
    const ArraySpan chunk{*chunks[i]->data()};
    DCHECK(chunk.type->Equals(type));
    DCHECK(util::IsFixedWidthLike(chunk));

    auto offset_pointer = util::OffsetPointerOfFixedBitWidthValues(chunk);
    if (chunk_values_are_byte_sized_) {
      src_residual_bit_offsets_[i] = offset_pointer.first;
    } else {
      DCHECK_EQ(offset_pointer.first, 0);
    }
    src_chunks_[i] = offset_pointer.second;
  }
}

GatherFromChunksState::GatherFromChunksState(const ChunkedArray& chunked_array,
                                             uint8_t* out) noexcept
    : GatherFromChunksState(*chunked_array.type(), chunked_array.chunks(),
                            chunked_array.null_count() != 0, out) {}

GatherFromChunksState::~GatherFromChunksState() = default;

}  // namespace arrow::internal
