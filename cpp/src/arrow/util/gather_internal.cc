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

GatherFromChunksBase::GatherFromChunksBase(const ArrayVector& chunks,
                                           bool chunks_have_nulls, MemoryPool* pool,
                                           uint8_t* out) noexcept
    : chunks_(chunks),
      resolver_(chunks),
      pool_(pool),
      chunks_have_nulls_(chunks_have_nulls),
      out_(out) {
  DCHECK(pool);
  src_chunks_.resize(chunks.size());
  for (size_t i = 0; i < chunks.size(); ++i) {
    const ArraySpan chunk{*chunks[i]->data()};
    if (chunk.type->id() == Type::BOOL) {
      src_chunks_[i] = chunk.GetValues<uint8_t>(1, 0);
    } else {
      DCHECK(util::IsFixedWidthLike(chunk, /*force_null_count=*/false,
                                    /*exclude_bool_and_dictionary=*/true));
      src_chunks_[i] = util::OffsetPointerOfFixedByteWidthValues(chunk);
    }
  }
}

GatherFromChunksBase::GatherFromChunksBase(const ChunkedArray& chunked_array,
                                           MemoryPool* pool, uint8_t* out) noexcept
    : GatherFromChunksBase(chunked_array.chunks(), chunked_array.null_count() != 0, pool,
                           out) {}

GatherFromChunksBase::~GatherFromChunksBase() = default;

}  // namespace arrow::internal
