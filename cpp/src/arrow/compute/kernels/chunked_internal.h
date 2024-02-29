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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array.h"
#include "arrow/chunk_resolver.h"
#include "arrow/compute/kernels/codegen_internal.h"

namespace arrow {
namespace compute {
namespace internal {

// The target chunk in a chunked array.
struct ResolvedChunk {
  // The target array in chunked array.
  const Array* array;
  // The index in the target array.
  const int64_t index;

  ResolvedChunk(const Array* array, int64_t index) : array(array), index(index) {}

 public:
  bool IsNull() const { return array->IsNull(index); }

  template <typename ArrowType, typename ViewType = GetViewType<ArrowType>>
  typename ViewType::T Value() const {
    using LogicalArrayType = typename TypeTraits<ArrowType>::ArrayType;
    auto* typed_array = checked_cast<const LogicalArrayType*>(array);
    return ViewType::LogicalValue(typed_array->GetView(index));
  }
};

template <typename ChunkType = std::shared_ptr<Array>>
class ChunkedArrayResolver {
 private:
  std::vector<const Array*> chunks_;
  ::arrow::internal::ChunkResolver resolver_;

  template <typename A>
  struct ChunkTypeTraitsTemplate {
    using InnerType = std::remove_const_t<std::remove_pointer_t<A>>;
    static const InnerType* ToRawPointer(const ChunkType& chunk) { return &chunk; }
  };
  template <typename A>
  struct ChunkTypeTraitsTemplate<std::shared_ptr<A>> {
    using InnerType = std::remove_const_t<A>;
    static const InnerType* ToRawPointer(const ChunkType& chunk) { return chunk.get(); }
  };

  using ChunkTypeTraits = ChunkTypeTraitsTemplate<std::remove_const_t<ChunkType>>;

 public:
  /// \brief The type of the array chunks without const and pointer (smart or raw).
  using ChunkInnerType = typename ChunkTypeTraits::InnerType;

 private:
  explicit ChunkedArrayResolver(std::vector<const Array*> chunks)
      : chunks_(std::move(chunks)), resolver_(chunks_) {}

  // turn any std::vector<ChunkType> into std::vector<const Array*>
  static std::vector<const Array*> MakeArrayPointers(
      const std::vector<ChunkType>& arrays) {
    std::vector<const Array*> pointers(arrays.size());
    std::transform(arrays.begin(), arrays.end(), pointers.begin(),
                   ChunkTypeTraits::ToRawPointer);
    return pointers;
  }

 public:
  explicit ChunkedArrayResolver(const std::vector<ChunkType>& chunks)
      : chunks_(MakeArrayPointers(chunks)), resolver_(chunks_) {}

  ChunkedArrayResolver(ChunkedArrayResolver&& other) = default;
  ChunkedArrayResolver& operator=(ChunkedArrayResolver&& other) = default;

  ChunkedArrayResolver(const ChunkedArrayResolver& other) = default;
  ChunkedArrayResolver& operator=(const ChunkedArrayResolver& other) = default;

  ResolvedChunk Resolve(int64_t index) const {
    const auto loc = resolver_.Resolve(index);
    return {chunks_[loc.chunk_index], loc.index_in_chunk};
  }
};

}  // namespace internal
}  // namespace compute
}  // namespace arrow
