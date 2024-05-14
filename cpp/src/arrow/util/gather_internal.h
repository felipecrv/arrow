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
#include <cstddef>
#include <cstdint>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/chunk_resolver.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/macros.h"

// Implementation helpers for kernels that need to load/gather fixed-width
// data from multiple, arbitrary indices.
//
// https://en.wikipedia.org/wiki/Gather/scatter_(vector_addressing)

namespace arrow::internal {
inline namespace gather_internal {
// class ValiditySpan {
//   const ArraySpan& array_;

//  public:
//   explicit ValiditySpan(const ArraySpan& array) : array_(array) {}

//   bool MayHaveNulls() const { return array_.MayHaveNulls(); }

//   /// \pre MayHaveNulls() returns true
//   ARROW_FORCE_INLINE bool IsValid(int64_t position) const {
//     ARROW_COMPILER_ASSUME(array_.buffers[0].data != nullptr);
//     return array_.IsValid(position);
//   }
// };

// CRTP [1] base class for Gather that provides a gathering loop in terms of
// Write*() methods that must be implemented by the derived class.
//
// [1] https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
template <class GatherImpl>
class GatherBaseCRTP {
 public:
  // Output offset is not supported by Gather and idx is supposed to have offset
  // pre-applied. idx_validity parameters on functions can use the offset they
  // carry to read the validity bitmap as bitmaps can't have pre-applied offsets
  // (they might not align to byte boundaries).
  GatherBaseCRTP() = default;
  GatherBaseCRTP(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP(GatherBaseCRTP&&) = delete;
  GatherBaseCRTP& operator=(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP& operator=(GatherBaseCRTP&&) = delete;

 protected:
  template <typename IndexCType>
  bool IsSrcValid(const ArraySpan& src_validity, const IndexCType* idx,
                  int64_t position) const {
    ARROW_COMPILER_ASSUME(src_validity.buffers[0].data != nullptr);
    return src_validity.IsValid(idx[position]);
  }

  ARROW_FORCE_INLINE int64_t ExecuteNoNulls(int64_t idx_length) {
    auto* self = static_cast<GatherImpl*>(this);
    for (int64_t position = 0; position < idx_length; position++) {
      self->WriteValue(position);
    }
    return idx_length;
  }

  // See derived Gather classes below for the meaning of the parameters, pre and
  // post-conditions.
  template <bool kOutputIsZeroInitialized, typename IndexCType,
            class ValiditySpan = ArraySpan>
  ARROW_FORCE_INLINE int64_t ExecuteWithNulls(const ValiditySpan& src_validity,
                                              int64_t idx_length, const IndexCType* idx,
                                              const ArraySpan& idx_validity,
                                              uint8_t* out_is_valid) {
    auto* self = static_cast<GatherImpl*>(this);
    OptionalBitBlockCounter indices_bit_counter(idx_validity.buffers[0].data,
                                                idx_validity.offset, idx_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < idx_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (!src_validity.MayHaveNulls()) {
        // Source values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither source values nor index nulls
          bit_util::SetBitsTo(out_is_valid, position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            self->WriteValue(position);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(idx_validity.buffers[0].data != nullptr);
            if (idx_validity.IsValid(position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, position);
              self->WriteValue(position);
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          self->WriteZeroSegment(position, block.length);
          position += block.length;
        }
      } else {
        // Source values may be null, so we must do random access with IsSrcValid()
        if (block.popcount == block.length) {
          // Faster path: indices are not null but source values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (self->IsSrcValid(src_validity, idx, position)) {
              // value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(idx_validity.buffers[0].data != nullptr);
            if (idx_validity.IsValid(position) &&
                self->IsSrcValid(src_validity, idx, position)) {
              // index is not null && value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          if constexpr (!kOutputIsZeroInitialized) {
            self->WriteZeroSegment(position, block.length);
          }
          position += block.length;
        }
      }
    }
    return valid_count;
  }
};

template <int kValueWidthInBits, typename IndexCType, bool kWithFactor>
class Gather : public GatherBaseCRTP<Gather<kValueWidthInBits, IndexCType, kWithFactor>> {
 public:
  static_assert(kValueWidthInBits >= 0 && kValueWidthInBits % 8 == 0);
  static constexpr int kValueWidth = kValueWidthInBits / 8;

 private:
  const int64_t src_length_;  // number of elements of kValueWidth bytes in src_
  const uint8_t* src_;
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;
  int64_t factor_;

 public:
  void WriteValue(int64_t position) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memcpy(out_ + position * scaled_factor, src_ + idx_[position] * scaled_factor,
             scaled_factor);
    } else {
      memcpy(out_ + position * kValueWidth, src_ + idx_[position] * kValueWidth,
             kValueWidth);
    }
  }

  void WriteZero(int64_t position) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, kValueWidth);
    }
  }

  void WriteZeroSegment(int64_t position, int64_t length) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, length * scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, length * kValueWidth);
    }
  }

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t zero_src_offset,
         int64_t idx_length, const IndexCType* idx, uint8_t* out, int64_t factor)
      : src_length_(src_length),
        src_(src),
        idx_length_(idx_length),
        idx_(idx),
        out_(out),
        factor_(factor) {
    assert(zero_src_offset == 0);
    assert(src && idx && out);
    assert((kWithFactor || factor == 1) &&
           "When kWithFactor is false, the factor is assumed to be 1 at compile time");
  }

  ARROW_FORCE_INLINE int64_t Execute() { return this->ExecuteNoNulls(idx_length_); }

  /// \pre If kOutputIsZeroInitialized, then this->out_ has to be zero initialized.
  /// \pre Bits in out_is_valid have to always be zero initialized.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \post If !kOutputIsZeroInitialized, then positions in this->_out containing null
  ///       elements have 0s written to them. This might be less efficient than
  ///       zero-initializing first and calling this->Execute() afterwards.
  /// \return The number of valid elements in out.
  template <bool kOutputIsZeroInitialized = false>
  ARROW_FORCE_INLINE int64_t Execute(const ArraySpan& src_validity,
                                     const ArraySpan& idx_validity,
                                     uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized>(
        src_validity, idx_length_, idx_, idx_validity, out_is_valid);
  }
};

template <typename IndexCType>
class Gather</*kValueWidthInBits=*/1, IndexCType, /*kWithFactor=*/false>
    : public GatherBaseCRTP<Gather<1, IndexCType, false>> {
 private:
  const int64_t src_length_;  // number of elements of bits bytes in src_ after offset
  const uint8_t* src_;        // the boolean array data buffer in bits
  const int64_t src_offset_;  // offset in bits
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;  // output boolean array data buffer in bits

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t src_offset, int64_t idx_length,
         const IndexCType* idx, uint8_t* out, int64_t factor)
      : src_length_(src_length),
        src_(src),
        src_offset_(src_offset),
        idx_length_(idx_length),
        idx_(idx),
        out_(out) {
    assert(src && idx && out);
    assert(factor == 1 &&
           "factor != 1 is not supported when Gather is used to gather bits/booleans");
  }

  void WriteValue(int64_t position) {
    bit_util::SetBitTo(out_, position,
                       bit_util::GetBit(src_, src_offset_ + idx_[position]));
  }

  void WriteZero(int64_t position) { bit_util::ClearBit(out_, position); }

  void WriteZeroSegment(int64_t position, int64_t block_length) {
    bit_util::SetBitsTo(out_, position, block_length, false);
  }

  ARROW_FORCE_INLINE int64_t Execute() { return this->ExecuteNoNulls(idx_length_); }

  /// \pre If kOutputIsZeroInitialized, then this->out_ has to be zero initialized.
  /// \pre Bits in out_is_valid have to always be zero initialized.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \post If !kOutputIsZeroInitialized, then positions in this->_out containing null
  ///       elements have 0s written to them. This might be less efficient than
  ///       zero-initializing first and calling this->Execute() afterwards.
  /// \return The number of valid elements in out.
  template <bool kOutputIsZeroInitialized = false>
  ARROW_FORCE_INLINE int64_t Execute(const ArraySpan& src_validity,
                                     const ArraySpan& idx_validity,
                                     uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length && src_offset_ == src_validity.offset);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized>(
        src_validity, idx_length_, idx_, idx_validity, out_is_valid);
  }
};
}  // namespace gather_internal

class GatherFromChunksState {
 private:
  const ArrayVector& chunks_;
  const bool chunks_have_nulls_;
  const bool chunk_values_are_byte_sized_;

  // If chunk_values_are_byte_sized_ is false, src_residual_bit_offsets_[i] is used
  // to store the bit offset of the first byte (0-7) in src_chunks_[i].
  //
  // NOTE: If chunk_values_are_byte_sized_ is true, src_residual_bit_offsets_ is empty.
  std::vector<int> src_residual_bit_offsets_;
  // Pre-computed pointers to the start of the values in each chunk.
  std::vector<const uint8_t*> src_chunks_;

  uint8_t* out_;

  /// \pre out is zero-initialized
  GatherFromChunksState(const DataType& type, const ArrayVector& chunks,
                        bool chunks_have_nulls, uint8_t* out) noexcept;

 public:
  /// \pre out is zero-initialized
  GatherFromChunksState(const ChunkedArray& chunked_array, uint8_t* out) noexcept;
  ~GatherFromChunksState();

  bool MayHaveNulls() const { return chunks_have_nulls_; }

  template <typename IndexCType>
  Status InitWithIndices(int64_t idx_length, const IndexCType* idx, MemoryPool* pool) {
    // Allocate vector of chunk indices for each logical index and resolve them.
    // All indices are resolved in one go without checking the validity bitmap.
    // This is OK as long the output corresponding to the invalid indices is not used.
    ARROW_ASSIGN_OR_RAISE(chunk_index_vec_buffer_,
                          AllocateBuffer(idx_length * sizeof(IndexCType), pool));
    ARROW_ASSIGN_OR_RAISE(index_in_chunk_vec_buffer_,
                          AllocateBuffer(idx_length * sizeof(IndexCType), pool));

    auto* chunk_index_vec =
        chunk_index_vec_buffer_->template mutable_data_as<IndexCType>();
    auto* index_in_chunk_vec =
        index_in_chunk_vec_buffer_->template mutable_data_as<IndexCType>();

    ChunkResolver resolver(chunks_);
    bool enough_precision = resolver.ResolveMany<IndexCType>(
        /*n=*/idx_length, /*logical_index_vec=*/idx, chunk_index_vec,
        /*chunk_hint=*/static_cast<IndexCType>(0), index_in_chunk_vec);
    if (ARROW_PREDICT_FALSE(!enough_precision)) {
      return Status::IndexError("IndexCType is too small");
    }
    return Status::OK();
  }

  template <typename IndexCType>
  bool IsSrcValid(const ArraySpan& src_validity, const IndexCType* idx,
                  int64_t position) const {}
};

template <int kValueWidthInBits, typename IndexCType, bool WithFactor>
class GatherFromChunks
    : public GatherBaseCRTP<GatherFromChunks<kValueWidthInBits, IndexCType, WithFactor>> {
 private:
  GatherFromChunksState state_;
  MemoryPool* pool_;

 public:
  /// \pre out is zero-initialized
  GatherFromChunks(const ChunkedArray& chunked_array, MemoryPool* pool,
                   uint8_t* out) noexcept
      : state_(chunked_array, out), pool_(pool) {}

  Result<int64_t> Execute(const ArraySpan& indices, uint8_t* out_is_valid,
                          int64_t factor) {
    static_assert(kValueWidthInBits % 8 == 0 || kValueWidthInBits == 1,
                  "kValueWidthInBits must be 1 or a multiple of 8");
    static_assert(!WithFactor || kValueWidthInBits == 8,
                  "WithFactor is only supported for kValueWidthInBits == 8");
    assert(indices.type->byte_width() == sizeof(IndexCType));
    assert(!(chunks_have_nulls_ || indices.MayHaveNulls()) || out_is_valid != NULLPTR);

    const auto idx_length = indices.length;
    const auto* idx = indices.GetValues<IndexCType>(1);

    ChunkResolver resolver(state_.chunks_);
    ARROW_ASSIGN_OR_RAISE(auto chunk_location_vec,
                          resolver.ResolveMany<IndexCType>(idx_length, idx, pool_));

    RETURN_NOT_OK(state_.InitWithIndices(idx_length, idx, pool_));
    return ExecuteImpl(indices, chunk_index_vec, index_in_chunk_vec, out_is_valid,
                       factor);
  }

 protected:
  /// \pre All the valid indices have been bounds-checked
  /// \pre indices.MayHaveNulls() implies out_is_valid != nullptr
  /// \pre Bits in out_is_valid, if provided, have to be zero initialized.
  ///
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \return The number of valid elements in out.
  Result<int64_t> ExecuteImpl(const ArraySpan& indices, const IndexCType* chunk_index_vec,
                              const IndexCType* index_in_chunk_vec, uint8_t* out_is_valid,
                              size_t factor) {
    constexpr int64_t kValueWidth = kValueWidthInBits / 8;
    const auto idx_length = indices.length;

    // chunk_index/index_in_chunk is the resolved location of the index at [position].
    auto WriteValueFromChunk = [&](int64_t position, auto chunk_index,
                                   auto index_in_chunk) {
      auto* src = src_chunks_[chunk_index];
      if constexpr (kValueWidthInBits == 1) {
        // The source values are bits, so we have to
        // apply the internal chunk offset to src.
        const int64_t src_offset = chunks_[chunk_index]->offset();
        bit_util::SetBitTo(out_, position,
                           bit_util::GetBit(src, src_offset + index_in_chunk));
      } else {
        if constexpr (WithFactor) {
          memcpy(out_ + position * factor, src + index_in_chunk * factor, factor);
        } else {
          memcpy(out_ + position * kValueWidth, src + index_in_chunk * kValueWidth,
                 kValueWidth);
        }
      }
    };

    auto WriteValue = [&](int64_t position) {
      auto chunk_index = chunk_index_vec[position];
      auto index_in_chunk = index_in_chunk_vec[position];
      WriteValueFromChunk(position, chunk_index, index_in_chunk);
    };

    // Both out_if_valid and this->out_ are zero-initialized, so we can
    // completely skip processing the nulls in the indices.
    OptionalBitBlockCounter indices_bit_counter(indices.buffers[0].data, indices.offset,
                                                idx_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < idx_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (!chunks_have_nulls_) {
        // Source values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither source values nor index nulls
          if (out_is_valid) {
            bit_util::SetBitsTo(out_is_valid, position, block.length, true);
          }
          for (int64_t i = 0; i < block.length; ++i) {
            WriteValue(position);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(indices.buffers[0].data != nullptr);
            if (indices.IsValid(position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, position);
              WriteValue(position);
            }
            ++position;
          }
        } else {
          position += block.length;
        }
      } else {
        // Source values may be null, so we must do random access into src_validity
        if (block.popcount == block.length) {
          // Faster path: indices are not null but source values may be
          for (int64_t i = 0; i < block.length; ++i) {
            auto chunk_index = chunk_index_vec[position];
            auto index_in_chunk = index_in_chunk_vec[position];
            if (chunks_[chunk_index]->IsValid(index_in_chunk)) {
              // value is not null
              WriteValueFromChunk(position, chunk_index, index_in_chunk);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            auto chunk_index = chunk_index_vec[position];
            auto index_in_chunk = index_in_chunk_vec[position];
            ARROW_COMPILER_ASSUME(indices.buffers[0].data != nullptr);
            if (indices.IsValid(position) &&
                chunks_[chunk_index]->IsValid(index_in_chunk)) {
              // index is not null && value is not null
              WriteValueFromChunk(position, chunk_index, index_in_chunk);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            }
            ++position;
          }
        } else {
          position += block.length;
        }
      }
    }

    return valid_count;
  }
};

}  // namespace arrow::internal
