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
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/type_fwd.h"
#include "arrow/util/bit_util.h"

namespace arrow {
namespace list_view_util {

/// \brief Get the child array holding the values from an ListView array
inline const ArraySpan& ValuesArray(const ArraySpan& span) { return span.child_data[0]; }

namespace internal {

/// \brief Calculate the smallest continuous range of values used by the input
///
/// List-view offsets are not necessarily sorted, so a full scan is required
/// to determine the used range.
///
/// \param list_view_array The array of type LIST_VIEW
/// \return A pair of (offset, length) describing the range
std::pair<int64_t, int64_t> RangeOfValuesUsed(const ArraySpan& list_view_array);

}  // namespace internal

class ListViewArrayIterator {
 public:
  const ArraySpan& array_span;

 private:
  int64_t i_;
  uint8_t* validity_;
  const ListViewType::offset_type* offsets_;
  const ListViewType::offset_type* sizes_;

  explicit ListViewArrayIterator(const ArraySpan& array_span, int64_t i)
      : array_span{array_span},
        i_(i),
        validity_(array_span.MayHaveNulls() ? array_span.buffers[0].data : nullptr),
        offsets_(reinterpret_cast<const ListViewType::offset_type*>(
            array_span.buffers[1].data)),
        sizes_(reinterpret_cast<const ListViewType::offset_type*>(
            array_span.buffers[2].data)) {
    assert(array_span.type->id() == Type::LIST_VIEW);
  }

 public:
  explicit ListViewArrayIterator(const ArraySpan& array_span)
      : ListViewArrayIterator(array_span, 0) {}

  static ListViewArrayIterator end(const ArraySpan& array_span) {
    return ListViewArrayIterator(array_span, array_span.length);
  }

  bool is_end() const { return i_ >= array_span.length; }

  bool IsValid() const {
    return !validity_ || bit_util::GetBit(validity_, array_span.offset + i_);
  }

  bool IsNull() const { return !IsValid(); }

  int32_t offset() const { return offsets_[array_span.offset + i_]; }
  int32_t size() const { return sizes_[array_span.offset + i_]; }
  int64_t index() const { return i_; }

  ListViewArrayIterator& operator++() {
    ++i_;
    return *this;
  }

  ListViewArrayIterator operator++(int) {
    auto copy = *this;
    ++*this;
    return copy;
  }

  ListViewArrayIterator& operator--() {
    --i_;
    return *this;
  }

  ListViewArrayIterator operator--(int) {
    auto copy = *this;
    --*this;
    return copy;
  }

  ListViewArrayIterator& operator+=(int64_t n) {
    i_ += n;
    return *this;
  }

  ListViewArrayIterator& operator-=(int64_t n) {
    i_ -= n;
    return *this;
  }

  bool operator==(const ListViewArrayIterator& other) const { return i_ == other.i_; }
  bool operator!=(const ListViewArrayIterator& other) const { return i_ != other.i_; }
};

}  // namespace list_view_util
}  // namespace arrow
