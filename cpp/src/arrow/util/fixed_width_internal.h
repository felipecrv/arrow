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

#include <cstdint>

#include "arrow/array/data.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"

// Fixed-width types are the ones defined by the is_fixed_width() predicate in
// type_traits.h. They are all the types that passes any of the following
// predicates:
//
//  - is_primitive()
//  - is_fixed_size_binary()
//  - is_dictionary()
//
// At least 3 types in this set require special care:
//  - `Type::BOOL` is fixed-width, but it's a 1-bit type and pointers to first bit
//    in boolean buffers are not always aligned to byte boundaries.
//  - `Type::DICIONARY` is fixed-width because the indices are fixed-width, but the
//    dictionary values are not necessarily fixed-width and have to be managed
//    by separate operations.
//  - Type::FIXED_SIZE_BINARY unlike other fixed-width types, fixed-size binary
//    values are defined by a size attribute that is not known at compile time.
//    The other types have power-of-2 byte widths, while fixed-size can have any
//    byte width including 0.
//
// Additionally, we say that a type is "fixed-width modulo nesting" [1] if it's a
// fixed-width as defined above, or if it's a fixed-size list (or nested fixed-size lists)
// and the innermost type is fixed-width and the following restrictions also apply:
//  - The value type of the innermost fixed-size list is not BOOL (it has to be excluded
//    because a 1-bit type doesn't byte-align)
//  - Only the top-level array may have nulls, all the inner array have to be completely
//    free of nulls so we don't need to manage internal validity bitmaps.
//
// Take the following `fixed_size_list<fixed_size_list<int32, 2>, 3>` array as an example:
//
//     [
//       [[1, 2], [3,  4], [ 5,  6]],
//       null,
//       [[7, 8], [9, 10], [11, 12]]
//     ]
//
// in memory, it would look like:
//
//     {
//        type: fixed_size_list<fixed_size_list<int32, 2>, 3>,
//        length: 3,
//        null_count: 1,
//        offset: 0,
//        buffers: [
//          0: [0b00000101]
//        ],
//        child_data: [
//          0: {
//            type: fixed_size_list<int32, 2>,
//            length: 9,
//            null_count: 0,
//            offset: 0,
//            buffers: [0: NULL],
//            child_data: [
//              0: {
//                type: int32,
//                length: 18,
//                null_count: 0,
//                offset: 0,
//                buffers: [
//                  0: NULL,
//                  1: [ 1,  2,  3,  4,  5,  6,
//                       0,  0,  0,  0,  0,  0
//                       7,  8,  9, 10, 11, 12 ]
//                ],
//                child_data: []
//              }
//            ]
//          }
//        ]
//     }
//
// This layout fits the fixed-width modulo nesting definition because the innermost
// type is byte-aligned fixed-width (int32 = 4 bytes) and the internal arrays don't
// have nulls. The validity bitmap is only needed at the top-level array.
//
// Writing to this array can be done in the same way writing to a flat fixed-width
// array is done, by:
// 1. Updating the validity bitmap at the top-level array if nulls are present.
// 2. Updating a continuous fixed-size block of memory through a single pointer.
//
// The length of this block of memory is the product of the list sizes in the
// `FixedSizeList` types and the byte width of the innermost fixed-width type:
//
//     3 * 2 * 4 = 24 bytes
//
// Writing the `[[1, 2], [3, 4], [5, 6]]` value at a given index can be done by
// simply setting the validity bit to 1 and writing the 24-byte sequence of
// integers `[1, 2, 3, 4, 5, 6]` to the memory block at `byte_ptr + index * 24`.
//
// The length of the top-level array fully defines the lengths that all the nested
// arrays must have, which makes defining all the lengths as easy as defining the
// length of the top-level array.
//
//     length = 3
//     child_data[0].length == 3 * 3 == 9
//     child_data[0].child_data[0].length == 3 * 3 * 2 == 18
//
//     child_data[0].child_data[0].buffers[1].size() >=
//       (3 * (3 * 2 * sizeof(int32)) == 3 * 24 == 72)
//
// Dealing with offsets is a bit involved. Let's say the array described above has
// the offsets 2, 5, and 7:
//
//     {
//        type: fixed_size_list<fixed_size_list<int32, 2>, 3>,
//        offset: 2,
//        ...
//        child_data: [
//          0: {
//            type: fixed_size_list<int32, 2>,
//            offset: 5,
//            ...
//            child_data: [
//              0: {
//                type: int32,
//                offset: 7,
//                buffers: [
//                  0: NULL,
//                  1: [ 1, 1, 1, 1, 1, 1, 1,      // 7 values skipped
//                       0,1, 0,1, 0,1, 0,1, 0,1,  // 5 [x,x] values skipped
//
//                       0,0,0,0,0,1,  // \
//                       0,0,0,0,0,1,  // / 2 [[x,x], [x,x], [x,x]] values skipped
//
//                       1,  2,  3,  4,  5,  6,  //  \
//                       0,  0,  0,  0,  0,  0   //  / the actual values
//                       7,  8,  9, 10, 11, 12   // /
//                     ]
//                ],
//              }
//            ]
//          }
//        ]
//     }
//
// The offset of the innermost values buffer, in bytes, is calculated as:
//
//     ((2 * 3) + (5 * 2) + 7) * sizeof(int32) = 29 * 4 bytes = 116 bytes
//
// In general, the formula to calculate the offset of the innermost values buffer is:
//
//     ((off_0 * fsl_size_0) + (off_1 * fsl_size_1) + ... + innermost_off)
//        * sizeof(innermost_type)
//
// `OffsetPointerOfFixedWidthValues()` can calculate this byte offset and return the
// pointer to the first relevant byte of the innermost values buffer.
//
// [1]: "The term modulo is often used to assert that two distinct objects can be regarded
//       as equivalent --- if their difference is accounted for by an additional factor."
//       https://en.wikipedia.org/wiki/Modulo_(mathematics)

namespace arrow::compute {
// XXX: remove dependency on compute::KernelContext
class KernelContext;
}  // namespace arrow::compute

namespace arrow::util {

namespace match {

struct AnyType {
  constexpr bool operator()(Type::type id) const { return true; }
};

template <Type::type... TypeIds>
struct ExcludeType {
  constexpr bool operator()(Type::type id) const { return !((id == TypeIds) || ...); }
};

}  // namespace match

/// \brief Checks if the given array has a fixed-width type or if it's a fixed-size
/// list that can be flattened to a fixed-width type.
///
/// A FixedSizeList array (of any level of nesting) can be treated as a fixed-width
/// array if the internal arrays don't need validity bitmaps and the innermost values
/// are fixed-width but not BOOL.
///
/// \tparam ExtraPred A predicate that can be used to further restrict the types that are
///                   considered fixed-width
/// \param[in] array The array to check
/// \param[in] force_null_count If true, GetNullCount() is used instead of null_count
template <typename ExtraPred = match::AnyType>
bool IsFixedWidthModuloNesting(const ArraySpan& array, bool force_null_count = false) {
  ExtraPred extra_pred;
  auto type_id = array.type->id();
  // BOOL is considered fixed-width if not nested under FIXED_SIZE_LIST.
  if (is_fixed_width(type_id) && extra_pred(type_id)) {
    return true;
  }
  if (type_id == Type::FIXED_SIZE_LIST) {
    // All the inner arrays must not contain any nulls.
    const auto* values = &array.child_data[0];
    while ((force_null_count ? values->GetNullCount() : values->null_count) == 0) {
      type_id = values->type->id();
      if (type_id == Type::FIXED_SIZE_LIST) {
        values = &values->child_data[0];
        continue;
      }
      // BOOL has to be excluded because it's not byte-aligned.
      return type_id != Type::BOOL && is_fixed_width(type_id) && extra_pred(type_id);
    }
  }
  return false;
}

namespace internal {

ARROW_EXPORT int64_t GetFixedByteWidthModuloNestingFallback(const FixedSizeListType&);

/// \brief Allocate an ArrayData for a type that is fixed-width modulo nesting.
///
/// This function performs the same checks performed by
/// `IsFixedWidthModuloNesting(source, false)`. If `source.type` is not a simple
/// fixed-width type, caller should make sure it passes the
/// `IsFixedWidthModuloNesting(source)` checks. That guarantees that it's possible to
/// allocate an array that can serve as a destination for a kernel that writes values
/// through a single pointer to fixed-width byte blocks.
///
/// \param[in] length The length of the array to allocate (unrelated to the length of
///                   the source array)
/// \param[in] source The source array that carries the type information and the
///                   validity bitmaps that are relevant for the type validation
///                   when the source is a FixedSizeList.
/// \see IsFixedWidthModuloNesting
ARROW_EXPORT Status PreallocateFixedWidthArrayData(::arrow::compute::KernelContext* ctx,
                                                   int64_t length,
                                                   const ArraySpan& source,
                                                   bool allocate_validity,
                                                   ArrayData* out);

}  // namespace internal

/// \brief Get the fixed-width in bytes of a type if it is a fixed-width modulo
/// nesting type, but not BOOL.
///
/// If the array is a FixedSizeList (of any level of nesting), the byte width of
/// the values is the product of all fixed-list sizes and the byte width of the
/// innermost fixed-width value type.
///
/// IsFixedWidthModuloNesting(array) performs more checks than this function and should
/// be used to guarantee that, if type is not BOOL, this function will not return -1.
///
/// \pre The instance of the array where this type is from must pass
///      IsFixedWidthModuloNesting(array) and should not be BOOL.
/// \return The fixed-byte width of the values or -1 if the type is BOOL or not
///         fixed-width modulo nesting. 0 is a valid return value as
///         fixed-size-lists and fixed-size-binary with size 0 are allowed.
inline int64_t GetFixedByteWidthModuloNesting(const DataType& type) {
  auto type_id = type.id();
  if (is_fixed_width(type_id)) {
    return type.byte_width();  // -1 if type_id == Type::BOOL
  }
  if (type_id == Type::FIXED_SIZE_LIST) {
    auto& fsl = ::arrow::internal::checked_cast<const FixedSizeListType&>(type);
    return internal::GetFixedByteWidthModuloNestingFallback(fsl);
  }
  return -1;
}

/// \brief Get the fixed-width in bits of a type if it is a fixed-width modulo
/// nesting type.
///
/// \return The bit-width of the values or -1
/// \see GetFixedByteWidthModuloNesting
inline int64_t GetFixedBitWidthModuloNesting(const DataType& type) {
  auto type_id = type.id();
  if (type_id == Type::BOOL) {
    return 1;
  }
  const int64_t byte_width = GetFixedByteWidthModuloNesting(type);
  if (ARROW_PREDICT_FALSE(byte_width < 0)) {
    return -1;
  }
  return byte_width * 8;
}

/// \pre array.type MUST NOT be BOOL
/// \pre is_fixed_width(*mutable_array->type) or
///      IsFixedWidthModuloNesting(array) MUST be true
/// \return The pointer to the fixed-width values of an array or NULLPTR
const uint8_t* OffsetPointerOfFixedWidthValues(const ArraySpan& array);

/// \brief Get the mutable pointer to the fixed-width values of an array
///        allocated by PreallocateFixedWidthArrayData.
///
/// \pre is_fixed_width(*mutable_array->type) or
///      IsFixedWidthModuloNesting(ArraySpan(mutable_array)) MUST be true
/// \pre mutable_array->offset and the offset of child array (if it's a
///      FixedSizeList) must be 0 (recursively).
uint8_t* MutableFixedWidthValuesPointer(ArrayData* mutable_array);

}  // namespace arrow::util
