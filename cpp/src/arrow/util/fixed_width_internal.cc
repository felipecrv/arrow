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
#include <optional>
#include <utility>

#include "arrow/array/data.h"
#include "arrow/compute/kernel.h"
#include "arrow/result.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging.h"
#include "arrow/util/small_vector.h"

namespace arrow::util {

using ::arrow::internal::checked_cast;

namespace internal {

Status PreallocateFixedWidthArrayData(::arrow::compute::KernelContext* ctx,
                                      int64_t length, const ArraySpan& source,
                                      bool allocate_validity, ArrayData* out) {
  DCHECK(!source.MayHaveNulls() || allocate_validity)
      << "allocate_validity cannot be false if source may have nulls";
  auto* type = source.type;
  out->length = length;
  out->buffers.resize(type->id() == Type::FIXED_SIZE_LIST ? 1 : 2);
  if (allocate_validity) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[0], ctx->AllocateBitmap(length));
  }

  if (type->id() == Type::BOOL) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->AllocateBitmap(length));
    return Status::OK();
  }

  if (is_fixed_width(type->id())) {
    if (type->id() == Type::DICTIONARY) {
      return Status::NotImplemented(
          "PreallocateFixedWidthArrayData: DICTIONARY type allocation: ", *type);
    }
    ARROW_ASSIGN_OR_RAISE(out->buffers[1],
                          ctx->Allocate(length * source.type->byte_width()));
    return Status::OK();
  }
  if (type->id() == Type::FIXED_SIZE_LIST) {
    auto& fsl_type = checked_cast<const FixedSizeListType&>(*type);
    auto& value_type = fsl_type.value_type();
    if (is_fixed_width(value_type->id())) {
      if (value_type->id() == Type::BOOL) {
        return Status::Invalid("PreallocateFixedWidthArrayData: Invalid type: ",
                               fsl_type);
      }
      if (value_type->id() == Type::DICTIONARY) {
        return Status::NotImplemented(
            "PreallocateFixedWidthArrayData: DICTIONARY type allocation: ", *type);
      }
      auto* values = &source.child_data[0];
      if (values->MayHaveNulls()) {
        return Status::Invalid(
            "PreallocateFixedWidthArrayData: "
            "FixedSizeList may have null values in child array: ",
            fsl_type);
      }
      auto allocated_values = std::make_shared<ArrayData>();
      allocated_values->type = fsl_type.value_type();
      RETURN_NOT_OK(PreallocateFixedWidthArrayData(
          ctx, length * fsl_type.list_size(), *values,
          /*allocate_validity=*/false, allocated_values.get()));
      out->child_data.resize(1);
      out->child_data[0] = std::move(allocated_values);
    }
    return Status::OK();
  }
  return Status::Invalid("PreallocateFixedWidthArrayData: Invalid type: ", *type);
}

int64_t GetFixedByteWidthModuloNestingFallback(
    const FixedSizeListType& fixed_size_list_type) {
  auto* fsl = &fixed_size_list_type;
  int64_t list_size = fsl->list_size();
  for (auto type = fsl->value_type().get();;) {
    if (type->id() == Type::FIXED_SIZE_LIST) {
      fsl = checked_cast<const FixedSizeListType*>(type);
      list_size *= fsl->list_size();
      type = fsl->value_type().get();
      continue;
    }
    if (type->id() != Type::BOOL && is_fixed_width(type->id())) {
      const int64_t flat_byte_width = list_size * type->byte_width();
      DCHECK_GE(flat_byte_width, 0);
      return flat_byte_width;
    }
    break;
  }
  return -1;
}

}  // namespace internal

const uint8_t* OffsetPointerOfFixedWidthValues(const ArraySpan& array) {
  using OffsetAndListSize = std::pair<int64_t, int64_t>;
  auto get_offset = [](auto pair) { return pair.first; };
  auto get_list_size = [](auto pair) { return pair.second; };
  ::arrow::internal::SmallVector<OffsetAndListSize, 1> stack;

  if (array.type->id() == Type::BOOL) {
    DCHECK(false)
        << "BOOL arrays are bit-packed, a byte-aligned pointer cannot be produced.";
    return nullptr;
  }

  int64_t list_size = 1;
  auto* arr = &array;
  while (arr->type->id() == Type::FIXED_SIZE_LIST) {
    list_size *= checked_cast<const FixedSizeListType*>(arr->type)->list_size();
    stack.emplace_back(arr->offset, list_size);
    arr = &arr->child_data[0];
  }
  // Now that innermost values were reached, pop the stack and calculate the offset
  // in bytes of the innermost values buffer by considering the offset at each
  // level of nesting.
  DCHECK(arr->type->id() != Type::BOOL && is_fixed_width(*arr->type));
  DCHECK(arr == &array || !arr->MayHaveNulls())
      << "OffsetPointerOfFixedWidthValues: array is expected to be flat or have no "
         "nulls in the arrays nested by FIXED_SIZE_LIST.";
  int64_t value_width = arr->type->byte_width();
  int64_t offset_in_bytes = arr->offset * value_width;
  for (auto it = stack.rbegin(); it != stack.rend(); ++it) {
    value_width *= get_list_size(*it);
    offset_in_bytes += get_offset(*it) * value_width;
  }
  return value_width < 0 ? nullptr : arr->GetValues<uint8_t>(1, offset_in_bytes);
}

uint8_t* MutableFixedWidthValuesPointer(ArrayData* mutable_array) {
  auto* type = mutable_array->type.get();
  if (type->id() == Type::FIXED_SIZE_LIST) {
    auto* fsl = checked_cast<const FixedSizeListType*>(type);
    DCHECK_EQ(mutable_array->offset, 0);
    for (;;) {
      mutable_array = mutable_array->child_data[0].get();
      type = fsl->value_type().get();
      DCHECK_EQ(mutable_array->offset, 0);
      if (type->id() == Type::FIXED_SIZE_LIST) {
        fsl = checked_cast<const FixedSizeListType*>(type);
      } else {
        DCHECK(type->id() != Type::BOOL && is_fixed_width(*type));
        break;
      }
    }
  }
  // BOOL is allowed here only because the offset is expected to be 0,
  // so the byte-aligned pointer also points to the first *bit* of the buffer.
  DCHECK(is_fixed_width(*type));
  DCHECK_EQ(mutable_array->offset, 0);
  return mutable_array->GetMutableValues<uint8_t>(1, 0);
}

}  // namespace arrow::util
