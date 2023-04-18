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

// Array accessor classes for list-view arrays

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/data.h"
#include "arrow/result.h"
#include "arrow/type_fwd.h"

namespace arrow {

/// \addtogroup list-view-arrays
///
/// @{

// ----------------------------------------------------------------------
// ListView

/// \brief Array type for list-view arrays
class ARROW_EXPORT ListViewArray : public BaseListArray<ListViewType> {
 public:
  explicit ListViewArray(const std::shared_ptr<ArrayData>& data);

  // TODO(felipecrv): Make ListViewArray more similar to ListView
  // ListViewArray(std::shared_ptr<DataType> type, int64_t length,
  //           std::shared_ptr<Buffer> value_offsets,
  //           std::shared_ptr<Buffer> value_sizes,
  //           std::shared_ptr<Array> values,
  //           std::shared_ptr<Buffer> null_bitmap = NULLPTR,
  //           int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  /// \brief Construct ListViewArray from all parameters
  ///
  /// The length and offset parameters refer to the dimensions of the logical
  /// array. As such, length can be much smaller than the length of the nested
  /// values array.
  ListViewArray(const std::shared_ptr<DataType>& type, int64_t length,
                const std::vector<std::shared_ptr<Buffer>>& buffers,
                const std::shared_ptr<Array>& values,
                int64_t null_count = kUnknownNullCount, int64_t offset = 0);

  /// \brief Construct an ListViewArray
  ///
  /// The offsets and sizes in the buffers should be valid for the nested values array.
  static Result<std::shared_ptr<ListViewArray>> Make(
      const std::shared_ptr<DataType>& type, int64_t length,
      const std::vector<std::shared_ptr<Buffer>>& buffers,
      const std::shared_ptr<Array>& values, int64_t null_count = kUnknownNullCount,
      int64_t offset = 0, const std::shared_ptr<Buffer>& offsets_validity = NULLPTR);

  /// \brief Construct an ListViewArray
  ///
  /// The data type is automatically inferred from the arguments. The offsets and sizes in
  /// the buffers should be valid for the nested values array.
  static Result<std::shared_ptr<ListViewArray>> Make(
      int64_t length, const std::vector<std::shared_ptr<Buffer>>& buffers,
      const std::shared_ptr<Array>& values, int64_t null_count = kUnknownNullCount,
      int64_t offset = 0, const std::shared_ptr<Buffer>& offsets_validity = NULLPTR);

  /// \brief Construct an ListViewArray
  ///
  /// Construct an ListViewArray using buffers from offsets and sizes arrays
  /// that project views into the values array.
  ///
  /// \note Each individual array is assumed to be valid by itself, and this
  /// function only validates that they can be combined into a valid ListViewArray.
  ///
  /// \note As this function is not expected to allocate new buffers, the
  /// offset of the offsets and sizes arrays is expected to be the same. If that
  /// is not the case a Status::Invalid will be returned.
  ///
  /// \param type An ListViewType instance
  /// \param offsets An array of int32 offsets into the values array. NULL values are
  /// supported if the corresponding values in sizes is NULL or 0.
  /// \param sizes An array containing the int32 sizes of every view. NULL values are
  /// taken to represent a NULL listy-view in the array being created.
  /// \param values The array that is being nested into the ListViewArray.
  static Result<std::shared_ptr<ListViewArray>> FromArrays(
      const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& offsets,
      const std::shared_ptr<Array>& sizes, const std::shared_ptr<Array>& values);

  // The following functions will not perform boundschecking
  offset_type value_length(int64_t i) const {
    return raw_value_sizes_[data_->offset + i];
  }
  std::shared_ptr<Array> value_slice(int64_t i) const {
    return values_->Slice(value_offset(i), value_length(i));
  }

 protected:
  void SetData(const std::shared_ptr<ArrayData>& data);

  const offset_type* raw_value_sizes_ = NULLPTR;
};

/// @}

}  // namespace arrow
