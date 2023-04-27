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
#include <memory>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_nested.h"

namespace arrow {

/// \addtogroup list-view-builders
///
/// @{

// TODO(felipecrv): consider moving this to builder_nested.h

// ----------------------------------------------------------------------
// ListView builder

class ARROW_EXPORT ListViewBuilder final : public BaseListBuilder<ListViewType> {
 public:
  ListViewBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> const& value_builder,
                  const std::shared_ptr<DataType>& type,
                  int64_t alignment = kDefaultBufferAlignment);

  ListViewBuilder(MemoryPool* pool, std::shared_ptr<ArrayBuilder> const& value_builder,
                  int64_t alignment = kDefaultBufferAlignment)
      : ListViewBuilder(pool, value_builder, list_view(value_builder->type()),
                        alignment) {}

  ~ListViewBuilder() override;

  /// \brief Allocate enough memory for a given number of list views.
  ///
  /// NOTE: this does not affect the nested values array builder.
  Status Resize(int64_t capacity) override;

  Status ReserveChildValues(int64_t elements) {
    return value_builder_->Reserve(elements);
  }

  void Reset() override;

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<RunEndEncodedArray>* out) { return FinishTyped(out); }

 protected:
  void UnsafeAppendEmptyDimensions(int64_t num_values) override {
    for (int64_t i = 0; i < num_values; ++i) {
      offsets_builder_.UnsafeAppend(0);
      sizes_builder_.UnsafeAppend(0);
    }
  }

  void UnsafeAppendDimensions(int64_t offset, int64_t size) override {
    offsets_builder_.UnsafeAppend(static_cast<offset_type>(offset));
    sizes_builder_.UnsafeAppend(static_cast<offset_type>(size));
  }

 private:
  TypedBufferBuilder<int32_t> sizes_builder_;
};

/// @}

}  // namespace arrow
