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

#include "arrow/array/builder_list_view.h"
#include "arrow/array/builder_primitive.h"

#include <cstdint>
#include <limits>
#include <vector>

#include "arrow/scalar.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/list_view_util.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// ListViewBuilder

ListViewBuilder::ListViewBuilder(MemoryPool* pool,
                                 std::shared_ptr<ArrayBuilder> const& value_builder,
                                 const std::shared_ptr<DataType>& type, int64_t alignment)
    : BaseListBuilder(pool, value_builder, type, alignment),
      sizes_builder_(pool, kDefaultBufferAlignment) {}

ListViewBuilder::~ListViewBuilder() = default;

Status ListViewBuilder::Resize(int64_t capacity) {
  // This allocates one extra slot for the offsets
  // that ListView's don't really need.
  RETURN_NOT_OK(BaseListBuilder::Resize(capacity));
  return sizes_builder_.Resize(capacity);
}

void ListViewBuilder::Reset() {
  BaseListBuilder::Reset();
  sizes_builder_.Reset();
}

Status ListViewBuilder::FinishInternal(std::shared_ptr<ArrayData>* out) {
  // Offset and sizes padding zeroed by BufferBuilder
  std::shared_ptr<Buffer> null_bitmap;
  std::shared_ptr<Buffer> offsets;
  std::shared_ptr<Buffer> sizes;
  ARROW_RETURN_NOT_OK(null_bitmap_builder_.Finish(&null_bitmap));
  ARROW_RETURN_NOT_OK(offsets_builder_.Finish(&offsets));
  ARROW_RETURN_NOT_OK(sizes_builder_.Finish(&sizes));

  if (value_builder_->length() == 0) {
    // Try to make sure we get a non-null values buffer (ARROW-2744)
    ARROW_RETURN_NOT_OK(value_builder_->Resize(0));
  }

  std::shared_ptr<ArrayData> items;
  ARROW_RETURN_NOT_OK(value_builder_->FinishInternal(&items));

  *out = ArrayData::Make(type(), length_,
                         {std::move(null_bitmap), std::move(offsets), std::move(sizes)},
                         {std::move(items)}, null_count_);
  Reset();
  return Status::OK();
}

}  // namespace arrow
