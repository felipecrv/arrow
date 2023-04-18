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

#include "arrow/array/list_view.h"
#include "arrow/array/util.h"
#include "arrow/util/logging.h"

namespace arrow {

// ----------------------------------------------------------------------
// ListViewArray

Result<std::shared_ptr<ListViewArray>> ListViewArray::Make(
    const std::shared_ptr<DataType>& type, int64_t length,
    const std::vector<std::shared_ptr<Buffer>>& buffers,
    const std::shared_ptr<Array>& values, int64_t null_count, int64_t offset,
    const std::shared_ptr<Buffer>& offsets_validity) {
  if (type->id() != Type::LIST_VIEW) {
    return Status::Invalid("Type must be LIST_VIEW");
  }
  // TODO(felipecrv): validation?
  return std::make_shared<ListViewArray>(type, length, buffers, values, null_count,
                                         offset);
}

Result<std::shared_ptr<ListViewArray>> ListViewArray::Make(
    int64_t length, const std::vector<std::shared_ptr<Buffer>>& buffers,
    const std::shared_ptr<Array>& values, int64_t null_count, int64_t offset,
    const std::shared_ptr<Buffer>& offsets_validity) {
  auto list_view_type = list_view(values->type());
  return Make(list_view_type, length, buffers, values, null_count, offset,
              offsets_validity);
}

Result<std::shared_ptr<ListViewArray>> ListViewArray::FromArrays(
    const std::shared_ptr<DataType>& type, const std::shared_ptr<Array>& offsets,
    const std::shared_ptr<Array>& sizes, const std::shared_ptr<Array>& values) {
  if (type->id() != Type::LIST_VIEW) {
    return Status::Invalid("Expected an list-view type");
  }
  if (offsets->type()->id() != Type::INT32) {
    return Status::Invalid("offsets must be int32");
  }
  if (sizes->type()->id() != Type::INT32) {
    return Status::Invalid("sizes must be int32");
  }
  const auto* list_view_type = internal::checked_cast<const ListViewType*>(type.get());
  if (!list_view_type->value_type()->Equals(*values->type())) {
    return Status::Invalid("values type must match ",
                           list_view_type->value_type()->ToString());
  }
  if (offsets->length() != sizes->length()) {
    return Status::Invalid("offsets and sizes must have the same length");
  }
  if (offsets->offset() != sizes->offset()) {
    return Status::Invalid("offsets and sizes must have the same offset");
  }
  const std::vector<std::shared_ptr<Buffer>> buffers = {
      sizes->data()->buffers[0],
      offsets->data()->buffers[1],
      sizes->data()->buffers[1],
  };
  return Make(type, sizes->length(), buffers, values, sizes->null_count(),
              sizes->offset(), offsets->data()->buffers[0]);
}

}  // namespace arrow
