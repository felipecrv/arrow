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

#include "arrow/flight/ng/grpc_serde.h"

// TODO(felipecrv): cleanup includes

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/util/config.h"  // IWYU pragma: keep

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4267)
#endif

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

#include <grpc/byte_buffer_reader.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/proto_utils.h>

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

#include "arrow/buffer.h"
#include "arrow/device.h"
#include "arrow/ipc/message.h"
#include "arrow/ipc/writer.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/logging.h"

namespace protobuf = google::protobuf;

namespace arrow::flight {
inline namespace ng {
namespace grpc {

// Internal wrapper for gRPC ByteBuffer so its memory can be exposed to Arrow
// consumers with zero-copy.
class GrpcBuffer : public MutableBuffer {
 public:
  GrpcBuffer(grpc_slice slice, bool incref)
      : MutableBuffer(GRPC_SLICE_START_PTR(slice),
                      static_cast<int64_t>(GRPC_SLICE_LENGTH(slice))),
        slice_(incref ? grpc_slice_ref(slice) : slice) {}

  ~GrpcBuffer() override {
    // Decref slice
    grpc_slice_unref(slice_);
  }

  static Status Wrap(::grpc::ByteBuffer* cpp_buf, std::shared_ptr<Buffer>* out) {
    // These types are guaranteed by static assertions in gRPC
    // to have the same in-memory representation.
    auto buffer = *reinterpret_cast<grpc_byte_buffer**>(cpp_buf);

    // This part below is based on the Flatbuffers gRPC SerializationTraits
    // in flatbuffers/grpc.h

    // Check if this is a single uncompressed slice.
    if ((buffer->type == GRPC_BB_RAW) &&
        (buffer->data.raw.compression == GRPC_COMPRESS_NONE) &&
        (buffer->data.raw.slice_buffer.count == 1)) {
      // If it is, then we can reference the `grpc_slice` directly.
      grpc_slice slice = buffer->data.raw.slice_buffer.slices[0];

      if (slice.refcount) {
        // Increment reference count so this memory remains valid
        *out = std::make_shared<GrpcBuffer>(slice, true);
      } else {
        // Small slices (less than GRPC_SLICE_INLINED_SIZE bytes) are
        // inlined into the structure and must be copied.
        const uint8_t length = slice.data.inlined.length;
        ARROW_ASSIGN_OR_RAISE(*out, arrow::AllocateBuffer(length));
        std::memcpy((*out)->mutable_data(), slice.data.inlined.bytes, length);
      }
    } else {
      // Otherwise, we need to use `grpc_byte_buffer_reader_readall` to read
      // `buffer` into a single contiguous `grpc_slice`. The gRPC reader gives
      // us back a new slice with the refcount already incremented.
      grpc_byte_buffer_reader reader;
      if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return Status::IOError("Internal gRPC error reading from ByteBuffer");
      }
      grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
      if (slice.refcount) {
        // Steal the slice reference
        *out = std::make_shared<GrpcBuffer>(slice, false);
      } else {
        // grpc_byte_buffer_reader_readall can give us an inlined slice,
        // copy the data as above
        const uint8_t length = slice.data.inlined.length;
        ARROW_ASSIGN_OR_RAISE(*out, arrow::AllocateBuffer(length));
        std::memcpy((*out)->mutable_data(), slice.data.inlined.bytes, length);
      }
      grpc_byte_buffer_reader_destroy(&reader);
    }

    return Status::OK();
  }

 private:
  grpc_slice slice_;
};

// Destructor callback for grpc::Slice
static void ReleaseBuffer(void* buf_ptr) {
  delete reinterpret_cast<std::shared_ptr<Buffer>*>(buf_ptr);
}

// Initialize gRPC Slice from arrow Buffer
arrow::Result<::grpc::Slice> SliceFromBuffer(const std::shared_ptr<Buffer>& buf) {
  // Allocate persistent shared_ptr to control Buffer lifetime
  std::shared_ptr<Buffer>* ptr = nullptr;
  if (ARROW_PREDICT_TRUE(buf->is_cpu())) {
    ptr = new std::shared_ptr<Buffer>(buf);
  } else {
    // Non-CPU buffer, must copy to CPU-accessible buffer first
    ARROW_ASSIGN_OR_RAISE(auto cpu_buf,
                          Buffer::ViewOrCopy(buf, default_cpu_memory_manager()));
    ptr = new std::shared_ptr<Buffer>(cpu_buf);
  }
  ::grpc::Slice slice(const_cast<uint8_t*>((*ptr)->data()),
                      static_cast<size_t>((*ptr)->size()), &ReleaseBuffer, ptr);
  // Make sure no copy was done (some grpc::Slice() constructors do an implicit memcpy)
  DCHECK_EQ(slice.begin(), (*ptr)->data());
  return slice;
}

// static const uint8_t kPaddingBytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};

// Update the sizes of our Protobuf fields based on the given IPC payload.
::grpc::Status IpcMessageHeaderSize(const arrow::ipc::IpcPayload& ipc_msg, bool has_body,
                                    size_t* header_size, int32_t* metadata_size) {
  static constexpr int64_t kInt32Max = std::numeric_limits<int32_t>::max();

  DCHECK_LE(ipc_msg.metadata->size(), kInt32Max);
  *metadata_size = static_cast<int32_t>(ipc_msg.metadata->size());

  // 1 byte for metadata tag
  *header_size +=
      1 + protobuf::internal::WireFormatLite::LengthDelimitedSize(*metadata_size);

  // 2 bytes for body tag
  if (has_body) {
    // We write the body tag in the header but not the actual body data
    *header_size +=
        2 + protobuf::internal::WireFormatLite::LengthDelimitedSize(ipc_msg.body_length) -
        ipc_msg.body_length;
  }

  return ::grpc::Status::OK;
}

::grpc::Status FlightDataSerialize(const arrow::flight::FlightPayload& msg,
                                   ::grpc::ByteBuffer* out, bool* own_buffer) {
  // TODO(felipecrv): implement this
  return {};
}

::grpc::Status FlightDataDeserialize(::grpc::ByteBuffer* buffer,
                                     arrow::flight::FlightPayload* out) {
  // Reset fields in case the caller reuses a single allocation
  out->Clear();
  if (!buffer) {
    return {::grpc::StatusCode::INTERNAL, "No payload"};
  }
  // TODO(felipecrv): implement this
  return {};
}

}  // namespace grpc
}  // namespace ng
}  // namespace arrow::flight
