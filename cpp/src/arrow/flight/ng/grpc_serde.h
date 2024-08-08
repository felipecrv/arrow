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

#include <functional>
#include <string_view>

#ifdef _MSC_VER

// The protobuf documentation says that C4251 warnings when using the
// library are spurious and suppressed when the build the library and
// compiler, but must be also suppressed in downstream projects
#pragma warning(disable : 4251)

#endif  // _MSC_VER

#include "arrow/flight/ng/flight_fwd.h"
#include "arrow/flight/ng/types.h"
#include "arrow/status.h"
#include "arrow/util/config.h"  // IWYU pragma: keep

#include "arrow/flight/Flight.pb.h"

// Silence protobuf warnings
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4244)
#pragma warning(disable : 4267)
#endif
#include <grpc/byte_buffer_reader.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/config_protobuf.h>  // XXX: needed?
#include <grpcpp/impl/codegen/proto_utils.h>      // XXX: needed?
#include <grpcpp/support/sync_stream.h>
#ifdef _MSC_VER
#pragma warning(pop)
#endif

namespace arrow::flight {
inline namespace ng {
// Flight+gRPC Zero-Copy SerDe {{{
namespace grpc {
::grpc::Status FlightDataSerialize(const arrow::flight::FlightPayload& msg,
                                   ::grpc::ByteBuffer* out, bool* own_buffer);
::grpc::Status FlightDataDeserialize(::grpc::ByteBuffer* buffer,
                                     arrow::flight::FlightPayload* out);

}  // namespace grpc
// }}}
}  // namespace ng
}  // namespace arrow::flight

// Customized gRPC serialization traits for Flight {{{
namespace grpc {
template <>
class SerializationTraits<arrow::flight::protocol::FlightData> {
#ifdef GRPC_CUSTOM_MESSAGELITE
  using MessageType = grpc::protobuf::MessageLite;
#else
  using MessageType = grpc::protobuf::Message;
#endif

 public:
  static Status Serialize(const MessageType& msg, ByteBuffer* bb, bool* own_buffer) {
    auto& flight_payload = reinterpret_cast<const arrow::flight::ng::FlightPayload&>(msg);
    return arrow::flight::grpc::FlightDataSerialize(flight_payload, bb, own_buffer);
  }

  static Status Deserialize(ByteBuffer* buffer, MessageType* msg) {
    auto* flight_payload = reinterpret_cast<arrow::flight::ng::FlightPayload*>(msg);
    return arrow::flight::grpc::FlightDataDeserialize(buffer, flight_payload);
  }
};
}  // namespace grpc
// }}}

namespace arrow::flight {
inline namespace ng {
namespace grpc {

// Pointer bitcast explanation: grpc::*Writer<T>::Write() and grpc::*Reader<T>::Read()
// both take a T* argument (here protocol::FlightData*).  But they don't do anything
// with that argument except pass it to SerializationTraits<T>::Serialize() and
// SerializationTraits<T>::Deserialize().
//
// Since we control SerializationTraits<protocol::FlightData>, we can interpret the
// pointer argument whichever way we want, including cast it back to the original type.

inline Result<bool> ServerWriterWrite(const FlightPayload& payload,
                                      ::grpc::ServerWriter<protocol::FlightData>* writer,
                                      ::grpc::WriteOptions opts) {
  RETURN_NOT_OK(payload.Validate());
  auto& data = reinterpret_cast<const protocol::FlightData&>(payload);
  return writer->Write(data, opts);
}

template <typename W, typename R>
std::enable_if_t<std::is_same_v<W, protocol::FlightData>, Result<bool>>
ClientReaderWriterWrite(const FlightPayload& payload,
                        ::grpc::ClientReaderWriter<W, R>* writer,
                        ::grpc::WriteOptions opts) {
  RETURN_NOT_OK(payload.Validate());
  auto& data = reinterpret_cast<const protocol::FlightData&>(payload);
  return writer->Write(data, opts);
}

template <typename W, typename R>
std::enable_if_t<std::is_same_v<W, protocol::FlightData>, Result<bool>>
ServerReaderWriterWrite(const FlightPayload& payload,
                        ::grpc::ServerReaderWriter<W, R>* writer,
                        ::grpc::WriteOptions opts) {
  RETURN_NOT_OK(payload.Validate());
  auto& data = reinterpret_cast<const protocol::FlightData&>(payload);
  return writer->Write(data, opts);
}

inline bool ClientReaderRead(::grpc::ClientReader<protocol::FlightData>* reader,
                             flight::FlightPayload* out_payload) {
  auto* out_data = reinterpret_cast<protocol::FlightData*>(out_payload);
  return reader->Read(out_data);
}

template <typename W, typename R>
std::enable_if_t<std::is_same_v<R, protocol::FlightData>, bool> ClientReaderWriterRead(
    ::grpc::ClientReaderWriter<W, R>* reader, FlightPayload* out_payload) {
  auto* out_data = reinterpret_cast<R*>(out_payload);
  return reader->Read(out_data);
}

template <typename W, typename R>
std::enable_if_t<std::is_same_v<R, protocol::FlightData>, bool> ServerReaderWriterRead(
    ::grpc::ServerReaderWriter<W, R>* reader, FlightPayload* out_payload) {
  auto* out_data = reinterpret_cast<R*>(out_payload);
  return reader->Read(out_data);
}

}  // namespace grpc
}  // namespace ng
}  // namespace arrow::flight
