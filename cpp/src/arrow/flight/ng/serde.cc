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
#include <string_view>

#include <google/protobuf/io/coded_stream.h>

#include "arrow/flight/Flight.pb.h"
#include "arrow/flight/ng/serde.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow::flight {
inline namespace ng {

namespace {

// https://protobuf.dev/programming-guides/encoding/#structure
// LEN: string, bytes, embedded messages, packed repeated fields
const uint32_t kLenWireType = 2;
// EGROUP: group end (deprecated)
const uint32_t kGroupEndWireType = 4;

inline bool TagIsFieldAndWireType(uint32_t tag, uint32_t field_number,
                                  uint32_t wire_type) {
  return tag == ((field_number << 3) | wire_type);
}

#define FAIL_OTHERWISE(expr)          \
  if (ARROW_PREDICT_FALSE(!(expr))) { \
    return false;                     \
  }

#define HANDLE_UNUSUAL                                  \
  if ((tag == 0 && !is->BytesUntilTotalBytesLimit()) || \
      ((tag & 0x7) == kGroupEndWireType)) {             \
    return true;                                        \
  }                                                     \
  FAIL_OTHERWISE(google::protobuf::internal::WireFormatLite::SkipField(is, tag))

// Zero-copy parse the protocol.FlightData message by consuming the entire input stream.
bool ParseFlightData(google::protobuf::io::CodedInputStream* is,
                     protocol::FlightDescriptor* flight_descriptor,
                     std::string_view* data_header, std::string_view* app_metadata,
                     std::string_view* data_body) {
  while (is->BytesUntilTotalBytesLimit()) {
    const uint32_t tag = is->ReadTag();
    // tag == 0 is handled by the default: case.
    const uint32_t field_number = tag >> 3;
    const void* ptr;
    int field_size;
    int size;
    switch (field_number) {
      case 1:  // FlightDescriptor flight_descriptor = 1;
        if (ARROW_PREDICT_TRUE(TagIsFieldAndWireType(tag, 1, kLenWireType))) {
          FAIL_OTHERWISE(is->ReadVarintSizeAsInt(&field_size));
          is->GetDirectBufferPointerInline(&ptr, &size);
          FAIL_OTHERWISE(size >= field_size);
          FAIL_OTHERWISE(is->Skip(field_size));
          FAIL_OTHERWISE(flight_descriptor->ParseFromArray(ptr, field_size));
        } else {
          HANDLE_UNUSUAL;
        }
        break;
      case 2:  // bytes data_header = 2;
        if (ARROW_PREDICT_TRUE(TagIsFieldAndWireType(tag, 2, kLenWireType))) {
          FAIL_OTHERWISE(is->ReadVarintSizeAsInt(&field_size));
          is->GetDirectBufferPointerInline(&ptr, &size);
          FAIL_OTHERWISE(size >= field_size);
          FAIL_OTHERWISE(is->Skip(field_size));
          *data_header = std::string_view(reinterpret_cast<const char*>(ptr), field_size);
        } else {
          HANDLE_UNUSUAL;
        }
        break;
      case 3:  // bytes app_metadata = 3;
        if (ARROW_PREDICT_TRUE(TagIsFieldAndWireType(tag, 3, kLenWireType))) {
          FAIL_OTHERWISE(is->ReadVarintSizeAsInt(&field_size));
          is->GetDirectBufferPointerInline(&ptr, &size);
          FAIL_OTHERWISE(size >= field_size);
          FAIL_OTHERWISE(is->Skip(field_size));
          *app_metadata =
              std::string_view(reinterpret_cast<const char*>(ptr), field_size);
        } else {
          HANDLE_UNUSUAL;
        }
      case 1000:  // bytes data_body = 1000;
        if (ARROW_PREDICT_TRUE(TagIsFieldAndWireType(tag, 1000, kLenWireType))) {
          FAIL_OTHERWISE(is->ReadVarintSizeAsInt(&field_size));
          is->GetDirectBufferPointerInline(&ptr, &size);
          FAIL_OTHERWISE(size >= field_size);
          FAIL_OTHERWISE(is->Skip(field_size));
          *data_body = std::string_view(reinterpret_cast<const char*>(ptr), field_size);
        } else {
          HANDLE_UNUSUAL;
        }
        break;
      default:
        HANDLE_UNUSUAL;
        break;
    }
  }
  return true;
}

#undef FAIL_OTHERWISE
#undef HANDLE_UNUSUAL
}  // namespace

Status VisitFlightData(const std::string_view serialized, FlightDataVisitor visitor) {
  const auto* data = reinterpret_cast<const uint8_t*>(serialized.data());
  const auto size = static_cast<int>(serialized.size());
  protocol::FlightDescriptor flight_descriptor;
  std::string_view data_header;
  std::string_view app_metadata;
  std::string_view data_body;
  google::protobuf::io::CodedInputStream is(data, size);
  if (ARROW_PREDICT_FALSE(!ParseFlightData(&is, &flight_descriptor, &data_header,
                                           &app_metadata, &data_body))) {
    return Status::Invalid("Failed to parse arrow.flight.protocol.FlightData");
  }
  return visitor(&flight_descriptor, data_header, app_metadata, data_body);
}

Status ReadAllHandshakeRequests(Reader<protocol::HandshakeRequest>* reader,
                                HandshakeRequestVisitor visitor) {
  protocol::HandshakeRequest request;
  if (!reader->Read(&request)) {
    return Status::Cancelled();
  }
  do {
    const auto protocol_version = request.protocol_version();
    const auto& payload = request.payload();
    ARROW_RETURN_NOT_OK(visitor(protocol_version, payload));
  } while (reader->Read(&request));
  return Status::OK();
}

}  // namespace ng
}  // namespace arrow::flight
