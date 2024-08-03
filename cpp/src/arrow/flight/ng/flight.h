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

#ifdef _MSC_VER

// The protobuf documentation says that C4251 warnings when using the
// library are spurious and suppressed when the build the library and
// compiler, but must be also suppressed in downstream projects
#pragma warning(disable : 4251)

#endif  // _MSC_VER

#include "arrow/status.h"
#include "arrow/util/config.h"  // IWYU pragma: keep

// Silence protobuf warnings
#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4244)
#pragma warning(disable : 4267)
#endif
// #include <grpcpp/impl/codegen/config_protobuf.h>  // XXX: needed?
// #include <grpcpp/impl/codegen/proto_utils.h>      // XXX: needed?
#ifdef _MSC_VER
#pragma warning(pop)
#endif

// Forward declarations of gRPC types {{{
namespace grpc {
class ByteBuffer;
}  // namespace grpc
// }}}

namespace arrow::flight {

// Forward declarations of Protocol Buffers types {{{
namespace protocol {
class FlightData;
class FlightDescriptor;
}  // namespace protocol
// }}}

inline namespace ng {

using FlightDataVisitor =
    std::function<void(const protocol::FlightDescriptor&, std::string_view data_header,
                       std::string_view app_metadata, std::string_view data_body)>;

void VisitFlightData(const protocol::FlightData& data, FlightDataVisitor visitor);

// gRPC transport {{{
namespace transport::grpc {
//
}  // namespace transport::grpc
// }}}

}  // namespace ng
}  // namespace arrow::flight

// Customized gRPC serialization traits for Flight {{{
namespace grpc {
//
}
// }}}
