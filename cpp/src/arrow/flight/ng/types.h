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

#include <memory>

#include "arrow/buffer.h"
#include "arrow/flight/visibility.h"
#include "arrow/ipc/writer.h"
#include "arrow/status.h"

namespace arrow::flight {
inline namespace ng {

// FlightData in Flight.proto maps to FlightPayload here.

/// \brief Staging data structure for messages about to be put on the wire
///
/// This structure corresponds to FlightData in the protocol.
struct ARROW_FLIGHT_EXPORT FlightPayload {
  std::shared_ptr<Buffer> descriptor;
  std::shared_ptr<Buffer> app_metadata;
  ipc::IpcPayload ipc_message;

  FlightPayload();
  ~FlightPayload();

  FlightPayload(std::shared_ptr<Buffer> descriptor, std::shared_ptr<Buffer> app_metadata,
                ipc::IpcPayload ipc_message)
      : descriptor(std::move(descriptor)),
        app_metadata(std::move(app_metadata)),
        ipc_message(std::move(ipc_message)) {}

  /// \brief Check that the payload can be written to the wire.
  Status Validate() const;
};

}  // namespace ng
}  // namespace arrow::flight
