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
#include <string>
#include <utility>

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>

#include "arrow/flight/ng/server.h"
#include "arrow/flight/ng/test/flight_test_fwd.h"
#include "arrow/flight/ng/test/test_auth_handlers.h"
#include "arrow/status.h"

namespace flight_test {

std::unique_ptr<::grpc::Server> BuildAndStartGrpcServer(
    std::unique_ptr<flight::FlightServer> flight_server,
    const std::string& server_address,
    std::shared_ptr<::grpc::ServerCredentials> server_credentials,
    int* selected_port = nullptr);

/// \brief Example server implementation to use for unit testing
/// and benchmarking purposes.
class TestFlightServer : public flight::FlightServer {
 public:
  explicit TestFlightServer(std::unique_ptr<flight::ServerAuthHandler> auth_handler);
  TestFlightServer(std::string username, std::string password);
  ~TestFlightServer() override;

  // TODO(felipecrv): Implement the Flight server logic
};

}  // namespace flight_test
