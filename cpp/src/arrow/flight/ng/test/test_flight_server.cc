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

#include <memory>

#include <grpcpp/grpcpp.h>

#include "arrow/flight/ng/grpc_server.h"
#include "arrow/flight/ng/test/test_flight_server.h"

namespace flight_test {

// See test/test_server.cc for the usage of this function from main().
std::unique_ptr<::grpc::Server> BuildAndStartGrpcServer(
    std::unique_ptr<flight::FlightServer> flight_server,
    const std::string& server_address,
    std::shared_ptr<::grpc::ServerCredentials> server_credentials, int* selected_port) {
  auto grpc_flight_server =
      std::make_unique<flight::GrpcFlightServer>(std::move(flight_server));

  ::grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, std::move(server_credentials), selected_port);
  builder.RegisterService(grpc_flight_server.get());
  // Disable SO_REUSEPORT - it makes debugging/testing a pain as
  // leftover processes can handle requests on accident
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  return builder.BuildAndStart();
}

//------------------------------------------------------------
// TestFlightServer

TestFlightServer::TestFlightServer(
    std::unique_ptr<flight::ServerAuthHandler> auth_handler)
    : flight::FlightServer(std::move(auth_handler)) {}

TestFlightServer::TestFlightServer(std::string username, std::string password)
    : TestFlightServer(std::make_unique<TestServerAuthHandler>(std::move(username),
                                                               std::move(password))) {}

TestFlightServer::~TestFlightServer() = default;

}  // namespace flight_test
