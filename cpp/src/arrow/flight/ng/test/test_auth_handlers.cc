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
#include <optional>
#include <string>

#include "arrow/flight/ng/test/flight_test_fwd.h"
#include "arrow/flight/ng/test/test_auth_handlers.h"

#include "arrow/flight/Flight.pb.h"

namespace flight_test {

namespace {
const char* kGrpcAuthHeader = "auth-token-bin";
}

//------------------------------------------------------------
// TestServerAuthHandler

TestServerAuthHandler::TestServerAuthHandler(std::string username, std::string token)
    : username_(std::move(username)), token_(std::move(token)) {}

TestServerAuthHandler::~TestServerAuthHandler() = default;

Status TestServerAuthHandler::HandshakeGrpc(
    ::grpc::ServerContext* context, flight::Reader<protocol::HandshakeRequest>* reader,
    flight::Writer<protocol::HandshakeResponse>* writer) {
  protocol::HandshakeRequest request;
  if (!reader->Read(&request)) {
    return Status::Cancelled("missing HandshakeRequest");
  }
  // Validate the username...
  if (request.payload() != username_) {
    // XXX: use a better error status
    return Status::Invalid("Invalid username or password");
  }
  // ...and trust it right away with the token.
  protocol::HandshakeResponse response;
  response.set_protocol_version(request.protocol_version());
  response.set_payload(token_);
  writer->WriteLast(response);
  return Status::OK();
}

Status TestServerAuthHandler::ValidateGrpcContext(::grpc::ServerContext* context) {
  auto maybe_token = GetTokenFromContext(context);
  if (!maybe_token.has_value()) {
    return Status::Invalid("No token provided in client metadata -- 'auth-token-bin'");
  }
  if (maybe_token.value() != token_) {
    return Status::Invalid("Invalid token");
  }
  return Status::OK();
}

std::optional<std::string_view> TestServerAuthHandler::GetTokenFromContext(
    ::grpc::ServerContext* context) {
  const auto& client_metadata = context->client_metadata();
  const auto auth_header = client_metadata.find(kGrpcAuthHeader);
  if (auth_header == client_metadata.end()) {
    return std::nullopt;
  }
  return std::string_view{auth_header->second.data(), auth_header->second.length()};
}

//------------------------------------------------------------
// TestServerBasicAuthHandler

TestServerBasicAuthHandler::TestServerBasicAuthHandler(std::string username,
                                                       std::string password)
    : username_(std::move(username)), password_(std::move(password)) {}

TestServerBasicAuthHandler::~TestServerBasicAuthHandler() = default;

Status TestServerBasicAuthHandler::HandshakeGrpc(
    ::grpc::ServerContext* context, flight::Reader<protocol::HandshakeRequest>* reader,
    flight::Writer<protocol::HandshakeResponse>* writer) {
  protocol::HandshakeRequest request;
  if (!reader->Read(&request)) {
    return Status::Cancelled("missing HandshakeRequest");
  }
  protocol::BasicAuth basic_auth;
  if (!basic_auth.ParseFromString(request.payload())) {
    return Status::Invalid("Invalid protocol.BasicAuth payload in handshake request");
  }
  // Check username and password...
  if (basic_auth.username() != username_ || basic_auth.password() != password_) {
    // XXX: use FlightStatusCode::Unauthenticated
    return Status::Invalid("Invalid token");
  }
  // ...and use the username as the token (a terrible idea for a real-world
  // scenario).
  protocol::HandshakeResponse response;
  response.set_protocol_version(request.protocol_version());
  response.set_payload(basic_auth.username());
  return Status::OK();
}

Status TestServerBasicAuthHandler::ValidateGrpcContext(::grpc::ServerContext* context) {
  auto maybe_token = TestServerAuthHandler::GetTokenFromContext(context);
  if (!maybe_token.has_value()) {
    return Status::Invalid("No token provided in client metadata -- 'auth-token-bin'");
  }
  if (maybe_token.value() != username_) {
    return Status::Invalid("Invalid token");
  }
  return Status::OK();
}

}  // namespace flight_test
