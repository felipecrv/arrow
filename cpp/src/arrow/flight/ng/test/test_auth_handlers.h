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

#include <optional>
#include <string>

#include "arrow/flight/ng/server.h"
#include "arrow/flight/ng/test/flight_test_fwd.h"
#include "arrow/status.h"

namespace flight_test {

class ARROW_FLIGHT_EXPORT TestServerAuthHandler : public flight::ServerAuthHandler {
 private:
  std::string username_;
  std::string token_;

 public:
  TestServerAuthHandler(std::string username, std::string password);
  ~TestServerAuthHandler() override;

  Status HandshakeGrpc(::grpc::ServerContext* context,
                       flight::Reader<protocol::HandshakeRequest>* reader,
                       flight::Writer<protocol::HandshakeResponse>* writer) override;

  Status ValidateGrpcContext(::grpc::ServerContext* context) override;

  static std::optional<std::string_view> GetTokenFromContext(
      ::grpc::ServerContext* context);
};

class ARROW_FLIGHT_EXPORT TestServerBasicAuthHandler : public flight::ServerAuthHandler {
 public:
  explicit TestServerBasicAuthHandler(std::string username, std::string password);
  ~TestServerBasicAuthHandler() override;

  Status HandshakeGrpc(::grpc::ServerContext* context,
                       flight::Reader<protocol::HandshakeRequest>* reader,
                       flight::Writer<protocol::HandshakeResponse>* writer) override;

  Status ValidateGrpcContext(::grpc::ServerContext* context) override;

 private:
  // Fields from protocol::BasicAuth
  std::string username_;
  std::string password_;
};

}  // namespace flight_test
