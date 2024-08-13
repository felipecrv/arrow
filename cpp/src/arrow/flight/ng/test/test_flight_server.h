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

// Example server implementation to use for unit testing and benchmarking
// purposes.

#include "arrow/flight/ng/server.h"

namespace flight_test {

class TestFlightServer : public arrow::flight::FlightServer {
 public:
  class AuthHandler : public arrow::flight::ServerAuthHandler {
   public:
    // TODO(felipecrv): Implement the authentication logic.
  };

  TestFlightServer();
  ~TestFlightServer() override;

  // TODO(felipecrv): Implement the Flight server logic
};

}  // namespace flight_test
