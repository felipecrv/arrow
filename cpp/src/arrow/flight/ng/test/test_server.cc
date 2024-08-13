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

#include <csignal>
#include <cstdio>
#include <memory>

#include <gflags/gflags.h>

#include "arrow/flight/ng/grpc_server.h"
#include "arrow/flight/ng/test/test_flight_server.h"
#include "arrow/util/io_util.h"

std::shared_ptr<::grpc::Server> g_server = nullptr;

void HandleSignal(int signum) {
  switch (signum) {
    case SIGTERM:
    case SIGINT:
      if (g_server) {
        printf("Shutting down...");
        fflush(stdout);
        g_server->Shutdown();
        puts("");
      }
      break;
  }
}

DEFINE_int32(port, 31337, "Server port to listen on");
DEFINE_string(unix, "", "Unix socket path to listen on");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto flight_server =
      std::make_unique<flight_test::TestFlightServer>("flight-user", "token123");

  // Build the server address for gRPC.
  std::string server_address;
  if (FLAGS_unix.empty()) {
    server_address = "0.0.0.0:" + std::to_string(FLAGS_port);
  } else {
    server_address = "unix:" + FLAGS_unix;
  }
  // Alternatively, ::grpc::SslServerCredentials could be used.
  auto server_credentials = ::grpc::InsecureServerCredentials();
  // The server is instantiated by injecting the FlightServer implementation
  // and gRPC-specific server configuration.
  g_server = BuildAndStartGrpcServer(std::move(flight_server), server_address,
                                     std::move(server_credentials));
  if (!g_server) {
    fprintf(stderr, "Server did not start properly due an unknown error.\n");
    return EXIT_FAILURE;
  }
  printf(
      "Server listening on:\n"
      "    %s\n"
      "Use Control-C to stop this server and shut down all connections.\n",
      server_address.c_str());
  fflush(stdout);

  // Exit cleanly on some signals
#if ARROW_HAVE_SIGACTION
  struct sigaction sa;
  sa.sa_handler = HandleSignal;
  sa.sa_flags = 0;
  sigemptyset(&sa.sa_mask);
  if (sigaction(SIGINT, &sa, nullptr) < 0 || sigaction(SIGTERM, &sa, nullptr) < 0) {
    perror("Error setting up signal handler.");
    return EXIT_FAILURE;
  }
#endif

  // Wait until the server is shut down.
  g_server->Wait();
  return EXIT_SUCCESS;
}
