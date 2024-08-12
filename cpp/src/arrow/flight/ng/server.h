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

#include <grpcpp/security/auth_context.h>
#include <grpcpp/server_context.h>

#include "arrow/flight/ng/flight_fwd.h"
#include "arrow/flight/ng/serde.h"
#include "arrow/flight/ng/types.h"
#include "arrow/status.h"

namespace arrow::flight {
inline namespace ng {

/// \brief An authentication handler for a FlightServer.
///
/// Authentication includes both an initial negotiation and a per-call token
/// validation. Implementations may choose to use either or both mechanisms.
/// An implementation may need to track some state (e.g. a mapping of client
/// tokens to authenticated identities).
///
/// Implementations should be thread-safe as handshake and validation may be
/// called from different threads depending on the server implementation.
class ServerAuthHandler {
 public:
  virtual ~ServerAuthHandler() = default;

  /// \brief Authenticate the client on initial connection.
  ///
  /// The handler implementation can send and read responses from
  /// the client at any time.
  ///
  /// \param[in] reader The reader for messages from the client.
  /// \param[in] writer The writer for messages to the client.
  /// \return Status OK if this authentication succeeded.
  virtual Status HandshakeGrpc(::grpc::ServerContext* context,
                               Reader<protocol::HandshakeRequest>* reader,
                               Writer<protocol::HandshakeResponse>* writer) {
    return ReadAllHandshakeRequests(reader, [](uint64_t protocol_version,
                                               const std::string& payload) {
      ARROW_UNUSED(protocol_version);
      ARROW_UNUSED(payload);
      return Status::NotImplemented("ServerAuthHandler::Handshake() isn't implemented");
    });
  }

  /// \brief Validate authenticity of auth information in the gRPC context.
  ///
  /// Endpoint-specific checks like access authorization are implemented by
  /// each FlightServer method
  /// \return Status OK if the token is valid, any other status if
  ///         validation failed.
  virtual Status ValidateGrpcContext(::grpc::ServerContext* context) {
    return Status::NotImplemented("ServerAuthHandler::Validate() isn't implemented");
  }
};

/// \brief An INSECURE authentication mechanism that simply trusts any client
/// request as valid.
class TrustAuthHandler final : public ServerAuthHandler {
 public:
  ~TrustAuthHandler() override = default;

  static std::unique_ptr<TrustAuthHandler> Make() {
    return std::make_unique<TrustAuthHandler>();
  }

  Status HandshakeGrpc(::grpc::ServerContext* context,
                       Reader<protocol::HandshakeRequest>* reader,
                       Writer<protocol::HandshakeResponse>* writer) override {
    return ReadAllHandshakeRequests(
        reader, [](uint64_t protocol_version, const std::string& payload) {
          ARROW_UNUSED(protocol_version);
          ARROW_UNUSED(payload);
          return Status::OK();
        });
  }

  Status ValidateGrpcContext(::grpc::ServerContext* context) override {
    return Status::OK();
  }
};

class FlightServer {
 private:
  std::unique_ptr<ServerAuthHandler> auth_handler_;

 public:
  explicit FlightServer(std::unique_ptr<ServerAuthHandler> auth_handler);
  virtual ~FlightServer();

  ServerAuthHandler& auth_handler() { return *auth_handler_; }

  static Status ListFlightsFromIterator(FlightInfoIterator* iterator,
                                        Writer<protocol::FlightInfo>* writer);

  // FlightService interface returning arrow::Status instead of grpc::Status.
  // -------------------------------------------------------------------------

  /// Handshake between client and server. Depending on the server, the
  /// handshake may be required to determine the token that should be
  /// used for future operations. Both request and response are streams
  /// to allow multiple round-trips depending on auth mechanism.
  virtual Status Handshake(::grpc::ServerContext* context,
                           Reader<protocol::HandshakeRequest>* reader,
                           Writer<protocol::HandshakeResponse>* writer);

  /// Get a list of available streams given a particular criteria. Most flight
  /// services will expose one or more streams that are readily available for
  /// retrieval. This api allows listing the streams available for consumption.
  /// A user can also provide a criteria. The criteria can limit the subset of
  /// streams that can be listed via this interface. Each flight service allows
  /// its own definition of how to consume criteria.
  virtual Status ListFlights(::grpc::ServerContext* context,
                             const protocol::Criteria& criteria,
                             Writer<protocol::FlightInfo>* writer) = 0;

  /// For a given FlightDescriptor, get information about how the flight can be
  /// consumed. This is a useful interface if the consumer of the interface
  /// already can identify the specific flight to consume. This interface can
  /// also allow a consumer to generate a flight stream through a specified
  /// descriptor. For example, a flight descriptor might be something that
  /// includes a SQL statement or a Pickled Python operation that will be
  /// executed. In those cases, the descriptor will not be previously available
  /// within the list of available streams provided by ListFlights but will be
  /// available for consumption for the duration defined by the specific flight
  /// service.
  virtual Status GetFlightInfo(::grpc::ServerContext* context,
                               const protocol::FlightDescriptor& request,
                               protocol::FlightInfo* out_info) = 0;

  /// For a given FlightDescriptor, get the Schema as described in Schema.fbs::Schema
  /// This is used when a consumer needs the Schema of flight stream. Similar to
  /// GetFlightInfo this interface may generate a new flight that was not previously
  /// available in ListFlights.
  virtual Status GetSchema(::grpc::ServerContext* context,
                           const protocol::FlightDescriptor& request,
                           protocol::SchemaResult* out_schema) = 0;

  /// Retrieve a single stream associated with a particular descriptor
  /// associated with the referenced ticket. A Flight can be composed of one or
  /// more streams where each stream can be retrieved using a separate opaque
  /// ticket that the flight service uses for managing a collection of streams.
  virtual Status DoGet(::grpc::ServerContext* context, const protocol::Ticket& request,
                       Writer<FlightPayload>* writer) = 0;

  /// Push a stream to the flight service associated with a particular
  /// flight stream. This allows a client of a flight service to upload a stream
  /// of data. Depending on the particular flight service, a client consumer
  /// could be allowed to upload a single stream per descriptor or an unlimited
  /// number. In the latter, the service might implement a 'seal' action that
  /// can be applied to a descriptor once all streams are uploaded.
  virtual Status DoPut(::grpc::ServerContext* context, Reader<FlightPayload>* reader,
                       Writer<protocol::PutResult>* writer) = 0;

  /// Open a bidirectional data channel for a given descriptor. This
  /// allows clients to send and receive arbitrary Arrow data and
  /// application-specific metadata in a single logical stream. In
  /// contrast to DoGet/DoPut, this is more suited for clients
  /// offloading computation (rather than storage) to a Flight service.
  virtual Status DoExchange(::grpc::ServerContext* context, Reader<FlightPayload>* reader,
                            Writer<FlightPayload>* writer);

  /// Flight services can support an arbitrary number of simple actions in
  /// addition to the possible ListFlights, GetFlightInfo, DoGet, DoPut
  /// operations that are potentially available. DoAction allows a flight client
  /// to do a specific action against a flight service. An action includes
  /// opaque request and response objects that are specific to the type action
  /// being undertaken.
  virtual Status DoAction(::grpc::ServerContext* context, const protocol::Action& action,
                          Writer<protocol::Result>* writer) = 0;

  /// A flight service exposes all of the available action types that it has
  /// along with descriptions. This allows different flight consumers to
  /// understand the capabilities of the flight service.
  virtual Status ListActions(::grpc::ServerContext* context,
                             const protocol::Empty* request,
                             Writer<protocol::ActionType>* writer) = 0;
};

}  // namespace ng
}  // namespace arrow::flight
