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

#include "arrow/flight/ng/flight_fwd.h"
#include "arrow/status.h"

namespace arrow::flight {
inline namespace ng {

template <typename R>
class Reader {
 public:
  virtual ~Reader() = default;
  virtual bool Read(R* out_value) = 0;
};

template <typename W>
class Writer {
 public:
  virtual ~Writer() = default;
  virtual bool Write(const W& value) = 0;
};

class FlightService {
 public:
  virtual ~FlightService() = default;

  /// Handshake between client and server. Depending on the server, the
  /// handshake may be required to determine the token that should be
  /// used for future operations. Both request and response are streams
  /// to allow multiple round-trips depending on auth mechanism.
  virtual Status Handshake(::grpc::ServerContext* context,
                           Reader<protocol::HandshakeRequest>* reader,
                           Writer<protocol::HandshakeResponse>* writer) = 0;

  /// Get a list of available streams given a particular criteria. Most flight
  /// services will expose one or more streams that are readily available for
  /// retrieval. This api allows listing the streams available for consumption.
  /// A user can also provide a criteria. The criteria can limit the subset of
  /// streams that can be listed via this interface. Each flight service allows
  /// its own definition of how to consume criteria.
  virtual Status ListFlights(::grpc::ServerContext* context,
                             const protocol::Criteria* criteria,
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
