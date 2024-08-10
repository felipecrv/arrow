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

#include "arrow/flight/ng/server.h"
#include "arrow/util/logging.h"

namespace arrow::flight {
inline namespace ng {

FlightServer::FlightServer(std::unique_ptr<ServerAuthHandler> auth_handler)
    : auth_handler_(std::move(auth_handler)) {
  DCHECK(auth_handler_);
}

FlightServer::~FlightServer() = default;

Status FlightServer::Handshake(::grpc::ServerContext* context,
                               Reader<protocol::HandshakeRequest>* reader,
                               Writer<protocol::HandshakeResponse>* writer) {
  return auth_handler_->Handshake(context, reader, writer);
}

Status FlightServer::ListFlights(::grpc::ServerContext* context,
                                 const protocol::Criteria* criteria,
                                 Writer<protocol::FlightInfo>* writer) {
  RETURN_NOT_OK(auth_handler_->Validate(context));
  return Status::OK();
}

Status FlightServer::GetFlightInfo(::grpc::ServerContext* context,
                                   const protocol::FlightDescriptor& request,
                                   protocol::FlightInfo* out_info) {
  return Status::NotImplemented("FlightServer::GetFlightInfo()");
}

Status FlightServer::GetSchema(::grpc::ServerContext* context,
                               const protocol::FlightDescriptor& request,
                               protocol::SchemaResult* out_schema) {
  return Status::NotImplemented("FlightServer::GetSchema()");
}

Status FlightServer::DoGet(::grpc::ServerContext* context,
                           const protocol::Ticket& request,
                           Writer<FlightPayload>* writer) {
  return Status::NotImplemented("FlightServer::DoGet()");
}

Status FlightServer::DoPut(::grpc::ServerContext* context, Reader<FlightPayload>* reader,
                           Writer<protocol::PutResult>* writer) {
  return Status::NotImplemented("FlightServer::DoPut()");
}

Status FlightServer::DoExchange(::grpc::ServerContext* context,
                                Reader<FlightPayload>* reader,
                                Writer<FlightPayload>* writer) {
  return Status::NotImplemented("FlightServer::DoExchange()");
}

Status FlightServer::DoAction(::grpc::ServerContext* context,
                              const protocol::Action& action,
                              Writer<protocol::Result>* writer) {
  return Status::NotImplemented("FlightServer::DoAction()");
}

Status FlightServer::ListActions(::grpc::ServerContext* context,
                                 const protocol::Empty* request,
                                 Writer<protocol::ActionType>* writer) {
  return Status::NotImplemented("FlightServer::ListActions()");
}

}  // namespace ng
}  // namespace arrow::flight
