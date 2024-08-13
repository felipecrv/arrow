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

#include <cassert>
#include <string>

#include <grpcpp/grpcpp.h>

#include "arrow/flight/Flight.grpc.pb.h"  // XXX: to be removed
#include "arrow/flight/ng/grpc_serde.h"
#include "arrow/flight/ng/server.h"
#include "arrow/util/string.h"

namespace arrow::flight {
inline namespace ng {

/// \brief Convert an Arrow status to a gRPC status.
inline ::grpc::Status ToRawGrpcStatus(::arrow::Status arrow_status) {
  if (arrow_status.ok()) {
    return ::grpc::Status::OK;
  }
  auto grpc_code = ::grpc::StatusCode::UNKNOWN;
  switch (arrow_status.code()) {
    case StatusCode::OK:
      grpc_code = ::grpc::StatusCode::OK;
      break;
    case StatusCode::KeyError:
      grpc_code = ::grpc::StatusCode::NOT_FOUND;
      break;
    case StatusCode::Invalid:
      grpc_code = ::grpc::StatusCode::INVALID_ARGUMENT;
      break;
    case StatusCode::Cancelled:
      grpc_code = ::grpc::StatusCode::CANCELLED;
      break;
    case StatusCode::NotImplemented:
      grpc_code = ::grpc::StatusCode::UNIMPLEMENTED;
      break;
    case StatusCode::AlreadyExists:
      grpc_code = ::grpc::StatusCode::ALREADY_EXISTS;
      break;
    case StatusCode::UnknownError:
      grpc_code = ::grpc::StatusCode::UNKNOWN;
      break;
    // StatusCode::OutOfMemory = 1
    // StatusCode::TypeError = 3
    // StatusCode::IOError = 5
    // StatusCode::CapacityError = 6
    // StatusCode::IndexError = 7
    // StatusCode::SerializationError = 11
    default:
      return {::grpc::StatusCode::UNKNOWN, arrow_status.ToString()};
  }

  std::string message = arrow_status.message();
  if (arrow_status.detail()) {
    message += ". Detail: ";
    message += arrow_status.detail()->ToString();
  }
  return {grpc_code, std::move(message)};
}

/// \brief Convert an Arrow status to a gRPC status with extra headers.
///
/// The extra headers are added to the response to encode the original Arrow status.
inline ::grpc::Status ToGrpcStatus(const Status& arrow_status,
                                   ::grpc::ServerContext* ctx) {
  static const char* kGrpcStatusCodeHeader = "x-arrow-status";
  static const char* kGrpcStatusMessageHeader = "x-arrow-status-message-bin";
  static const char* kGrpcStatusDetailHeader = "x-arrow-status-detail-bin";
  // static const char* kBinaryErrorDetailsKey = "grpc-status-details-bin";

  assert(ctx);  // XXX: might remove later and make ctx optional
  ::grpc::Status status = ToRawGrpcStatus(arrow_status);
  if (status.ok() || !ctx) {
    return status;
  }
  const std::string code = internal::ToChars(static_cast<int>(arrow_status.code()));
  ctx->AddTrailingMetadata(kGrpcStatusCodeHeader, code);
  ctx->AddTrailingMetadata(kGrpcStatusMessageHeader, arrow_status.message());
  if (arrow_status.detail()) {
    const std::string detail_string = arrow_status.detail()->ToString();
    ctx->AddTrailingMetadata(kGrpcStatusDetailHeader, detail_string);
  }
  // auto fsd = FlightStatusDetail::UnwrapStatus(arrow_status);
  // if (fsd && !fsd->extra_info().empty()) {
  //   ctx->AddTrailingMetadata(kBinaryErrorDetailsKey, fsd->extra_info());
  // }
  return status;
}

#define GRPC_RETURN_NOT_OK(expr)                         \
  do {                                                   \
    ::arrow::Status _s = (expr);                         \
    if (ARROW_PREDICT_FALSE(!_s.ok())) {                 \
      return ::arrow::flight::ToGrpcStatus(_s, context); \
    }                                                    \
  } while (0)

#define GRPC_RETURN_NOT_GRPC_OK(expr)    \
  do {                                   \
    ::grpc::Status _s = (expr);          \
    if (ARROW_PREDICT_FALSE(!_s.ok())) { \
      return _s;                         \
    }                                    \
  } while (0)

template <typename R>
class GrpcReader : public flight::Reader<R> {
 private:
  ::grpc::internal::ReaderInterface<R>* reader_;

 public:
  explicit GrpcReader(::grpc::internal::ReaderInterface<R>* reader) : reader_(reader) {}
  ~GrpcReader() override = default;
  bool Read(R* out_value) override { return reader_->Read(out_value); }
};

template <typename W>
class GrpcWriter : public flight::Writer<W> {
 private:
  ::grpc::internal::WriterInterface<W>* writer_;

 public:
  explicit GrpcWriter(::grpc::internal::WriterInterface<W>* writer) : writer_(writer) {}
  ~GrpcWriter() override = default;
  bool Write(const W& value) override { return writer_->Write(value); }
};

template <>
class GrpcReader<FlightPayload> : public flight::Reader<FlightPayload> {
 private:
  ::grpc::internal::ReaderInterface<protocol::FlightData>* reader_;

 public:
  explicit GrpcReader(::grpc::internal::ReaderInterface<protocol::FlightData>* reader)
      : reader_(reader) {}

  ~GrpcReader() override = default;

  bool Read(FlightPayload* out_value) override {
    auto* out_as_flight_data = reinterpret_cast<protocol::FlightData*>(out_value);
    return reader_->Read(out_as_flight_data);
  }
};

template <>
class GrpcWriter<FlightPayload> : public flight::Writer<FlightPayload> {
 private:
  ::grpc::internal::WriterInterface<protocol::FlightData>* writer_;

 public:
  explicit GrpcWriter(::grpc::internal::WriterInterface<protocol::FlightData>* writer)
      : writer_(writer) {}

  ~GrpcWriter() override = default;

  bool Write(const FlightPayload& value) override {
    auto& value_as_flight_data = reinterpret_cast<const protocol::FlightData&>(value);
    return writer_->Write(value_as_flight_data);
  }
};

/// \brief gRPC Flight server implementation.
class GrpcFlightServer : public protocol::FlightService::Service {
 private:
  std::unique_ptr<FlightServer> impl_;

 public:
  explicit GrpcFlightServer(std::unique_ptr<FlightServer> impl)
      : impl_(std::move(impl)) {}

  ::grpc::Status Handshake(
      ::grpc::ServerContext* context,
      ::grpc::ServerReaderWriter<protocol::HandshakeResponse, protocol::HandshakeRequest>*
          stream) override {
    GrpcReader<protocol::HandshakeRequest> reader(stream);
    GrpcWriter<protocol::HandshakeResponse> writer(stream);
    GRPC_RETURN_NOT_OK(impl_->Handshake(context, &reader, &writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status ListFlights(
      ::grpc::ServerContext* context, const protocol::Criteria* request,
      ::grpc::ServerWriter<protocol::FlightInfo>* writer) override {
    GrpcWriter<protocol::FlightInfo> response_writer(writer);
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->ListFlights(context, *request, &response_writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status GetFlightInfo(::grpc::ServerContext* context,
                               const protocol::FlightDescriptor* request,
                               protocol::FlightInfo* response) override {
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->GetFlightInfo(context, *request, response));
    return ::grpc::Status::OK;
  }

  ::grpc::Status PollFlightInfo(::grpc::ServerContext* context,
                                const protocol::FlightDescriptor* request,
                                protocol::PollInfo* response) override {
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->PollFlightInfo(context, *request, response));
    return ::grpc::Status::OK;
  }

  ::grpc::Status GetSchema(::grpc::ServerContext* context,
                           const protocol::FlightDescriptor* request,
                           protocol::SchemaResult* response) override {
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->GetSchema(context, *request, response));
    return ::grpc::Status::OK;
  }

  ::grpc::Status DoGet(::grpc::ServerContext* context, const protocol::Ticket* request,
                       ::grpc::ServerWriter<protocol::FlightData>* writer) override {
    GrpcWriter<FlightPayload> response_writer(writer);
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->DoGet(context, *request, &response_writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status DoPut(
      ::grpc::ServerContext* context,
      ::grpc::ServerReaderWriter<protocol::PutResult, protocol::FlightData>* stream)
      override {
    GrpcReader<FlightPayload> reader(stream);
    GrpcWriter<protocol::PutResult> writer(stream);
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->DoPut(context, &reader, &writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status DoExchange(
      ::grpc::ServerContext* context,
      ::grpc::ServerReaderWriter<protocol::FlightData, protocol::FlightData>* stream)
      override {
    GrpcReader<FlightPayload> reader(stream);
    GrpcWriter<FlightPayload> writer(stream);
    GRPC_RETURN_NOT_OK(impl_->DoExchange(context, &reader, &writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status DoAction(::grpc::ServerContext* context, const protocol::Action* request,
                          ::grpc::ServerWriter<protocol::Result>* writer) override {
    GrpcWriter<protocol::Result> response_writer(writer);
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->DoAction(context, *request, &response_writer));
    return ::grpc::Status::OK;
  }

  ::grpc::Status ListActions(
      ::grpc::ServerContext* context, const protocol::Empty* request,
      ::grpc::ServerWriter<protocol::ActionType>* writer) override {
    GrpcWriter<protocol::ActionType> response_writer(writer);
    GRPC_RETURN_NOT_OK(impl_->auth_handler().ValidateGrpcContext(context));
    GRPC_RETURN_NOT_OK(impl_->ListActions(context, request, &response_writer));
    return ::grpc::Status::OK;
  }
};

}  // namespace ng
}  // namespace arrow::flight
