# Arrow Flight SQL

Flight SQL is a standard defined on top of Flight that allows clients to send
SQL queries to a server and receive the results as Arrow data.

Control messages between Flight SQL clients and servers are defined in terms of
a Protocol Buffers spec. As Flight itself defines a fixed set of control
messages in terms of Protocol Buffers [1], Flight SQL messages must be wrapped
and unwrapped as Flight messages. `google.protobuf.Any` is used to wrap the
Flight SQL messages into the smaller set of Flight messages.

## Development Notes

This directory contains code implementing Flight SQL clients and stubs for the
Flight SQL server implementations.

**This is a prototype** that differs very much from how `flight/sql` was done.
All the symbols (except for protocol messages) are in the
`arrow::flight::sql::ng` namespace.

Most of the code in the prototype is automatically generated from a structured
definition of the Flight SQL protocol instead of being hand-written.

    cpp/src/arrow/flight
    [ ] ├── api.h
    [x] ├── ArrowFlightConfig.cmake.in
    [x] ├── arrow-flight.pc.in
    [x] ├── ArrowFlightTestingConfig.cmake.in
    [x] ├── arrow-flight-testing.pc.in
    [ ] ├── client_auth.h
    [ ] ├── client.cc
    [ ] ├── client_cookie_middleware.cc
    [ ] ├── client_cookie_middleware.h
    [ ] ├── client.h
    [ ] ├── client_middleware.h
    [ ] ├── client_tracing_middleware.cc
    [ ] ├── client_tracing_middleware.h
    [ ] ├── CMakeLists.txt
    [ ] ├── cookie_internal.cc
    [ ] ├── cookie_internal.h
    [ ] ├── flight_benchmark.cc
    [ ] ├── flight_internals_test.cc
    [ ] ├── flight_test.cc
    [ ] ├── integration_tests
    [ ] │   ├── CMakeLists.txt
    [ ] │   ├── flight_integration_test.cc
    [ ] │   ├── test_integration.cc
    [ ] │   ├── test_integration_client.cc
    [ ] │   ├── test_integration.h
    [ ] │   └── test_integration_server.cc
    [ ] ├── middleware.cc
    [ ] ├── middleware.h
    [ ] ├── otel_logging.cc
    [ ] ├── otel_logging.h
    [ ] ├── otel_logging_internal.h
    [ ] ├── pch.h
    [ ] ├── perf.proto
    [ ] ├── perf_server.cc
    [ ] ├── platform.h
    [ ] ├── protocol_internal.h
    [ ] ├── README.md
    [ ] ├── serialization_internal.cc
    [ ] ├── serialization_internal.h
    [ ] ├── server_auth.cc
    [ ] ├── server_auth.h
    [ ] ├── server.cc
    [ ] ├── server.h
    [ ] ├── server_middleware.cc
    [ ] ├── server_middleware.h
    [ ] ├── server_tracing_middleware.cc
    [ ] ├── server_tracing_middleware.h
    [ ] ├── sql
    [ ] │   ├── acero_test.cc
    [ ] │   ├── api.h
    [ ] │   ├── ArrowFlightSqlConfig.cmake.in
    [ ] │   ├── arrow-flight-sql.pc.in
    [ ] │   ├── client.cc
    [ ] │   ├── client.h
    [ ] │   ├── CMakeLists.txt
    [ ] │   ├── column_metadata.cc
    [ ] │   ├── column_metadata.h
    [ ] │   ├── protocol_internal.cc
    [ ] │   ├── protocol_internal.h
    [ ] │   ├── server.cc
    [ ] │   ├── server.h
    [ ] │   ├── server_session_middleware.cc
    [ ] │   ├── server_session_middleware_factory.h
    [ ] │   ├── server_session_middleware.h
    [ ] │   ├── server_session_middleware_internals_test.cc
    [ ] │   ├── server_test.cc
    [ ] │   ├── sql_info_internal.cc
    [ ] │   ├── sql_info_internal.h
    [ ] │   ├── test_app_cli.cc
    [ ] │   ├── test_server_cli.cc
    [ ] │   ├── types.h
    [ ] │   └── visibility.h
    [ ] ├── test_definitions.cc
    [ ] ├── test_definitions.h
    [ ] ├── test_server.cc
    [ ] ├── test_util.cc
    [ ] ├── test_util.h
    [ ] ├── transport
    [ ] │   ├── grpc
    [x] │   │   ├── customize_grpc.h
    [ ] │   │   ├── grpc_client.cc
    [ ] │   │   ├── grpc_client.h
    [ ] │   │   ├── grpc_server.cc
    [x] │   │   ├── grpc_server.h
    [ ] │   │   ├── protocol_grpc_internal.cc
    [ ] │   │   ├── protocol_grpc_internal.h
    [ ] │   │   ├── serialization_internal.cc
    [?] │   │   ├── serialization_internal.h
    [ ] │   │   ├── util_internal.cc
    [ ] │   │   └── util_internal.h
    [ ] │   └── ucx
    [ ] │       ├── CMakeLists.txt
    [ ] │       ├── flight_transport_ucx_test.cc
    [ ] │       ├── ucx.cc
    [ ] │       ├── ucx_client.cc
    [ ] │       ├── ucx.h
    [ ] │       ├── ucx_internal.cc
    [ ] │       ├── ucx_internal.h
    [ ] │       ├── ucx_server.cc
    [ ] │       ├── util_internal.cc
    [ ] │       └── util_internal.h
    [ ] ├── transport.cc
    [ ] ├── transport.h
    [ ] ├── transport_server.cc
    [ ] ├── transport_server.h
    [ ] ├── type_fwd.h
    [ ] ├── types_async.h
    [ ] ├── types.cc
    [ ] ├── types.h
    [x] └── visibility.h  (included from ng/)


[1] https://arrow.apache.org/docs/format/FlightSql.html
