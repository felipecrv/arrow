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

#include "arrow/flight/ng/serde.h"
#include "arrow/flight/visibility.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"

namespace arrow::flight {
inline namespace ng {

class ARROW_FLIGHT_EXPORT Iterator {
 public:
  virtual ~Iterator() = default;
  ARROW_DISALLOW_COPY_AND_ASSIGN(Iterator);

  /// \brief Return true if the iterator is ready to generate values and no
  /// error has occurred.
  ///
  /// An Iterator is either ready to generate values, finished, or has
  /// encountered an error. When false is returned, status() must be checked
  /// to determine if the producer is finished or if an error occurred.
  virtual bool Valid() const = 0;

  /// \brief If an error has occurred, return it. Else return an OK status.
  virtual Status status() const = 0;
};

/// \brief An Iterator that produces values of type T and writes them into a
/// Writer<T>.
///
///     auto it = GetIterator();
///     while (it->Valid()) {
///       if (!it->Next(writer)) {
///         // writer.Write() returned false, indicating an error or that
///         // the writer is no longer interested in receiving  more values.
///         break;
///       }
///     }
///     RETURN_NOT_OK(it->status());
template <typename T>
class ARROW_FLIGHT_EXPORT TypedIterator : public Iterator {
 public:
  ~TypedIterator() override = default;

  /// \brief Retrieve the next T from this iterator.
  ///
  /// If it returns false, call the status() method to determine if the
  /// iterator (or writer) is finished or if an error occurred.
  ///
  /// \pre Valid() must return true.
  /// \param writer The writer to write the next T to.
  /// \return true if the writer accepted the value, false if the writer
  ///         is no longer interested in receiving values or an error
  ///         occurred.
  virtual bool Next(Writer<T>* writer) = 0;

  /// \brief Convenience method that takes a lambda instead of a Writer<T>.
  ///
  /// If it returns false, call the status() method to determine if the
  /// iterator (or writer) is finished or if an error occurred.
  ///
  /// \pre Valid() must return true.
  /// \param listener A lambda that takes a T and returns false when it is no longer
  ///                 interested in receiving values.
  /// \return true if the writer listener returned true, false if the listener
  ///         returned false or an error occurred while producing the value.
  template <typename Listener>
  bool Next(Listener listener) {
    class WriterAdapter final : public Writer<T> {
     private:
      Listener listener_;

     public:
      explicit WriterAdapter(Listener listener) : listener_(std::move(listener)) {}
      bool Write(const protocol::FlightInfo& value) override { return listener_(value); }
    };
    WriterAdapter writer_adapter(std::move(listener));
    const bool wants_more = Next(&writer_adapter);
    return wants_more && status().ok();
  }
};

}  // namespace ng
}  // namespace arrow::flight
