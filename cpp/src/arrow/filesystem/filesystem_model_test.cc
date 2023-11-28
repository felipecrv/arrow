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

#include <set>
#include <string>

#include <gtest/gtest.h>

#include "arrow/filesystem/filesystem.h"
#include "arrow/result.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/io_util.h"
#include "arrow/util/logging.h"

// Model-based testing [1] for filesystem implementations.
//
// [1] https://en.wikipedia.org/wiki/Model-based_testing

namespace arrow::fs {

namespace internal {

struct AllPaths {
  const std::vector<std::string> dir_names = {"abra", "abracadabra", "dir"};
  const std::vector<std::string> subdir_names = {"abra", "abracadabra", "dir", "subdir"};
  const std::vector<std::string> file_names = {"abra", "abracadabra", "file1.txt"};

  // Let DIR, SUBDIR and FILE be the regular expressions matching the arrays above,
  // then the set of all possible paths used in the tests is represented by the
  // following grammar (in EBNF):
  //
  //     path = dir | file;
  //
  //     dir =
  //         | DIR, '/'
  //         | DIR, '/', SUBDIR, '/'
  //         | DIR, '/', SUBDIR, '/', SUBDIR, '/'
  //         ;
  //
  //     file = dir, FILE;
  std::vector<std::string> all_paths;

  AllPaths() : all_paths(EnumeratePaths()) {}

 private:
  std::vector<std::string> EnumeratePaths() {
    // All directory paths without a trailing slash.
    std::vector<std::string> dirs;
    {
      auto dir1 = dir_names;
      auto dir2 = ExtendPaths(dir1, subdir_names);
      auto dir3 = ExtendPaths(dir2, subdir_names);
      // dirs = dir1 ++ dir2 ++ dir3;
      dirs.reserve(dir1.size() + dir2.size() + dir3.size());
      std::move(dir1.begin(), dir1.end(), std::back_inserter(dirs));
      std::move(dir2.begin(), dir2.end(), std::back_inserter(dirs));
      std::move(dir3.begin(), dir3.end(), std::back_inserter(dirs));
    }

    std::vector<std::string> paths;
    {
      // extend directory paths with a trailing slash
      paths = ExtendPaths(dirs, {""});
      // include file paths
      auto files = ExtendPaths(dirs, file_names);
      std::move(files.begin(), files.end(), std::back_inserter(paths));
    }
    DCHECK_EQ(paths.size(), 270);
    return paths;
  }

  std::vector<std::string> ExtendPaths(const std::vector<std::string>& paths,
                                       const std::vector<std::string>& names) {
    std::vector<std::string> ret;
    ret.reserve(paths.size() * names.size());
    for (const auto& path : paths) {
      for (const auto& name : names) {
        ret.push_back(path + "/" + name);
      }
    }
    return ret;
  }
};

struct State {
  FileSystem& fs;
  std::set<std::string> mirrored_paths;

  void MirrorPath(const std::string& path) { mirrored_paths.insert(path); }

  void DropPath(const std::string& path) { mirrored_paths.erase(path); }
};

struct Action {
  enum {
    // Create a directory non-recursively.
    //
    // Pre-conditions:
    // - The parent directory exists.
    // - The directory does not exist.
    CREATE_DIR,

    // Create a file.
    CREATE_FILE,
  } type;
  std::string path_arg;

  // Return a string describing this action.
  std::string Description() const;

  // Execute this action.
  Status Execute(State& state);

 private:
  Status DirectoryExists(State& state, const std::string& path) {
    ARROW_ASSIGN_OR_RAISE(auto info, state.fs.GetFileInfo(path));
    if (info.IsFile()) {
      return Status::Invalid(path + " is a file.");
    }
    return Status::OK();
  }

  Status IsEmptyDirectory(State& state, std::string dir_path, bool allow_not_found) {
    FileSelector selector;
    selector.base_dir = std::move(dir_path);
    selector.allow_not_found = false;
    selector.recursive = false;
    selector.max_recursion = 0;
    ARROW_ASSIGN_OR_RAISE(auto children, state.fs.GetFileInfo(selector));
    return children.empty() ? Status::OK() : Status::Invalid(dir_path + " is not empty.");
  }
};

class TransitionSystem {
 public:
};

std::string TransitionSystem::Action::Description() const {
  switch (type) {
    case CREATE_DIR:
      return "CreateDir(" + path_arg + " /*recursive=*/false)";
    case CREATE_FILE:
      return "CreateFile(" + path_arg + ")";
  }
}

Status TransitionSystem::Action::Execute(State& state) {
  switch (type) {
    case CREATE_DIR:
      RETURN_NOT_OK(state.fs.CreateDir(path_arg, /*recursive=*/false));
      state.MirrorPath(path_arg);
      return Status::OK();
    case CREATE_FILE:
      ARROW_ASSIGN_OR_RAISE(auto output, state.fs.OpenOutputStream(path_arg));
      RETURN_NOT_OK(output->Close());
      state.MirrorPath(path_arg);
      return Status::OK();
  }
}

}  // namespace internal

class FilesystemModelTest : public ::testing::Test {
 protected:
};

}  // namespace arrow::fs
