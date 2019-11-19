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

#include <functional>
#include <iostream>
#include <memory>

#include "kudu/util/status.h"

namespace kudu {
namespace cfile {

class CFileReader;
class CFileIterator;

// Used to set the CFileFooterPB bitset tracking incompatible features
enum IncompatibleFeatures {
  NONE = 0,

  // Write a crc32 checksum at the end of each cfile block
  CHECKSUM = 1 << 0,

  SUPPORTED = NONE | CHECKSUM
};


// Dumps the contents of a cfile to 'out'; 'reader' and 'iterator'
// must be initialized. If 'num_rows' is 0, all rows will be printed.
Status DumpIterator(const CFileReader& reader,
                    CFileIterator* it,
                    std::ostream* out,
                    int num_rows,
                    int indent);

}  // namespace cfile
}  // namespace kudu
