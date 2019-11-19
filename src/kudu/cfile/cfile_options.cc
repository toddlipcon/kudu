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

#include "kudu/cfile/cfile_options.h"

#include <boost/optional/optional.hpp>

#include "kudu/util/mem_tracker.h"

namespace kudu {
namespace cfile {

WriterOptions::WriterOptions()
  : index_block_size(32*1024),
    block_restart_interval(16),
    write_posidx(false),
    write_validx(false),
    optimize_index_keys(true),
    validx_key_encoder(boost::none) {
}

ReaderOptions::ReaderOptions()
  : parent_mem_tracker(MemTracker::GetRootTracker()) {
}


}
}
