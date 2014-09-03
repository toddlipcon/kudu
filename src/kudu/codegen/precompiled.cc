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

// This file contains all of the functions that must be precompiled
// to an LLVM IR format (note: not bitcode to preserve function
// names for retrieval later).
//
// Note namespace scope is just for convenient symbol resolution.
// To preserve function names, extern "C" linkage is used, so these
// functions (1) must not be duplicated in any of the above headers
// and (2) do not belong to namespace kudu.
//
// NOTE: This file may rely on external definitions from any part of Kudu
// because the code generator will resolve external symbols at load time.
// However, the code generator relies on the fact that our Kudu binaries
// are built with unstripped visible symbols, so this style of code generation
// cannot be used in builds with settings that conflict with the required
// visibility (e.g., the client library).
// NOTE: This file is NOT compiled with ASAN annotations, even if Kudu
// is being built with ASAN.

#include <cstdlib>
#include <cstring>

#include "kudu/common/rowblock.h"
#include "kudu/common/wire_protocol.pb.h"
#include "kudu/util/bitmap.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

#include "kudu/codegen/codegen_params_generated.h"

// Even though this file is only needed for IR purposes, we need to check for
// IR_BUILD because we use a fake static library target to workaround a cmake
// dependencies bug. See 'ir_fake_target' in CMakeLists.txt.
#ifdef IR_BUILD

// This file uses the 'always_inline' attribute on a bunch of functions to force
// the LLVM optimizer at runtime to inline them where it otherwise might not.
// Because the functions themselves aren't marked 'inline', gcc is unhappy with this.
// But, we can't mark them 'inline' or else they'll get optimized away and not even
// included in the .ll file. So, instead, we just mark them as always_inline in
// the IR_BUILD context.
#define IR_ALWAYS_INLINE __attribute__((always_inline))

// Workaround for an MCJIT deficiency where we see a link error when trying
// to load the JITted library. See the following LLVM bug and suggested workaround.
// https://llvm.org/bugs/show_bug.cgi?id=18062
extern "C" void *__dso_handle __attribute__((__visibility__("hidden"))) = NULL;

#else
#define IR_ALWAYS_INLINE
#endif

namespace kudu {

// Returns whether copy was successful (fails iff slice relocation fails,
// which can only occur if is_string is true).
// If arena is NULL, then no relocation occurs.
IR_ALWAYS_INLINE static bool BasicCopyCell(
    uint64_t size, const uint8_t* src, uint8_t* dst, bool is_string, Arena* arena) {
  // Relocate indirect data
  if (is_string) {
    if (PREDICT_TRUE(arena != nullptr)) {
      return PREDICT_TRUE(arena->RelocateSlice(*reinterpret_cast<const Slice*>(src),
                                               reinterpret_cast<Slice*>(dst)));
    }
    // If arena is NULL, don't relocate, but do copy the pointers to the raw
    // data (callers that pass arena as NULL should be sure that the indirect
    // data will stay alive after the projections)
  }

  // Copy direct data
  memcpy(dst, src, size);
  return true;
}

extern "C" {

// Preface all used functions with _Precompiled to avoid the possibility
// of name clashes. Notice all the nontrivial types must be passed as
// void* parameters, otherwise LLVM will complain that the type does not match
// (and it is not possible to consistently extract the llvm::Type* from a
// parsed module which has the same signature as the one that would be passed
// as a parameter for the below functions if the did not use void* types).
//
// Note that:
//   (1) There is no void* type in LLVM, instead i8* is used.
//   (2) The functions below are all prefixed with _Precompiled to avoid
//       any potential naming conflicts.

IR_ALWAYS_INLINE
bool _PrecompiledProjectRow(uint8_t*  __restrict__ src,
                            RowBlockRow* __restrict__ dst,
                            Arena* dst_arena,
                            const char* flatbuf, int64_t fb_len) {
  auto fb = flatbuffers::GetRoot<fbs::RowProjectorParam>(flatbuf);

#if 0
  flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(flatbuf), fb_len);
  CHECK(fb->Verify(v));
#endif

  const uint8_t* src_null_bitmap = src + fb->src_null_bitmap_offset();
  const RowBlock* dst_block = dst->row_block();

  // Copy directly from base Data
  #pragma unroll
  for (const auto* m : *fb->base_cols_mapping()) {
    auto dst_col_idx = m->first();
    auto src_col_idx = m->second();
    auto src_info = fb->src_cell_info()->Get(src_col_idx);
    auto size = src_info->size();

    const uint8_t* src_cell = &src[src_info->offset()];
    uint8_t* dst_cell = dst_block->column_data_base_ptr(dst_col_idx) + dst->row_index() * size;

    if (src_info->nullable()) {
      bool is_null = BitmapTest(src_null_bitmap, src_col_idx);
      dst->cell(dst_col_idx).set_null(is_null);
      if (is_null) continue;
    }
    bool is_binary = src_info->physical_type() == DataType::BINARY;
    if (!BasicCopyCell(size, src_cell, dst_cell, is_binary, dst_arena)) {
      return false;
    }
  }

  // Fill in defaults
  for (auto dst_col_idx : *fb->default_cols()) {
    auto dst_info = fb->dst_cell_info()->Get(dst_col_idx);
    auto size = dst_info->size();
    uint8_t* dst_cell = dst_block->column_data_base_ptr(dst_col_idx) + dst->row_index() * size;
    const uint8_t* default_val = reinterpret_cast<const uint8_t*>(
        fb->for_read() ? dst_info->read_default_ptr() : dst_info->write_default_ptr());

    if (dst_info->nullable()) {
      bool is_null = default_val == nullptr;
      dst->cell(dst_col_idx).set_null(is_null);
      if (is_null) continue;
    }

    if (!BasicCopyCell(size, default_val, dst_cell,
                       dst_info->physical_type() == DataType::BINARY, dst_arena)) {
      return false;
    }
  }

  return true;
}


IR_ALWAYS_INLINE
void _PrecompiledSerializeRowBlock(
    const RowBlock* rb, RowwiseRowBlockPB* pb,
    faststring* data_buf, faststring* indirect_data,
    const char* flatbuf, size_t flatbuf_len) {
  auto fb = flatbuffers::GetRoot<fbs::SerializeRowBlockParam>(flatbuf);

#if 0
  flatbuffers::Verifier v(reinterpret_cast<const uint8_t*>(flatbuf), fb_len);
  CHECK(fb->Verify(v));
#endif

  size_t old_size = data_buf->size();
  int num_rows = rb->selection_vector()->CountSelected();
  pb->set_num_rows(pb->num_rows() + num_rows);

  data_buf->resize(old_size + fb->dst_row_stride() * num_rows);
  uint8_t* base = reinterpret_cast<uint8_t*>(&(*data_buf)[old_size]);

  BitmapIterator selected_row_iter(rb->selection_vector()->bitmap(),
      rb->nrows());

  int run_size;
  bool selected;
  int row_idx = 0;
  uint8_t* dst_row = base;

  while ((run_size = selected_row_iter.Next(&selected))) {
    if (!selected) {
      row_idx += run_size;
      continue;
    }
    for (int i = 0; i < run_size; i++) {
      #pragma unroll
      for (int dst_col_idx = 0; dst_col_idx < fb->dst_cols()->size(); dst_col_idx++) {
        const auto& dst_col = fb->dst_cols()->Get(dst_col_idx);
        const auto& src_col_idx = fb->dst_to_src_idx_mapping()->Get(dst_col_idx);

        const uint8_t* src_cell = rb->column_data_base_ptr(src_col_idx) +
            dst_col->size() * row_idx;
        uint8_t* dst_cell = dst_row + dst_col->offset();

        bool is_null = false;
        if (dst_col->nullable()) {
          is_null = rb->column_block(src_col_idx).is_null(row_idx);
        }

        if (is_null) {
          memset(dst_cell, 0, dst_col->size());
        } else if (dst_col->physical_type() == DataType::BINARY) {
          const Slice *slice = reinterpret_cast<const Slice *>(src_cell);
          size_t offset_in_indirect = indirect_data->size();
          indirect_data->append(reinterpret_cast<const char*>(slice->data()),
                                slice->size());

          Slice *dst_slice = reinterpret_cast<Slice *>(dst_cell);
          *dst_slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                             slice->size());
        } else { // non-string non-null
          memcpy(dst_cell, src_cell, dst_col->size());
        }

        if (dst_col->nullable()) {
          BitmapChange(dst_row + fb->offset_to_null_bitmap(), dst_col_idx, is_null);
        }
      }
      dst_row += fb->dst_row_stride();
      row_idx++;
    }
  }
}

} // extern "C"
} // namespace kudu
