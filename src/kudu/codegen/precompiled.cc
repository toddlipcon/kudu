// Copyright 2014 Cloudera inc.
// Confidential Cloudera Information: Covered by NDA.

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
#include "kudu/util/bitmap.h"
#include "kudu/util/faststring.h"
#include "kudu/util/memory/arena.h"
#include "kudu/util/slice.h"

// Even though this file is only needed for IR purposes, we need to check for
// IR_BUILD because we use a fake static library target to workaround a cmake
// dependencies bug. See 'ir_fake_target' in CMakeLists.txt.
#ifdef IR_BUILD
// Workaround for an MCJIT deficiency where we see a link error when trying
// to load the JITted library. See the following LLVM bug and suggested workaround.
// https://llvm.org/bugs/show_bug.cgi?id=18062
extern "C" void *__dso_handle __attribute__((__visibility__("hidden"))) = NULL;
#endif

namespace kudu {

// Returns whether copy was successful (fails iff slice relocation fails,
// which can only occur if is_string is true).
// If arena is NULL, then no relocation occurs.
static bool BasicCopyCell(uint64_t size, uint8_t* src, uint8_t* dst,
                          bool is_string, Arena* arena) {
  // Relocate indirect data
  if (is_string) {
    if (PREDICT_TRUE(arena != NULL)) {
      return PREDICT_TRUE(arena->RelocateSlice(*reinterpret_cast<Slice*>(src),
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


// declare i1 @_PrecompiledCopyCellToRowBlock(
//   i64 size, i8* src, RowBlockRow* dst, i64 col, i1 is_string, Arena* arena)
//
//   Performs the same function as CopyCell, copying size bytes of the
//   cell pointed to by src to the cell of column col in the row pointed
//   to by dst, copying indirect data to the parameter arena if is_string
//   is true. Will hard crash if insufficient memory is available for
//   relocation. Copies size bytes directly from the src cell.
//   If arena is NULL then only the direct copy will occur.
//   Returns whether successful. If not, out-of-memory during relocation of
//   slices has occured, which can only happen if is_string is true.
bool _PrecompiledCopyCellToRowBlock(uint64_t size, uint8_t* src, RowBlockRow* dst,
                                    uint64_t col, bool is_string, Arena* arena) {

  // We manually compute the destination cell pointer here, rather than
  // using dst->cell_ptr(), since we statically know the size of the column
  // type. Using the normal access path would generate an 'imul' instruction,
  // since it would be loading the column type info from the RowBlock object
  // instead of our static parameter here.
  size_t idx = dst->row_index();
  const RowBlock* block = dst->row_block();
  uint8_t* dst_cell = block->column_data_base_ptr(col) + idx * size;
  return BasicCopyCell(size, src, dst_cell, is_string, arena);
}

// declare i1 @_PrecompiledCopyCellToRowBlockNullable(
//   i64 size, i8* src, RowBlockRow* dst, i64 col, i1 is_string, Arena* arena,
//   i8* src_bitmap, i64 bitmap_idx)
//
//   Performs the same function as _PrecompiledCopyCellToRowBlock but for nullable
//   columns. Checks the parameter bitmap at the specified index and updates
//   The row's bitmap accordingly. Then goes on to copy the cell over if it
//   is not null.
//   If arena is NULL then only the direct copy will occur (if the source
//   bitmap indicates the cell itself is non-null).
//   Returns whether successful. If not, out-of-memory during relocation of
//   slices has occured, which can only happen if is_string is true.
bool _PrecompiledCopyCellToRowBlockNullable(
  uint64_t size, uint8_t* src, RowBlockRow* dst, uint64_t col, bool is_string,
  Arena* arena, uint8_t* src_bitmap, uint64_t bitmap_idx) {
  // Using this method implies the nullablity of the column.
  // Write whether the column is nullable to the RowBlock's ColumnBlock's bitmap
  bool is_null = BitmapTest(src_bitmap, bitmap_idx);
  dst->cell(col).set_null(is_null);
  // No more copies necessary if null
  if (is_null) return true;
  return _PrecompiledCopyCellToRowBlock(size, src, dst, col, is_string, arena);
}

// declare void @_PrecompiledSetRowBlockCellSetNull(
//   RowBlockRow* %dst, i64 <column index>, i1 %is_null)
//
//   Sets the cell at column 'col' for destination RowBlockRow 'dst'
//   to be marked as 'is_null' (requires the column is nullable).
void _PrecompiledCopyCellToRowBlockSetNull(
  RowBlockRow* dst, uint64_t col, bool is_null) {
  dst->cell(col).set_null(is_null);
}

// declare void @_PrecompiledCopyColumn(
//   RowBlock* rb, i8* dst_base, faststring* indirect, i64 col_idx,
//   i64 dst_col_idx, i64 row_stride, i64 offset_to_dst_col,
//   i64 offset_to_null_bitmap, i64 cell_size, i1 is_nullable, i1 is_string)
//
//   Copies column 'col_idx' from RowBlock 'rb' to the destination column
//   'dst_col_idx' (copied with rowwise orientation using 'row_stride')
//   into 'dst_base', moving all indirect data over to 'indirect_data' and
//   using 'offset_to_null_bitmap' to access each row's null bitmap if
//   is_string or is_nullable are true, respectively.
void _PrecompiledCopyColumn(
  RowBlock* rb, uint8_t* dst_base, faststring* indirect, uint64_t col_idx,
  uint64_t dst_col_idx, uint64_t row_stride, uint64_t offset_to_dst_col,
  uint64_t offset_to_null_bitmap, uint64_t cell_size, bool is_nullable,
  bool is_string) {
  // We can use the ColumnBlock to read null cells but we don't want to use
  // it for accessing cells because it won't be using 'cell_size' to do so.
  ColumnBlock cblock = rb->column_block(col_idx);
  uint8_t* src_cell = rb->column_data_base_ptr(col_idx);

  uint8_t* dst_cell = dst_base + offset_to_dst_col;
  uint8_t* dst_bitmap = dst_base + offset_to_null_bitmap;

  BitmapIterator selected_row_iter(rb->selection_vector()->bitmap(),
                                   rb->nrows());

  int run_size;
  bool selected;
  int row_idx = 0;
  while ((run_size = selected_row_iter.Next(&selected))) {
    if (!selected) {
      src_cell += run_size * cell_size;
      row_idx += run_size;
      continue;
    }
    for (int i = 0; i < run_size; ++i, dst_cell += row_stride,
             dst_bitmap += row_stride, src_cell += cell_size, ++row_idx) {
      if (is_nullable) {
        if (cblock.is_null(row_idx)) {
          memset(dst_cell, 0, cell_size); // avoid leaking server memory
          BitmapChange(dst_bitmap, dst_col_idx, true);
          continue;
        }
        BitmapChange(dst_bitmap, dst_col_idx, false);
      }
      if (is_string) {
        const Slice *slice = reinterpret_cast<const Slice*>(src_cell);
        size_t offset_in_indirect = indirect->size();
        indirect->append(slice->data(), slice->size());
        Slice *dst_slice = reinterpret_cast<Slice*>(dst_cell);
        *dst_slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                           slice->size());
      } else { // non-string
        memcpy(dst_cell, src_cell, cell_size);
      }
    }
  }
}

ATTRIBUTE_ALWAYS_INLINE
static void LoopIter(const uint8_t** src_cells,
                     const uint8_t** src_nulls,

                     const int32_t* src_col_indexes,
                     const int32_t* proj_col_sizes,
                     const bool* proj_col_nullable,
                     const bool* proj_cols_string,
                     const int num_proj_cols,
                     const int row_idx,
                     const int i,
                     uint8_t*& __restrict__  dst_cell,
                     uint8_t* __restrict__ const dst_null_bitmap,
                     faststring* const indirect) {


  const uint8_t* src_cell = src_cells[i] + row_idx * proj_col_sizes[i];
  bool is_null = proj_col_nullable[i] && !BitmapTest(src_nulls[i], row_idx);

  BitmapChange(dst_null_bitmap, i, is_null);

  if (is_null) {
    memset(dst_cell, 0, proj_col_sizes[i]);
  } else {
    if (proj_cols_string[i]) {
      const Slice *slice = reinterpret_cast<const Slice*>(src_cell);
      size_t offset_in_indirect = indirect->size();
      indirect->append(slice->data(), slice->size());
      Slice *dst_slice = reinterpret_cast<Slice*>(dst_cell);
      *dst_slice = Slice(reinterpret_cast<const uint8_t*>(offset_in_indirect),
                         slice->size());
    } else {
      memcpy(dst_cell, src_cell, proj_col_sizes[i]);
    }
  }
  dst_cell += proj_col_sizes[i];
}

void _SerializeRowBlock2(RowBlock* block,
                         uint8_t* dst_base,
                         faststring* indirect,

                         const int32_t* src_col_indexes,
                         const int32_t* proj_col_sizes,
                         const bool* proj_col_nullable,
                         const bool* proj_cols_string,
                         int num_proj_cols,

                         int row_stride,
                         int offset_to_null_bitmap) {

  BitmapIterator selected_row_iter(block->selection_vector()->bitmap(),
                                   block->nrows());

  const uint8_t* src_cells[num_proj_cols];
  const uint8_t* src_nulls[num_proj_cols];
  for (int i = 0; i < num_proj_cols; i++) {
    int src_idx = src_col_indexes[i];
    src_cells[i] = block->column_data_base_ptr(src_idx);
    src_nulls[i] = block->column_null_bitmap_ptr(src_idx);
  }

  bool selected;
  int row_idx = 0;
  int run_size;
  while ((run_size = selected_row_iter.Next(&selected))) {
    if (!selected) {
      row_idx += run_size;
      continue;
    }

    for (int idx_in_run = 0; idx_in_run < run_size; idx_in_run++) {
      uint8_t* dst_cell = dst_base + row_idx * row_stride;
      uint8_t* dst_null_bitmap = dst_cell + offset_to_null_bitmap;

      int rem = num_proj_cols;
      int i = 0;

      while (rem >= 4) {
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        rem -= 4;
      }
      while (rem >= 2) {
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        rem -= 2;
      }

      while (rem >= 1) {
        LoopIter(src_cells, src_nulls, src_col_indexes, proj_col_sizes, proj_col_nullable, proj_cols_string, num_proj_cols, row_idx,
                 i++, dst_cell, dst_null_bitmap, indirect);
        rem--;
      }

      row_idx++;
    }
  }
}

} // extern "C"
} // namespace kudu
