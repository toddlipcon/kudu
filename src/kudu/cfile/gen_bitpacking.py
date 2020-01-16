#!/usr/bin/env python

import os
import sys

BITS = range(1, 33)

# TODO(todd) should we enforce aligned input/output for these?
# may need the format to ensure alignment.

class ScalarGenerator(object):
    reg_type = "uint32_t"
    vector_lanes=1
    num_ints = 32
    @staticmethod
    def load_unpacked(operand):
        return "*({})".format(operand)
    @staticmethod

    def load_packed(operand):
        return "*({})".format(operand)

    @staticmethod
    def store_unpacked(src, dst):
        return "*({}) = {}".format(dst, src)

    @staticmethod
    def store_packed(src, dst):
        return "*({}) = {}".format(dst, src)

    @staticmethod
    def bitwise_or(op1, op2):
        return "({}) | ({})".format(op1, op2)

    @staticmethod
    def bitwise_and(op1, op2):
        return "({}) & ({})".format(op1, op2)

    @staticmethod
    def shift_left(op, shift):
        return "(({}) << {})".format(op, shift)
    @staticmethod
    def shift_right(op, shift):
        return "(({}) >> {})".format(op, shift)
    @staticmethod
    def mask(bits):
        return "((1ULL << {}) - 1)".format(bits)

class SIMDGenerator(object):
    reg_type = "__m128i"
    vector_lanes=4
    num_ints = 128

    @staticmethod
    def load_unpacked(operand):
        return "load_4x32({})".format(operand)
    @staticmethod
    def store_unpacked(src, dst):
        return "store_4x32({}, {})".format(src, dst)

    @staticmethod
    def load_packed(operand):
        return "_mm_loadu_si128({})".format(operand)
    @staticmethod
    def store_packed(src, dst):
        return "_mm_storeu_si128({}, {})".format(dst, src)

    @staticmethod
    def bitwise_or(op1, op2):
        return "_mm_or_si128({}, {})".format(op1, op2)

    @staticmethod
    def bitwise_and(op1, op2):
        return "_mm_and_si128({}, {})".format(op1, op2)

    @staticmethod
    def shift_left(op, shift):
        return "_mm_slli_epi32({}, {})".format(op, shift)
    @staticmethod
    def shift_right(op, shift):
        return "_mm_srli_epi32({}, {})".format(op, shift)
    @staticmethod
    def mask(bits):
        return " _mm_set1_epi32((1ULL << {}) - 1)".format(bits)

def gen_packwithoutmask(func_name, bitsize, codegen):
    print("""
template<typename INT> // either uint32_t or uint64_t
__attribute__((noinline))
void __FUNC_NAME__(const INT* __restrict__ in,
    __REG_TYPE__* __restrict__ out) {
  __REG_TYPE__ in_reg, out_reg;
"""
          .replace("__REG_TYPE__", codegen.reg_type)
          .replace("__FUNC_NAME__", func_name))
    shift = 0
    for i in xrange(codegen.num_ints / codegen.vector_lanes):
        print("  in_reg = {};".format(codegen.load_unpacked("in")))
        print("  in += {};".format(codegen.vector_lanes))
        if shift == 0:
            print("  out_reg = in_reg;")
        else:
            print("  out_reg = {};".format(
              codegen.bitwise_or("out_reg", codegen.shift_left("in_reg", shift))))
        shift += bitsize
        if shift >= 32:
            print("  {};".format(codegen.store_packed("out_reg", "out++")))
            shift -= 32
            if shift:
                print("  // {rem} most significant bits of in_reg fell off the left in above shift."
                      .format(rem=shift))
                print("  // Shift right to recover them in next output word")
                print("  out_reg = {};".format(
                  codegen.shift_right("in_reg", bitsize - shift)))
        print
    assert shift == 0
    print "}"


def gen_unpack(func_name, bitsize, codegen):
    print("""
template<typename INT>
__attribute__((noinline))
void __FUNC_NAME__(const __REG_TYPE__* __restrict__ in,
    INT* __restrict__ out) {
  __REG_TYPE__ in_reg, out_reg, mask;
"""
          .replace("__REG_TYPE__", codegen.reg_type)
          .replace("__FUNC_NAME__", func_name))
    print("  mask = {};".format(codegen.mask(bitsize)))
    print("  in_reg = {};".format(codegen.load_packed("in++")))
    shift = 0
    for i in xrange(codegen.num_ints / codegen.vector_lanes):
        if shift != 0:
          print("  out_reg = {};".format(codegen.shift_right("in_reg", shift)))
        else:
          print("  out_reg = in_reg;")
        shift += bitsize
        if shift < 32:
            # unless the bitpacked value extended all the way to the
            # MSB of the source uint32, perform masking.
            print("  out_reg = {};".format(codegen.bitwise_and("out_reg", "mask")))
        else:
            # Done with this input byte.
            print("  in_reg = {};".format(codegen.load_packed("in++")))
            shift -= 32
            if shift:
                print("  // {bits} bits still need to be recovered...".format(bits=shift))
                print("  out_reg = {};".format(
                  codegen.bitwise_or(
                    "out_reg",
                    codegen.bitwise_and(codegen.shift_left("in_reg", bitsize - shift), "mask"))))

        print("  {};".format(codegen.store_unpacked("out_reg", "out")))
        print("  out += {};".format(codegen.vector_lanes))
        print
    assert shift == 0
    print "}"

def gen_wrapper(method, in_type, out_type, ints_per_invocation):
    print("""
void %(method)s(
    const %(in_type)s* __restrict__ in,
    %(out_type)s* __restrict__ out,
    int num_bits) {
  DCHECK_GE(num_bits, 0);
  DCHECK_LE(num_bits, 32);
  switch (num_bits) {
""" % dict(method=method, in_type=in_type, out_type=out_type))

    # For unpacking, if the values are all zero, we need to
    # actually unpack the 0s. For packing, there will be no
    # output so we can just return if num_bits = 0.
    if 'unpack' in method:
        print("""
    case 0:
      memset(out, 0, sizeof(out[0]) * {ints_per_invocation});
      break;
""".format(ints_per_invocation=ints_per_invocation))
    for i in BITS:
        print("    case {i}: {method}{i}(in, out); return;"
              .format(i=i, method=method))
    print("  }")
    print("}")

# redirect stdout
def main(argv):
    if len(argv) == 2:
        sys.stdout = open(sys.argv[1], "w")

    print("// AUTO-GENERATED BY {}".format(os.path.realpath(argv[0])))
    print("""
#include <stdint.h>
#include <xmmintrin.h>
#include <immintrin.h>
#include <glog/logging.h>

namespace kudu {
namespace cfile {
namespace bp {
namespace {

template<class SrcInt>
__m128i load_4x32(const SrcInt * __restrict__ src) {
  uint32_t x[4] = {
    static_cast<SrcInt>(src[0]),
    static_cast<SrcInt>(src[1]),
    static_cast<SrcInt>(src[2]),
    static_cast<SrcInt>(src[3])};
   return _mm_loadu_si128((const __m128i*)x);
}

template<>
__m128i load_4x32(const uint64_t * __restrict__ src) {
   return _mm_setr_epi32(src[0], src[1], src[2], src[3]);
}

// Store 4 32-bit integers from SSE 4 n-bit integers.
template<typename DstInt>
void store_4x32(__m128i m, DstInt * __restrict__ dst) {
  // This gets optimized into a sequence of instructions that
  // more efficient than the four separate extracts written here.
  *dst++ = static_cast<DstInt>(static_cast<uint32_t>(_mm_extract_epi32(m, 0)));
  *dst++ = static_cast<DstInt>(static_cast<uint32_t>(_mm_extract_epi32(m, 1)));
  *dst++ = static_cast<DstInt>(static_cast<uint32_t>(_mm_extract_epi32(m, 2)));
  *dst++ = static_cast<DstInt>(static_cast<uint32_t>(_mm_extract_epi32(m, 3)));
}

// Specialization for storing 32-bit.
template<>
void store_4x32(__m128i m, uint32_t * __restrict__ dst) {
   _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), m);
}


""")
    for i in BITS:
        gen_unpack("simd_fastunpack{}".format(i), i, SIMDGenerator)
        gen_unpack("fastunpack{}".format(i), i, ScalarGenerator)
    for i in BITS:
        gen_packwithoutmask("simd_fastpackwithoutmask{}".format(i), i, SIMDGenerator)
        gen_packwithoutmask("fastpackwithoutmask{}".format(i), i, ScalarGenerator)


    print("} // anonymous namespace")

    for scalar_type_bits in [8, 16, 32, 64]:
        scalar_type = "uint{}_t".format(scalar_type_bits)

        gen_wrapper("simd_fastunpack", in_type="__m128i", out_type=scalar_type,
                    ints_per_invocation=32)
        gen_wrapper("fastunpack", in_type="uint32_t", out_type=scalar_type,
                    ints_per_invocation=32)

        gen_wrapper("simd_fastpackwithoutmask", in_type=scalar_type, out_type="__m128i",
                    ints_per_invocation=128)
        gen_wrapper("fastpackwithoutmask", in_type=scalar_type, out_type="uint32_t",
                    ints_per_invocation=128)
    print("} // anonymous bp")
    print("} // anonymous cfile")
    print("} // anonymous kudu")

if __name__ == "__main__":
    main(sys.argv)
