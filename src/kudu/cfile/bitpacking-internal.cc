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


#include <cstdint>
#include <type_traits>

#include <glog/logging.h>

namespace kudu {
namespace cfile {
namespace bp {

namespace {

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) < 32>::type unpack_single_out(
    const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  *out = ((*in) >> SHR) % (1 << DELTA);
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) >= 32>::type unpack_single_out(
    const uint32_t *__restrict__ &in, uint32_t *__restrict__ out) {
  *out = (*in) >> SHR;
  ++in;

  static const uint8_t NEXT_SHR = SHR + DELTA - 32;
  *out |= ((*in) % (1U << NEXT_SHR)) << (32 - SHR);
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) < 32>::type unpack_single_out(
    const uint32_t *__restrict__ in, uint64_t *__restrict__ out) {
  *out = ((static_cast<uint64_t>(*in)) >> SHR) % (1ULL << DELTA);
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) >= 32 && (DELTA + SHR) < 64>::type
unpack_single_out(const uint32_t *__restrict__ &in,
                  uint64_t *__restrict__ out) {
  *out = static_cast<uint64_t>(*in) >> SHR;
  ++in;
  if (DELTA + SHR > 32) {
    static const uint8_t NEXT_SHR = SHR + DELTA - 32;
    *out |= static_cast<uint64_t>((*in) % (1U << NEXT_SHR)) << (32 - SHR);
  }
}

template <uint8_t DELTA, uint8_t SHR>
typename std::enable_if<(DELTA + SHR) >= 64>::type unpack_single_out(
    const uint32_t *__restrict__ &in, uint64_t *__restrict__ out) {
  *out = static_cast<uint64_t>(*in) >> SHR;
  ++in;

  *out |= static_cast<uint64_t>(*in) << (32 - SHR);
  ++in;

  if (DELTA + SHR > 64) {
    static const uint8_t NEXT_SHR = DELTA + SHR - 64;
    *out |= static_cast<uint64_t>((*in) % (1U << NEXT_SHR)) << (64 - SHR);
  }
}

template <uint16_t DELTA, uint16_t SHL, uint32_t MASK>
    typename std::enable_if <
    DELTA + SHL<32>::type pack_single_in(const uint32_t in,
                                         uint32_t *__restrict__ out) {
  if (SHL == 0) {
    *out = in & MASK;
  } else {
    *out |= (in & MASK) << SHL;
  }
}

template <uint16_t DELTA, uint16_t SHL, uint32_t MASK>
typename std::enable_if<DELTA + SHL >= 32>::type pack_single_in(
    const uint32_t in, uint32_t *__restrict__ &out) {
  *out |= in << SHL;
  ++out;

  if (DELTA + SHL > 32) {
    *out = (in & MASK) >> (32 - SHL);
  }
}

template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
    typename std::enable_if <
    DELTA + SHL<32>::type pack_single_in64(const uint64_t in,
                                           uint32_t *__restrict__ out) {
  if (SHL == 0) {
    *out = static_cast<uint32_t>(in & MASK);
  } else {
    *out |= (in & MASK) << SHL;
  }
}

template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
        typename std::enable_if < DELTA + SHL >= 32 &&
    DELTA + SHL<64>::type pack_single_in64(const uint64_t in,
                                           uint32_t *__restrict__ &out) {
  if (SHL == 0) {
    *out = static_cast<uint32_t>(in & MASK);
  } else {
    *out |= (in & MASK) << SHL;
  }

  ++out;

  if (DELTA + SHL > 32) {
    *out = static_cast<uint32_t>((in & MASK) >> (32 - SHL));
  }
}

template <uint16_t DELTA, uint16_t SHL, uint64_t MASK>
typename std::enable_if<DELTA + SHL >= 64>::type pack_single_in64(
    const uint64_t in, uint32_t *__restrict__ &out) {
  *out |= in << SHL;
  ++out;

  *out = static_cast<uint32_t>((in & MASK) >> (32 - SHL));
  ++out;

  if (DELTA + SHL > 64) {
    *out = (in & MASK) >> (64 - SHL);
  }
}

template <uint16_t DELTA, uint16_t OINDEX = 0>
struct Unroller {
  static void Unpack(const uint32_t *__restrict__ &in,
                     uint32_t *__restrict__ out) {
    unpack_single_out<DELTA, (DELTA * OINDEX) % 32>(in, out + OINDEX);

    Unroller<DELTA, OINDEX + 1>::Unpack(in, out);
  }

  static void Unpack(const uint32_t *__restrict__ &in,
                     uint64_t *__restrict__ out) {
    unpack_single_out<DELTA, (DELTA * OINDEX) % 32>(in, out + OINDEX);

    Unroller<DELTA, OINDEX + 1>::Unpack(in, out);
  }

  static void Pack(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
    pack_single_in<DELTA, (DELTA * OINDEX) % 32, (1U << DELTA) - 1>(in[OINDEX],
                                                                    out);

    Unroller<DELTA, OINDEX + 1>::Pack(in, out);
  }

  static void Pack(const uint64_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
    pack_single_in64<DELTA, (DELTA * OINDEX) % 32, (1ULL << DELTA) - 1>(
        in[OINDEX], out);

    Unroller<DELTA, OINDEX + 1>::Pack(in, out);
  }

  static void PackNoMask(const uint32_t *__restrict__ in,
                         uint32_t *__restrict__ out) {
    pack_single_in<DELTA, (DELTA * OINDEX) % 32, uint32_t(-1)>(in[OINDEX], out);

    Unroller<DELTA, OINDEX + 1>::PackNoMask(in, out);
  }

  static void PackNoMask(const uint64_t *__restrict__ in,
                         uint32_t *__restrict__ out) {
    pack_single_in64<DELTA, (DELTA * OINDEX) % 32, uint64_t(-1)>(in[OINDEX],
                                                                 out);

    Unroller<DELTA, OINDEX + 1>::PackNoMask(in, out);
  }
};

template <uint16_t DELTA>
struct Unroller<DELTA, 31> {
  enum { SHIFT = (DELTA * 31) % 32 };

  static void Unpack(const uint32_t *__restrict__ in,
                     uint32_t *__restrict__ out) {
    out[31] = (*in) >> SHIFT;
  }

  static void Unpack(const uint32_t *__restrict__ in,
                     uint64_t *__restrict__ out) {
    out[31] = (*in) >> SHIFT;
    if (DELTA > 32) {
      ++in;
      out[31] |= static_cast<uint64_t>(*in) << (32 - SHIFT);
    }
  }

  static void Pack(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
    *out |= (in[31] << SHIFT);
  }

  static void Pack(const uint64_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
    *out |= (in[31] << SHIFT);
    if (DELTA > 32) {
      ++out;
      *out = static_cast<uint32_t>(in[31] >> (32 - SHIFT));
    }
  }

  static void PackNoMask(const uint32_t *__restrict__ in,
                         uint32_t *__restrict__ out) {
    *out |= (in[31] << SHIFT);
  }

  static void PackNoMask(const uint64_t *__restrict__ in,
                         uint32_t *__restrict__ out) {
    *out |= (in[31] << SHIFT);
    if (DELTA > 32) {
      ++out;
      *out = static_cast<uint32_t>(in[31] >> (32 - SHIFT));
    }
  }
};

// Special cases
void __fastunpack0(const uint32_t *__restrict__, uint32_t *__restrict__ out) {
  for (uint32_t i = 0; i < 32; ++i) *(out++) = 0;
}

void __fastunpack0(const uint32_t *__restrict__, uint64_t *__restrict__ out) {
  for (uint32_t i = 0; i < 32; ++i) *(out++) = 0;
}

void __fastpack0(const uint32_t *__restrict__, uint32_t *__restrict__) {}
void __fastpack0(const uint64_t *__restrict__, uint32_t *__restrict__) {}

void __fastpackwithoutmask0(const uint32_t *__restrict__,
                            uint32_t *__restrict__) {}
void __fastpackwithoutmask0(const uint64_t *__restrict__,
                            uint32_t *__restrict__) {}

// fastunpack for 32 bits
void __fastunpack1(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<1>::Unpack(in, out);
}

void __fastunpack2(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<2>::Unpack(in, out);
}

void __fastunpack3(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<3>::Unpack(in, out);
}

void __fastunpack4(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 4; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 4)
      *(out++) = ((*in) >> inwordpointer) % (1U << 4);
    ++in;
  }
}

void __fastunpack5(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<5>::Unpack(in, out);
}

void __fastunpack6(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<6>::Unpack(in, out);
}

void __fastunpack7(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<7>::Unpack(in, out);
}

void __fastunpack8(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 8; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 8)
      *(out++) = ((*in) >> inwordpointer) % (1U << 8);
    ++in;
  }
}

void __fastunpack9(const uint32_t *__restrict__ in,
                   uint32_t *__restrict__ out) {
  Unroller<9>::Unpack(in, out);
}

void __fastunpack10(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<10>::Unpack(in, out);
}

void __fastunpack11(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<11>::Unpack(in, out);
}

void __fastunpack12(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<12>::Unpack(in, out);
}

void __fastunpack13(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<13>::Unpack(in, out);
}

void __fastunpack14(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<14>::Unpack(in, out);
}

void __fastunpack15(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<15>::Unpack(in, out);
}

void __fastunpack16(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 16; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 16)
      *(out++) = ((*in) >> inwordpointer) % (1U << 16);
    ++in;
  }
}

void __fastunpack17(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<17>::Unpack(in, out);
}

void __fastunpack18(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<18>::Unpack(in, out);
}

void __fastunpack19(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<19>::Unpack(in, out);
}

void __fastunpack20(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<20>::Unpack(in, out);
}

void __fastunpack21(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<21>::Unpack(in, out);
}

void __fastunpack22(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<22>::Unpack(in, out);
}

void __fastunpack23(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<23>::Unpack(in, out);
}

void __fastunpack24(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<24>::Unpack(in, out);
}

void __fastunpack25(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<25>::Unpack(in, out);
}

void __fastunpack26(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<26>::Unpack(in, out);
}

void __fastunpack27(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<27>::Unpack(in, out);
}

void __fastunpack28(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<28>::Unpack(in, out);
}

void __fastunpack29(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<29>::Unpack(in, out);
}

void __fastunpack30(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<30>::Unpack(in, out);
}

void __fastunpack31(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  Unroller<31>::Unpack(in, out);
}

void __fastunpack32(const uint32_t *__restrict__ in,
                    uint32_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) out[k] = in[k];
}

// fastupack for 64 bits
void __fastunpack1(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<1>::Unpack(in, out);
}

void __fastunpack2(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<2>::Unpack(in, out);
}

void __fastunpack3(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<3>::Unpack(in, out);
}

void __fastunpack4(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 4; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 4)
      *(out++) = ((*in) >> inwordpointer) % (1U << 4);
    ++in;
  }
}

void __fastunpack5(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<5>::Unpack(in, out);
}

void __fastunpack6(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<6>::Unpack(in, out);
}

void __fastunpack7(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<7>::Unpack(in, out);
}

void __fastunpack8(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 8; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 8) {
      *(out++) = ((*in) >> inwordpointer) % (1U << 8);
    }
    ++in;
  }
}

void __fastunpack9(const uint32_t *__restrict__ in,
                   uint64_t *__restrict__ out) {
  Unroller<9>::Unpack(in, out);
}

void __fastunpack10(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<10>::Unpack(in, out);
}

void __fastunpack11(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<11>::Unpack(in, out);
}

void __fastunpack12(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<12>::Unpack(in, out);
}

void __fastunpack13(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<13>::Unpack(in, out);
}

void __fastunpack14(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<14>::Unpack(in, out);
}

void __fastunpack15(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<15>::Unpack(in, out);
}

void __fastunpack16(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  for (uint32_t outer = 0; outer < 16; ++outer) {
    for (uint32_t inwordpointer = 0; inwordpointer < 32; inwordpointer += 16)
      *(out++) = ((*in) >> inwordpointer) % (1U << 16);
    ++in;
  }
}

void __fastunpack17(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<17>::Unpack(in, out);
}

void __fastunpack18(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<18>::Unpack(in, out);
}

void __fastunpack19(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<19>::Unpack(in, out);
}

void __fastunpack20(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<20>::Unpack(in, out);
}

void __fastunpack21(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<21>::Unpack(in, out);
}

void __fastunpack22(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<22>::Unpack(in, out);
}

void __fastunpack23(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<23>::Unpack(in, out);
}

void __fastunpack24(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<24>::Unpack(in, out);
}

void __fastunpack25(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<25>::Unpack(in, out);
}

void __fastunpack26(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<26>::Unpack(in, out);
}

void __fastunpack27(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<27>::Unpack(in, out);
}

void __fastunpack28(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<28>::Unpack(in, out);
}

void __fastunpack29(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<29>::Unpack(in, out);
}

void __fastunpack30(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<30>::Unpack(in, out);
}

void __fastunpack31(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<31>::Unpack(in, out);
}

void __fastunpack32(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) out[k] = in[k];
}

void __fastunpack33(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<33>::Unpack(in, out);
}

void __fastunpack34(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<34>::Unpack(in, out);
}

void __fastunpack35(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<35>::Unpack(in, out);
}

void __fastunpack36(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<36>::Unpack(in, out);
}

void __fastunpack37(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<37>::Unpack(in, out);
}

void __fastunpack38(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<38>::Unpack(in, out);
}

void __fastunpack39(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<39>::Unpack(in, out);
}

void __fastunpack40(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<40>::Unpack(in, out);
}

void __fastunpack41(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<41>::Unpack(in, out);
}

void __fastunpack42(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<42>::Unpack(in, out);
}

void __fastunpack43(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<43>::Unpack(in, out);
}

void __fastunpack44(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<44>::Unpack(in, out);
}

void __fastunpack45(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<45>::Unpack(in, out);
}

void __fastunpack46(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<46>::Unpack(in, out);
}

void __fastunpack47(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<47>::Unpack(in, out);
}

void __fastunpack48(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<48>::Unpack(in, out);
}

void __fastunpack49(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<49>::Unpack(in, out);
}

void __fastunpack50(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<50>::Unpack(in, out);
}

void __fastunpack51(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<51>::Unpack(in, out);
}

void __fastunpack52(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<52>::Unpack(in, out);
}

void __fastunpack53(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<53>::Unpack(in, out);
}

void __fastunpack54(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<54>::Unpack(in, out);
}

void __fastunpack55(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<55>::Unpack(in, out);
}

void __fastunpack56(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<56>::Unpack(in, out);
}

void __fastunpack57(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<57>::Unpack(in, out);
}

void __fastunpack58(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<58>::Unpack(in, out);
}

void __fastunpack59(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<59>::Unpack(in, out);
}

void __fastunpack60(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<60>::Unpack(in, out);
}

void __fastunpack61(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<61>::Unpack(in, out);
}

void __fastunpack62(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<62>::Unpack(in, out);
}

void __fastunpack63(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  Unroller<63>::Unpack(in, out);
}

void __fastunpack64(const uint32_t *__restrict__ in,
                    uint64_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) {
    out[k] = in[k * 2];
    out[k] |= static_cast<uint64_t>(in[k * 2 + 1]) << 32;
  }
}

// fastpack for 32 bits

void __fastpack1(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<1>::Pack(in, out);
}

void __fastpack2(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<2>::Pack(in, out);
}

void __fastpack3(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<3>::Pack(in, out);
}

void __fastpack4(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<4>::Pack(in, out);
}

void __fastpack5(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<5>::Pack(in, out);
}

void __fastpack6(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<6>::Pack(in, out);
}

void __fastpack7(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<7>::Pack(in, out);
}

void __fastpack8(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<8>::Pack(in, out);
}

void __fastpack9(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<9>::Pack(in, out);
}

void __fastpack10(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<10>::Pack(in, out);
}

void __fastpack11(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<11>::Pack(in, out);
}

void __fastpack12(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<12>::Pack(in, out);
}

void __fastpack13(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<13>::Pack(in, out);
}

void __fastpack14(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<14>::Pack(in, out);
}

void __fastpack15(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<15>::Pack(in, out);
}

void __fastpack16(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<16>::Pack(in, out);
}

void __fastpack17(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<17>::Pack(in, out);
}

void __fastpack18(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<18>::Pack(in, out);
}

void __fastpack19(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<19>::Pack(in, out);
}

void __fastpack20(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<20>::Pack(in, out);
}

void __fastpack21(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<21>::Pack(in, out);
}

void __fastpack22(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<22>::Pack(in, out);
}

void __fastpack23(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<23>::Pack(in, out);
}

void __fastpack24(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<24>::Pack(in, out);
}

void __fastpack25(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<25>::Pack(in, out);
}

void __fastpack26(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<26>::Pack(in, out);
}

void __fastpack27(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<27>::Pack(in, out);
}

void __fastpack28(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<28>::Pack(in, out);
}

void __fastpack29(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<29>::Pack(in, out);
}

void __fastpack30(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<30>::Pack(in, out);
}

void __fastpack31(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<31>::Pack(in, out);
}

void __fastpack32(const uint32_t *__restrict__ in, uint32_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) out[k] = in[k];
}

// fastpack for 64 bits

void __fastpack1(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<1>::Pack(in, out);
}

void __fastpack2(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<2>::Pack(in, out);
}

void __fastpack3(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<3>::Pack(in, out);
}

void __fastpack4(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<4>::Pack(in, out);
}

void __fastpack5(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<5>::Pack(in, out);
}

void __fastpack6(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<6>::Pack(in, out);
}

void __fastpack7(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<7>::Pack(in, out);
}

void __fastpack8(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<8>::Pack(in, out);
}

void __fastpack9(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<9>::Pack(in, out);
}

void __fastpack10(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<10>::Pack(in, out);
}

void __fastpack11(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<11>::Pack(in, out);
}

void __fastpack12(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<12>::Pack(in, out);
}

void __fastpack13(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<13>::Pack(in, out);
}

void __fastpack14(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<14>::Pack(in, out);
}

void __fastpack15(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<15>::Pack(in, out);
}

void __fastpack16(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<16>::Pack(in, out);
}

void __fastpack17(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<17>::Pack(in, out);
}

void __fastpack18(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<18>::Pack(in, out);
}

void __fastpack19(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<19>::Pack(in, out);
}

void __fastpack20(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<20>::Pack(in, out);
}

void __fastpack21(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<21>::Pack(in, out);
}

void __fastpack22(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<22>::Pack(in, out);
}

void __fastpack23(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<23>::Pack(in, out);
}

void __fastpack24(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<24>::Pack(in, out);
}

void __fastpack25(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<25>::Pack(in, out);
}

void __fastpack26(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<26>::Pack(in, out);
}

void __fastpack27(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<27>::Pack(in, out);
}

void __fastpack28(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<28>::Pack(in, out);
}

void __fastpack29(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<29>::Pack(in, out);
}

void __fastpack30(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<30>::Pack(in, out);
}

void __fastpack31(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<31>::Pack(in, out);
}

void __fastpack32(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) {
    out[k] = static_cast<uint32_t>(in[k]);
  }
}

void __fastpack33(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<33>::Pack(in, out);
}

void __fastpack34(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<34>::Pack(in, out);
}

void __fastpack35(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<35>::Pack(in, out);
}

void __fastpack36(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<36>::Pack(in, out);
}

void __fastpack37(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<37>::Pack(in, out);
}

void __fastpack38(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<38>::Pack(in, out);
}

void __fastpack39(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<39>::Pack(in, out);
}

void __fastpack40(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<40>::Pack(in, out);
}

void __fastpack41(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<41>::Pack(in, out);
}

void __fastpack42(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<42>::Pack(in, out);
}

void __fastpack43(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<43>::Pack(in, out);
}

void __fastpack44(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<44>::Pack(in, out);
}

void __fastpack45(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<45>::Pack(in, out);
}

void __fastpack46(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<46>::Pack(in, out);
}

void __fastpack47(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<47>::Pack(in, out);
}

void __fastpack48(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<48>::Pack(in, out);
}

void __fastpack49(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<49>::Pack(in, out);
}

void __fastpack50(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<50>::Pack(in, out);
}

void __fastpack51(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<51>::Pack(in, out);
}

void __fastpack52(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<52>::Pack(in, out);
}

void __fastpack53(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<53>::Pack(in, out);
}

void __fastpack54(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<54>::Pack(in, out);
}

void __fastpack55(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<55>::Pack(in, out);
}

void __fastpack56(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<56>::Pack(in, out);
}

void __fastpack57(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<57>::Pack(in, out);
}

void __fastpack58(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<58>::Pack(in, out);
}

void __fastpack59(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<59>::Pack(in, out);
}

void __fastpack60(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<60>::Pack(in, out);
}

void __fastpack61(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<61>::Pack(in, out);
}

void __fastpack62(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<62>::Pack(in, out);
}

void __fastpack63(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  Unroller<63>::Pack(in, out);
}

void __fastpack64(const uint64_t *__restrict__ in, uint32_t *__restrict__ out) {
  for (int i = 0; i < 32; ++i) {
    out[2 * i] = static_cast<uint32_t>(in[i]);
    out[2 * i + 1] = in[i] >> 32;
  }
}

// fastpackwithoutmask for 32 bits
/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask1(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<1>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask2(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<2>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask3(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<3>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask4(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<4>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask5(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<5>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask6(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<6>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask7(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<7>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask8(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<8>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask9(const uint32_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<9>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask10(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<10>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask11(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<11>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask12(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<12>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask13(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<13>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask14(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<14>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask15(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<15>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask16(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<16>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask17(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<17>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask18(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<18>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask19(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<19>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask20(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<20>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask21(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<21>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask22(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<22>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask23(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<23>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask24(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<24>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask25(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<25>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask26(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<26>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask27(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<27>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask28(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<28>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask29(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<29>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask30(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<30>::PackNoMask(in, out);
}

/*assumes that integers fit in the prescribed number of bits */
void __fastpackwithoutmask31(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<31>::PackNoMask(in, out);
}

void __fastpackwithoutmask32(const uint32_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  for (int k = 0; k < 32; ++k) out[k] = in[k];
}

// fastpackwithoutmask for 64 bits
void __fastpackwithoutmask1(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<1>::PackNoMask(in, out);
}

void __fastpackwithoutmask2(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<2>::PackNoMask(in, out);
}

void __fastpackwithoutmask3(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<3>::PackNoMask(in, out);
}

void __fastpackwithoutmask4(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<4>::PackNoMask(in, out);
}

void __fastpackwithoutmask5(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<5>::PackNoMask(in, out);
}

void __fastpackwithoutmask6(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<6>::PackNoMask(in, out);
}

void __fastpackwithoutmask7(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<7>::PackNoMask(in, out);
}

void __fastpackwithoutmask8(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<8>::PackNoMask(in, out);
}

void __fastpackwithoutmask9(const uint64_t *__restrict__ in,
                            uint32_t *__restrict__ out) {
  Unroller<9>::PackNoMask(in, out);
}

void __fastpackwithoutmask10(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<10>::PackNoMask(in, out);
}

void __fastpackwithoutmask11(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<11>::PackNoMask(in, out);
}

void __fastpackwithoutmask12(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<12>::PackNoMask(in, out);
}

void __fastpackwithoutmask13(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<13>::PackNoMask(in, out);
}

void __fastpackwithoutmask14(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<14>::PackNoMask(in, out);
}

void __fastpackwithoutmask15(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<15>::PackNoMask(in, out);
}

void __fastpackwithoutmask16(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<16>::PackNoMask(in, out);
}

void __fastpackwithoutmask17(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<17>::PackNoMask(in, out);
}

void __fastpackwithoutmask18(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<18>::PackNoMask(in, out);
}

void __fastpackwithoutmask19(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<19>::PackNoMask(in, out);
}

void __fastpackwithoutmask20(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<20>::PackNoMask(in, out);
}

void __fastpackwithoutmask21(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<21>::PackNoMask(in, out);
}

void __fastpackwithoutmask22(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<22>::PackNoMask(in, out);
}

void __fastpackwithoutmask23(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<23>::PackNoMask(in, out);
}

void __fastpackwithoutmask24(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<24>::PackNoMask(in, out);
}

void __fastpackwithoutmask25(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<25>::PackNoMask(in, out);
}

void __fastpackwithoutmask26(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<26>::PackNoMask(in, out);
}

void __fastpackwithoutmask27(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<27>::PackNoMask(in, out);
}

void __fastpackwithoutmask28(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<28>::PackNoMask(in, out);
}

void __fastpackwithoutmask29(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<29>::PackNoMask(in, out);
}

void __fastpackwithoutmask30(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<30>::PackNoMask(in, out);
}

void __fastpackwithoutmask31(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<31>::PackNoMask(in, out);
}

void __fastpackwithoutmask32(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  for (int i = 0; i < 32; ++i) {
    out[i] = static_cast<uint32_t>(in[i]);
  }
}

void __fastpackwithoutmask33(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<33>::PackNoMask(in, out);
}

void __fastpackwithoutmask34(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<34>::PackNoMask(in, out);
}

void __fastpackwithoutmask35(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<35>::PackNoMask(in, out);
}

void __fastpackwithoutmask36(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<36>::PackNoMask(in, out);
}

void __fastpackwithoutmask37(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<37>::PackNoMask(in, out);
}

void __fastpackwithoutmask38(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<38>::PackNoMask(in, out);
}

void __fastpackwithoutmask39(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<39>::PackNoMask(in, out);
}

void __fastpackwithoutmask40(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<40>::PackNoMask(in, out);
}

void __fastpackwithoutmask41(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<41>::PackNoMask(in, out);
}

void __fastpackwithoutmask42(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<42>::PackNoMask(in, out);
}

void __fastpackwithoutmask43(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<43>::PackNoMask(in, out);
}

void __fastpackwithoutmask44(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<44>::PackNoMask(in, out);
}

void __fastpackwithoutmask45(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<45>::PackNoMask(in, out);
}

void __fastpackwithoutmask46(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<46>::PackNoMask(in, out);
}

void __fastpackwithoutmask47(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<47>::PackNoMask(in, out);
}

void __fastpackwithoutmask48(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<48>::PackNoMask(in, out);
}

void __fastpackwithoutmask49(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<49>::PackNoMask(in, out);
}

void __fastpackwithoutmask50(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<50>::PackNoMask(in, out);
}

void __fastpackwithoutmask51(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<51>::PackNoMask(in, out);
}

void __fastpackwithoutmask52(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<52>::PackNoMask(in, out);
}

void __fastpackwithoutmask53(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<53>::PackNoMask(in, out);
}

void __fastpackwithoutmask54(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<54>::PackNoMask(in, out);
}

void __fastpackwithoutmask55(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<55>::PackNoMask(in, out);
}

void __fastpackwithoutmask56(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<56>::PackNoMask(in, out);
}

void __fastpackwithoutmask57(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<57>::PackNoMask(in, out);
}

void __fastpackwithoutmask58(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<58>::PackNoMask(in, out);
}

void __fastpackwithoutmask59(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<59>::PackNoMask(in, out);
}

void __fastpackwithoutmask60(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<60>::PackNoMask(in, out);
}

void __fastpackwithoutmask61(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<61>::PackNoMask(in, out);
}

void __fastpackwithoutmask62(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<62>::PackNoMask(in, out);
}

void __fastpackwithoutmask63(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  Unroller<63>::PackNoMask(in, out);
}

void __fastpackwithoutmask64(const uint64_t *__restrict__ in,
                             uint32_t *__restrict__ out) {
  for (int i = 0; i < 32; ++i) {
    out[2 * i] = static_cast<uint32_t>(in[i]);
    out[2 * i + 1] = in[i] >> 32;
  }
}

}  // anonymous namespace

void fastunpack(const uint32_t *__restrict__ in,
                uint32_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastunpack0(in, out);
    break;
  case 1:
    __fastunpack1(in, out);
    break;
  case 2:
    __fastunpack2(in, out);
    break;
  case 3:
    __fastunpack3(in, out);
    break;
  case 4:
    __fastunpack4(in, out);
    break;
  case 5:
    __fastunpack5(in, out);
    break;
  case 6:
    __fastunpack6(in, out);
    break;
  case 7:
    __fastunpack7(in, out);
    break;
  case 8:
    __fastunpack8(in, out);
    break;
  case 9:
    __fastunpack9(in, out);
    break;
  case 10:
    __fastunpack10(in, out);
    break;
  case 11:
    __fastunpack11(in, out);
    break;
  case 12:
    __fastunpack12(in, out);
    break;
  case 13:
    __fastunpack13(in, out);
    break;
  case 14:
    __fastunpack14(in, out);
    break;
  case 15:
    __fastunpack15(in, out);
    break;
  case 16:
    __fastunpack16(in, out);
    break;
  case 17:
    __fastunpack17(in, out);
    break;
  case 18:
    __fastunpack18(in, out);
    break;
  case 19:
    __fastunpack19(in, out);
    break;
  case 20:
    __fastunpack20(in, out);
    break;
  case 21:
    __fastunpack21(in, out);
    break;
  case 22:
    __fastunpack22(in, out);
    break;
  case 23:
    __fastunpack23(in, out);
    break;
  case 24:
    __fastunpack24(in, out);
    break;
  case 25:
    __fastunpack25(in, out);
    break;
  case 26:
    __fastunpack26(in, out);
    break;
  case 27:
    __fastunpack27(in, out);
    break;
  case 28:
    __fastunpack28(in, out);
    break;
  case 29:
    __fastunpack29(in, out);
    break;
  case 30:
    __fastunpack30(in, out);
    break;
  case 31:
    __fastunpack31(in, out);
    break;
  case 32:
    __fastunpack32(in, out);
    break;
  default:
    DCHECK(false);
    break;
  }
}

void fastunpack(const uint32_t *__restrict__ in,
                uint64_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastunpack0(in, out);
    break;
  case 1:
    __fastunpack1(in, out);
    break;
  case 2:
    __fastunpack2(in, out);
    break;
  case 3:
    __fastunpack3(in, out);
    break;
  case 4:
    __fastunpack4(in, out);
    break;
  case 5:
    __fastunpack5(in, out);
    break;
  case 6:
    __fastunpack6(in, out);
    break;
  case 7:
    __fastunpack7(in, out);
    break;
  case 8:
    __fastunpack8(in, out);
    break;
  case 9:
    __fastunpack9(in, out);
    break;
  case 10:
    __fastunpack10(in, out);
    break;
  case 11:
    __fastunpack11(in, out);
    break;
  case 12:
    __fastunpack12(in, out);
    break;
  case 13:
    __fastunpack13(in, out);
    break;
  case 14:
    __fastunpack14(in, out);
    break;
  case 15:
    __fastunpack15(in, out);
    break;
  case 16:
    __fastunpack16(in, out);
    break;
  case 17:
    __fastunpack17(in, out);
    break;
  case 18:
    __fastunpack18(in, out);
    break;
  case 19:
    __fastunpack19(in, out);
    break;
  case 20:
    __fastunpack20(in, out);
    break;
  case 21:
    __fastunpack21(in, out);
    break;
  case 22:
    __fastunpack22(in, out);
    break;
  case 23:
    __fastunpack23(in, out);
    break;
  case 24:
    __fastunpack24(in, out);
    break;
  case 25:
    __fastunpack25(in, out);
    break;
  case 26:
    __fastunpack26(in, out);
    break;
  case 27:
    __fastunpack27(in, out);
    break;
  case 28:
    __fastunpack28(in, out);
    break;
  case 29:
    __fastunpack29(in, out);
    break;
  case 30:
    __fastunpack30(in, out);
    break;
  case 31:
    __fastunpack31(in, out);
    break;
  case 32:
    __fastunpack32(in, out);
    break;
  case 33:
    __fastunpack33(in, out);
    break;
  case 34:
    __fastunpack34(in, out);
    break;
  case 35:
    __fastunpack35(in, out);
    break;
  case 36:
    __fastunpack36(in, out);
    break;
  case 37:
    __fastunpack37(in, out);
    break;
  case 38:
    __fastunpack38(in, out);
    break;
  case 39:
    __fastunpack39(in, out);
    break;
  case 40:
    __fastunpack40(in, out);
    break;
  case 41:
    __fastunpack41(in, out);
    break;
  case 42:
    __fastunpack42(in, out);
    break;
  case 43:
    __fastunpack43(in, out);
    break;
  case 44:
    __fastunpack44(in, out);
    break;
  case 45:
    __fastunpack45(in, out);
    break;
  case 46:
    __fastunpack46(in, out);
    break;
  case 47:
    __fastunpack47(in, out);
    break;
  case 48:
    __fastunpack48(in, out);
    break;
  case 49:
    __fastunpack49(in, out);
    break;
  case 50:
    __fastunpack50(in, out);
    break;
  case 51:
    __fastunpack51(in, out);
    break;
  case 52:
    __fastunpack52(in, out);
    break;
  case 53:
    __fastunpack53(in, out);
    break;
  case 54:
    __fastunpack54(in, out);
    break;
  case 55:
    __fastunpack55(in, out);
    break;
  case 56:
    __fastunpack56(in, out);
    break;
  case 57:
    __fastunpack57(in, out);
    break;
  case 58:
    __fastunpack58(in, out);
    break;
  case 59:
    __fastunpack59(in, out);
    break;
  case 60:
    __fastunpack60(in, out);
    break;
  case 61:
    __fastunpack61(in, out);
    break;
  case 62:
    __fastunpack62(in, out);
    break;
  case 63:
    __fastunpack63(in, out);
    break;
  case 64:
    __fastunpack64(in, out);
    break;
  default:
    DCHECK(false) << bit;
    break;
  }
}

void fastpack(const uint32_t *__restrict__ in,
              uint32_t *__restrict__ out, const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastpack0(in, out);
    break;
  case 1:
    __fastpack1(in, out);
    break;
  case 2:
    __fastpack2(in, out);
    break;
  case 3:
    __fastpack3(in, out);
    break;
  case 4:
    __fastpack4(in, out);
    break;
  case 5:
    __fastpack5(in, out);
    break;
  case 6:
    __fastpack6(in, out);
    break;
  case 7:
    __fastpack7(in, out);
    break;
  case 8:
    __fastpack8(in, out);
    break;
  case 9:
    __fastpack9(in, out);
    break;
  case 10:
    __fastpack10(in, out);
    break;
  case 11:
    __fastpack11(in, out);
    break;
  case 12:
    __fastpack12(in, out);
    break;
  case 13:
    __fastpack13(in, out);
    break;
  case 14:
    __fastpack14(in, out);
    break;
  case 15:
    __fastpack15(in, out);
    break;
  case 16:
    __fastpack16(in, out);
    break;
  case 17:
    __fastpack17(in, out);
    break;
  case 18:
    __fastpack18(in, out);
    break;
  case 19:
    __fastpack19(in, out);
    break;
  case 20:
    __fastpack20(in, out);
    break;
  case 21:
    __fastpack21(in, out);
    break;
  case 22:
    __fastpack22(in, out);
    break;
  case 23:
    __fastpack23(in, out);
    break;
  case 24:
    __fastpack24(in, out);
    break;
  case 25:
    __fastpack25(in, out);
    break;
  case 26:
    __fastpack26(in, out);
    break;
  case 27:
    __fastpack27(in, out);
    break;
  case 28:
    __fastpack28(in, out);
    break;
  case 29:
    __fastpack29(in, out);
    break;
  case 30:
    __fastpack30(in, out);
    break;
  case 31:
    __fastpack31(in, out);
    break;
  case 32:
    __fastpack32(in, out);
    break;
  default:
    DCHECK(false) << bit;
    break;
  }
}

void fastpack(const uint64_t *__restrict__ in,
                     uint32_t *__restrict__ out, const uint32_t bit) {
  switch (bit) {
  case 0:
    __fastpack0(in, out);
    break;
  case 1:
    __fastpack1(in, out);
    break;
  case 2:
    __fastpack2(in, out);
    break;
  case 3:
    __fastpack3(in, out);
    break;
  case 4:
    __fastpack4(in, out);
    break;
  case 5:
    __fastpack5(in, out);
    break;
  case 6:
    __fastpack6(in, out);
    break;
  case 7:
    __fastpack7(in, out);
    break;
  case 8:
    __fastpack8(in, out);
    break;
  case 9:
    __fastpack9(in, out);
    break;
  case 10:
    __fastpack10(in, out);
    break;
  case 11:
    __fastpack11(in, out);
    break;
  case 12:
    __fastpack12(in, out);
    break;
  case 13:
    __fastpack13(in, out);
    break;
  case 14:
    __fastpack14(in, out);
    break;
  case 15:
    __fastpack15(in, out);
    break;
  case 16:
    __fastpack16(in, out);
    break;
  case 17:
    __fastpack17(in, out);
    break;
  case 18:
    __fastpack18(in, out);
    break;
  case 19:
    __fastpack19(in, out);
    break;
  case 20:
    __fastpack20(in, out);
    break;
  case 21:
    __fastpack21(in, out);
    break;
  case 22:
    __fastpack22(in, out);
    break;
  case 23:
    __fastpack23(in, out);
    break;
  case 24:
    __fastpack24(in, out);
    break;
  case 25:
    __fastpack25(in, out);
    break;
  case 26:
    __fastpack26(in, out);
    break;
  case 27:
    __fastpack27(in, out);
    break;
  case 28:
    __fastpack28(in, out);
    break;
  case 29:
    __fastpack29(in, out);
    break;
  case 30:
    __fastpack30(in, out);
    break;
  case 31:
    __fastpack31(in, out);
    break;
  case 32:
    __fastpack32(in, out);
    break;
  case 33:
    __fastpack33(in, out);
    break;
  case 34:
    __fastpack34(in, out);
    break;
  case 35:
    __fastpack35(in, out);
    break;
  case 36:
    __fastpack36(in, out);
    break;
  case 37:
    __fastpack37(in, out);
    break;
  case 38:
    __fastpack38(in, out);
    break;
  case 39:
    __fastpack39(in, out);
    break;
  case 40:
    __fastpack40(in, out);
    break;
  case 41:
    __fastpack41(in, out);
    break;
  case 42:
    __fastpack42(in, out);
    break;
  case 43:
    __fastpack43(in, out);
    break;
  case 44:
    __fastpack44(in, out);
    break;
  case 45:
    __fastpack45(in, out);
    break;
  case 46:
    __fastpack46(in, out);
    break;
  case 47:
    __fastpack47(in, out);
    break;
  case 48:
    __fastpack48(in, out);
    break;
  case 49:
    __fastpack49(in, out);
    break;
  case 50:
    __fastpack50(in, out);
    break;
  case 51:
    __fastpack51(in, out);
    break;
  case 52:
    __fastpack52(in, out);
    break;
  case 53:
    __fastpack53(in, out);
    break;
  case 54:
    __fastpack54(in, out);
    break;
  case 55:
    __fastpack55(in, out);
    break;
  case 56:
    __fastpack56(in, out);
    break;
  case 57:
    __fastpack57(in, out);
    break;
  case 58:
    __fastpack58(in, out);
    break;
  case 59:
    __fastpack59(in, out);
    break;
  case 60:
    __fastpack60(in, out);
    break;
  case 61:
    __fastpack61(in, out);
    break;
  case 62:
    __fastpack62(in, out);
    break;
  case 63:
    __fastpack63(in, out);
    break;
  case 64:
    __fastpack64(in, out);
    break;
  default:
    DCHECK(false) << bit;
    break;
  }
}

/*assumes that integers fit in the prescribed number of bits*/
void fastpackwithoutmask(const uint32_t *__restrict__ in,
                         uint32_t *__restrict__ out,
                         const uint32_t bit) {
  // Could have used function pointers instead of switch.
  // Switch calls do offer the compiler more opportunities for optimization in
  // theory. In this case, it makes no difference with a good compiler.
  switch (bit) {
  case 0:
    __fastpackwithoutmask0(in, out);
    break;
  case 1:
    __fastpackwithoutmask1(in, out);
    break;
  case 2:
    __fastpackwithoutmask2(in, out);
    break;
  case 3:
    __fastpackwithoutmask3(in, out);
    break;
  case 4:
    __fastpackwithoutmask4(in, out);
    break;
  case 5:
    __fastpackwithoutmask5(in, out);
    break;
  case 6:
    __fastpackwithoutmask6(in, out);
    break;
  case 7:
    __fastpackwithoutmask7(in, out);
    break;
  case 8:
    __fastpackwithoutmask8(in, out);
    break;
  case 9:
    __fastpackwithoutmask9(in, out);
    break;
  case 10:
    __fastpackwithoutmask10(in, out);
    break;
  case 11:
    __fastpackwithoutmask11(in, out);
    break;
  case 12:
    __fastpackwithoutmask12(in, out);
    break;
  case 13:
    __fastpackwithoutmask13(in, out);
    break;
  case 14:
    __fastpackwithoutmask14(in, out);
    break;
  case 15:
    __fastpackwithoutmask15(in, out);
    break;
  case 16:
    __fastpackwithoutmask16(in, out);
    break;
  case 17:
    __fastpackwithoutmask17(in, out);
    break;
  case 18:
    __fastpackwithoutmask18(in, out);
    break;
  case 19:
    __fastpackwithoutmask19(in, out);
    break;
  case 20:
    __fastpackwithoutmask20(in, out);
    break;
  case 21:
    __fastpackwithoutmask21(in, out);
    break;
  case 22:
    __fastpackwithoutmask22(in, out);
    break;
  case 23:
    __fastpackwithoutmask23(in, out);
    break;
  case 24:
    __fastpackwithoutmask24(in, out);
    break;
  case 25:
    __fastpackwithoutmask25(in, out);
    break;
  case 26:
    __fastpackwithoutmask26(in, out);
    break;
  case 27:
    __fastpackwithoutmask27(in, out);
    break;
  case 28:
    __fastpackwithoutmask28(in, out);
    break;
  case 29:
    __fastpackwithoutmask29(in, out);
    break;
  case 30:
    __fastpackwithoutmask30(in, out);
    break;
  case 31:
    __fastpackwithoutmask31(in, out);
    break;
  case 32:
    __fastpackwithoutmask32(in, out);
    break;
  default:
    DCHECK(false) << bit;
    break;
  }
}

 void fastpackwithoutmask(const uint64_t *__restrict__ in,
                                uint32_t *__restrict__ out,
                                const uint32_t bit) {
  switch (bit) {
  case 0:
    __fastpackwithoutmask0(in, out);
    break;
  case 1:
    __fastpackwithoutmask1(in, out);
    break;
  case 2:
    __fastpackwithoutmask2(in, out);
    break;
  case 3:
    __fastpackwithoutmask3(in, out);
    break;
  case 4:
    __fastpackwithoutmask4(in, out);
    break;
  case 5:
    __fastpackwithoutmask5(in, out);
    break;
  case 6:
    __fastpackwithoutmask6(in, out);
    break;
  case 7:
    __fastpackwithoutmask7(in, out);
    break;
  case 8:
    __fastpackwithoutmask8(in, out);
    break;
  case 9:
    __fastpackwithoutmask9(in, out);
    break;
  case 10:
    __fastpackwithoutmask10(in, out);
    break;
  case 11:
    __fastpackwithoutmask11(in, out);
    break;
  case 12:
    __fastpackwithoutmask12(in, out);
    break;
  case 13:
    __fastpackwithoutmask13(in, out);
    break;
  case 14:
    __fastpackwithoutmask14(in, out);
    break;
  case 15:
    __fastpackwithoutmask15(in, out);
    break;
  case 16:
    __fastpackwithoutmask16(in, out);
    break;
  case 17:
    __fastpackwithoutmask17(in, out);
    break;
  case 18:
    __fastpackwithoutmask18(in, out);
    break;
  case 19:
    __fastpackwithoutmask19(in, out);
    break;
  case 20:
    __fastpackwithoutmask20(in, out);
    break;
  case 21:
    __fastpackwithoutmask21(in, out);
    break;
  case 22:
    __fastpackwithoutmask22(in, out);
    break;
  case 23:
    __fastpackwithoutmask23(in, out);
    break;
  case 24:
    __fastpackwithoutmask24(in, out);
    break;
  case 25:
    __fastpackwithoutmask25(in, out);
    break;
  case 26:
    __fastpackwithoutmask26(in, out);
    break;
  case 27:
    __fastpackwithoutmask27(in, out);
    break;
  case 28:
    __fastpackwithoutmask28(in, out);
    break;
  case 29:
    __fastpackwithoutmask29(in, out);
    break;
  case 30:
    __fastpackwithoutmask30(in, out);
    break;
  case 31:
    __fastpackwithoutmask31(in, out);
    break;
  case 32:
    __fastpackwithoutmask32(in, out);
    break;
  case 33:
    __fastpackwithoutmask33(in, out);
    break;
  case 34:
    __fastpackwithoutmask34(in, out);
    break;
  case 35:
    __fastpackwithoutmask35(in, out);
    break;
  case 36:
    __fastpackwithoutmask36(in, out);
    break;
  case 37:
    __fastpackwithoutmask37(in, out);
    break;
  case 38:
    __fastpackwithoutmask38(in, out);
    break;
  case 39:
    __fastpackwithoutmask39(in, out);
    break;
  case 40:
    __fastpackwithoutmask40(in, out);
    break;
  case 41:
    __fastpackwithoutmask41(in, out);
    break;
  case 42:
    __fastpackwithoutmask42(in, out);
    break;
  case 43:
    __fastpackwithoutmask43(in, out);
    break;
  case 44:
    __fastpackwithoutmask44(in, out);
    break;
  case 45:
    __fastpackwithoutmask45(in, out);
    break;
  case 46:
    __fastpackwithoutmask46(in, out);
    break;
  case 47:
    __fastpackwithoutmask47(in, out);
    break;
  case 48:
    __fastpackwithoutmask48(in, out);
    break;
  case 49:
    __fastpackwithoutmask49(in, out);
    break;
  case 50:
    __fastpackwithoutmask50(in, out);
    break;
  case 51:
    __fastpackwithoutmask51(in, out);
    break;
  case 52:
    __fastpackwithoutmask52(in, out);
    break;
  case 53:
    __fastpackwithoutmask53(in, out);
    break;
  case 54:
    __fastpackwithoutmask54(in, out);
    break;
  case 55:
    __fastpackwithoutmask55(in, out);
    break;
  case 56:
    __fastpackwithoutmask56(in, out);
    break;
  case 57:
    __fastpackwithoutmask57(in, out);
    break;
  case 58:
    __fastpackwithoutmask58(in, out);
    break;
  case 59:
    __fastpackwithoutmask59(in, out);
    break;
  case 60:
    __fastpackwithoutmask60(in, out);
    break;
  case 61:
    __fastpackwithoutmask61(in, out);
    break;
  case 62:
    __fastpackwithoutmask62(in, out);
    break;
  case 63:
    __fastpackwithoutmask63(in, out);
    break;
  case 64:
    __fastpackwithoutmask64(in, out);
    break;
  default:
    DCHECK(false) << bit;
    break;
  }
}

} // namespace bp
} // namespace cfile
} // namespace kudu
