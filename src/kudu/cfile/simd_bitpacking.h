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

#include <cstdint>
#include <xmmintrin.h>

#include "kudu/gutil/macros.h"

namespace kudu {
namespace cfile {
namespace bp {

// Pack 128 unsigned integers. The integers should already have been
// verified to fit within the given number of bits.
//
// 'in' should have 128 values.
// 'out' should have room for 128*num_bits bits = 16*num_bits bytes
void simd_fastpackwithoutmask(
    const uint64_t* __restrict__ in,
    __m128i* __restrict__ out,
    int num_bits);

void simd_fastpackwithoutmask(
    const uint32_t* __restrict__ in,
    __m128i* __restrict__ out,
    int num_bits);

void simd_fastpackwithoutmask(
    const uint16_t* __restrict__ in,
    __m128i* __restrict__ out,
    int num_bits);

void simd_fastpackwithoutmask(
    const uint8_t* __restrict__ in,
    __m128i* __restrict__ out,
    int num_bits);

// Pack 32 unsigned integers in the scalar layout. The integers should already have been
// verified to fit within the given number of bits.
//
// 'in' should have 32 values.
// 'out' should have room for 32*num_bits bits = 4*num_bits bytes
void fastpackwithoutmask(
    const uint64_t* __restrict__ in,
    uint32_t* __restrict__ out,
    int num_bits);

void fastpackwithoutmask(
    const uint32_t* __restrict__ in,
    uint32_t* __restrict__ out,
    int num_bits);

void fastpackwithoutmask(
    const uint16_t* __restrict__ in,
    uint32_t* __restrict__ out,
    int num_bits);

void fastpackwithoutmask(
    const uint8_t* __restrict__ in,
    uint32_t* __restrict__ out,
    int num_bits);




// Unpack 128 unsigned integers.
// 'in' should have 128*num_bits bits = 16*num_bits bytes
// 'out' should have room for 128 values.
void simd_fastunpack(
    const __m128i* __restrict__ in,
    uint64_t* __restrict__ out,
    int num_bits);

void simd_fastunpack(
    const __m128i* __restrict__ in,
    uint32_t* __restrict__ out,
    int num_bits);

void simd_fastunpack(
    const __m128i* __restrict__ in,
    uint16_t* __restrict__ out,
    int num_bits);

void simd_fastunpack(
    const __m128i* __restrict__ in,
    uint8_t* __restrict__ out,
    int num_bits);



} // namespace bp
} // namespace cfile
} // namespace kudu
