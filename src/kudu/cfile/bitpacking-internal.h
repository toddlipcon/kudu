/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire, http://lemire.me/en/
 */
#pragma once

#include <cstdint>

namespace kudu {
namespace cfile {
namespace bp {

// Functions in this file pack/unpack 32 ints at a time to a specified number
// of bits.
//
// The packed format 

////////////////////////////////////////////////////////////
// 32-bit
////////////////////////////////////////////////////////////

void fastpack(const uint32_t *__restrict__ in,
              uint32_t *__restrict__ out,
              const uint32_t bit);

void fastpackwithoutmask(const uint32_t *__restrict__ in,
                         uint32_t *__restrict__ out,
                         const uint32_t bit);

void fastunpack(const uint32_t *__restrict__ in,
                uint32_t *__restrict__ out, const uint32_t bit);

////////////////////////////////////////////////////////////
// 64-bit
////////////////////////////////////////////////////////////

void fastpackwithoutmask(const uint64_t *__restrict__ in,
                         uint32_t *__restrict__ out,
                         const uint32_t bit);

void fastpack(const uint64_t *__restrict__ in,
              uint32_t *__restrict__ out, const uint32_t bit);

void fastunpack(const uint32_t *__restrict__ in,
                uint64_t *__restrict__ out, const uint32_t bit);


} // namespace bp
} // namespace cfile
} // namespace kudu
