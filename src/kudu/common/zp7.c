// TODO(todd): import this via thirdparty instead? the upstream projcet
// doesn't have a header, though.

#include <stdint.h>

#define HAS_CLMUL
#define HAS_POPCNT

#if defined(HAS_CLMUL) || defined(HAS_BZHI) || defined(HAS_POPCNT)
#   include <immintrin.h>
#endif

#define N_BITS      (6)

typedef struct {
    uint64_t mask;
    uint64_t ppp_bit[N_BITS];
} zp7_masks_64_t;

#ifndef HAS_CLMUL
// If we don't have access to the CLMUL instruction, emulate it with
// shifts and XORs
static inline uint64_t prefix_sum(uint64_t x) {
    for (int i = 0; i < N_BITS; i++)
        x ^= x << (1 << i);
    return x;
}
#endif

#ifndef HAS_POPCNT
// POPCNT polyfill. See this page for information about the algorithm:
// https://www.chessprogramming.org/Population_Count#SWAR-Popcount
uint64_t popcnt_64(uint64_t x) {
    const uint64_t m_1 = 0x5555555555555555LLU;
    const uint64_t m_2 = 0x3333333333333333LLU;
    const uint64_t m_4 = 0x0f0f0f0f0f0f0f0fLLU;
    x = x - ((x >> 1) & m_1);
    x = (x & m_2) + ((x >> 2) & m_2);
    x = (x + (x >> 4)) & m_4;
    return (x * 0x0101010101010101LLU) >> 56;
}
#endif

// Parallel-prefix-popcount. This is used by both the PEXT/PDEP polyfills.
// It can also be called separately and cached, if the mask values will be used
// more than once (these can be shared across PEXT and PDEP calls if they use
// the same masks).
__attribute__((target("pclmul")))
zp7_masks_64_t zp7_ppp_64(uint64_t mask) {
    zp7_masks_64_t r;
    r.mask = mask;

    // Count *unset* bits
    mask = ~mask;

#ifdef HAS_CLMUL
    // Move the mask and -2 to XMM registers for CLMUL
    __m128i m = _mm_cvtsi64_si128(mask);
    __m128i neg_2 = _mm_cvtsi64_si128(-2LL);
    for (int i = 0; i < N_BITS - 1; i++) {
        // Do a 1-bit parallel prefix popcount, shifted left by 1,
        // in one carry-less multiply by -2.
        __m128i bit = _mm_clmulepi64_si128(m, neg_2, 0);
        r.ppp_bit[i] = _mm_cvtsi128_si64(bit);

        // Get the carry bit of the 1-bit parallel prefix popcount. On
        // the next iteration, we will sum this bit to get the next mask
        m = _mm_and_si128(m, bit);
    }
    // For the last iteration, we can use a regular multiply by -2 instead of a
    // carry-less one (or rather, a strength reduction of that, with
    // neg/add/etc), since there can't be any carries anyways. That is because
    // the last value of m (which has one bit set for every 32nd unset mask bit)
    // has at most two bits set in it, when mask is zero and thus there are 64
    // bits set in ~mask. If two bits are set, one of them is the top bit, which
    // gets shifted out, since we're counting bits below each mask bit.
    r.ppp_bit[N_BITS - 1] = -_mm_cvtsi128_si64(m) << 1;
#else
    for (int i = 0; i < N_BITS - 1; i++) {
        // Do a 1-bit parallel prefix popcount, shifted left by 1
        uint64_t bit = prefix_sum(mask << 1);
        r.ppp_bit[i] = bit;

        // Get the carry bit of the 1-bit parallel prefix popcount. On
        // the next iteration, we will sum this bit to get the next mask
        mask &= bit;
    }
    // The last iteration won't carry, so just use neg/shift. See the CLMUL
    // case above for justification.
    r.ppp_bit[N_BITS - 1] = -mask << 1;
#endif

    return r;
}

// PEXT

uint64_t zp7_pext_pre_64(uint64_t a, const zp7_masks_64_t *masks) {
    // Mask only the bits that are set in the input mask. Otherwise they collide
    // with input bits and screw everything up
    a &= masks->mask;

    // For each bit in the PPP, shift right only those bits that are set in
    // that bit's mask
    for (int i = 0; i < N_BITS; i++) {
        uint64_t shift = 1 << i;
        uint64_t bit = masks->ppp_bit[i];
        // Shift only the input bits that are set in
        a = (a & ~bit) | ((a & bit) >> shift);
    }
    return a;
}

uint64_t zp7_pext_64(uint64_t a, uint64_t mask) {
    zp7_masks_64_t masks = zp7_ppp_64(mask);
    return zp7_pext_pre_64(a, &masks);
}
