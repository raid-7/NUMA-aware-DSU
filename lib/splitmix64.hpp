#pragma once

#include <cstdint>
#include <numeric>


class SplitMix64 {
public:
    using result_type = uint64_t;

    constexpr static uint64_t min() {
        return std::numeric_limits<result_type>::min();
    }
    constexpr static uint64_t max() {
        return std::numeric_limits<result_type>::max();
    }

    SplitMix64(uint64_t x = 42)
        : x(x)
    {}

    uint64_t operator()() {
        uint64_t z = (x += 0x9E3779B97F4A7C15ull);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        return z ^ (z >> 31);
    }

private:
    uint64_t x;
};
