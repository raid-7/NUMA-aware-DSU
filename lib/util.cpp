#include "util.hpp"

#include <atomic>
#include <random>

static std::atomic<int> BlackholeValue;

thread_local std::mt19937 TlRandom{std::random_device()()};

void Blackhole(int* c) {
    if ((*c ^ 0x1E32234) == -17) {
        BlackholeValue.store(*c * 99, std::memory_order_relaxed);
    }
}

void RandomAdditionalWork(double mean) {
    if (mean <= 0.5)
        return;
    std::uniform_real_distribution<double> distribution{0., mean};
    while (distribution(TlRandom) >= 1.0);
}
