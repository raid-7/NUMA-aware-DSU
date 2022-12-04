#pragma once

#include <atomic>
#include <random>
#include <iostream>
#include <stdexcept>


#define VERIFY(cond, message) if (!(cond)) {                      \
    std::cerr << "Fatal: " << message << std::endl; std::abort(); \
}

#define REQUIRE(cond, message) if (!(cond)) {                     \
    throw std::runtime_error(message);                            \
}

extern thread_local std::mt19937 TlRandom;

void Blackhole(int*);

template<class T> requires (sizeof(T) >= 4)
static void Blackhole(T value) {
    Blackhole(reinterpret_cast<int*>(&value));
}

template<class T> requires (sizeof(T) < 4)
static void Blackhole(T value) {
    char* c = reinterpret_cast<char*>(&value);
    int v = *c;
    Blackhole(&v);
}

void RandomAdditionalWork(double mean);

template <class V>
void Shuffle(V& data) {
    std::shuffle(data.begin(), data.end(), TlRandom);
}
