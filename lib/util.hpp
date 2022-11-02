#pragma once

#include <atomic>
#include <random>

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
