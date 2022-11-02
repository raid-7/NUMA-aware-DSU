#pragma once

#include <chrono>

class Timer {
public:
    using RawDuration = std::chrono::high_resolution_clock::duration;

    Timer() {
        Reset();
    }

    void Reset() {
        Start_ = std::chrono::high_resolution_clock::now();
    }

    RawDuration Raw() const {
        return std::chrono::high_resolution_clock::now() - Start_;
    }

    template<class Duration>
    Duration Get() const {
        return std::chrono::duration_cast<Duration>(Raw());
    }

private:
    std::chrono::high_resolution_clock::time_point Start_{};
};