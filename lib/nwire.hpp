#pragma once

#include <atomic>


template <size_t size, size_t stride = 64 / sizeof(int)>
class NWire {
public:
    NWire() {
        for (size_t i = 0; i < size; ++i) {
            wire(i).store(W_IDLE);
        }
    }

    bool publishRequest(int wireId, int req) {
        int expected = W_IDLE;
        return wire(wireId).compare_exchange_strong(expected, wireRequest(req));
    }

    int waitAndGet(int wireId) {
        int state = wire(wireId).load();
        size_t cnt = 0;
        while (isWireRequest(state)) {
#if defined(__x86_64__)
            __builtin_ia32_pause();
#endif
            state = wire(wireId).load();
            if (++cnt % 4'000'000 == 0) {
                std::cout << "Thread " << NUMAContext::CurrentThreadId() << " is spinning on wire " << wireId << std::endl;
            }
        }
        wire(wireId).store(W_IDLE);
        return wireData(state); // response
    }

    bool readRequest(int wireId, int& req) {
        int state = wire(wireId).load();
        if (isWireRequest(state)) {
            req = wireData(state);
            return true;
        }
        return false;
    }

    void satisfyRequest(int wireId, int res) {
        wire(wireId).store(wireResponse(res));
    }

    bool poison(int wireId) {
        int expected = W_IDLE;
        if (wire(wireId).compare_exchange_strong(expected, W_POISON))
            return true;
        return wire(wireId) == W_POISON;
    }

private:
    inline std::atomic<int>& wire(int i) {
        return wire_[i * stride];
    }

    // wire states: idle, request, response
    static inline bool isWireRequest(int r) {
        return !(r & 1) && r != W_POISON;
    }

    static inline int wireRequest(int data) {
        return data << 1;
    }

    static inline int wireResponse(int data) {
        return (data << 1) | 1;
    }

    static inline int wireData(int r) {
        return r >> 1;
    }

private:
    constexpr static int W_IDLE = -1;
    constexpr static int W_POISON = (-1) ^ 1;

    std::atomic<int> wire_[size * stride];
};
