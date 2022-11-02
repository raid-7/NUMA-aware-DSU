#pragma once

#include <vector>
#include <atomic>
#include <thread>
#include <stdexcept>

#include <sched.h>
#include <numa.h>
#include <pthread.h>


class NUMAContext;

static thread_local NUMAContext* NumaCtx;
static thread_local int ThreadId;
static thread_local int NumaNodeId;

class NUMAContext {
public:
    NUMAContext(size_t numCpu = std::thread::hardware_concurrency(),
            size_t numNuma = numa_max_node() + 1, bool tesingNumIds = false)
        : NumCpu_(numCpu)
        , NumNuma_(numNuma)
        , TestingNumaIds_(tesingNumIds)
    {}

    template <class R>
    void StartThread(R runnable) {
        int id = Threads_.size();
        Threads_.emplace_back(
                [this, runnable](int id) {
                    SetupNewThread(id);
                    runnable();
                },
                id
        );
    }

    template <class R>
    void StartNThreads(R runnable, size_t n) {
        for (size_t i = 0; i < n; ++i)
            StartThread(runnable);
    }

    size_t NodeCount() const {
        return NumNuma_;
    }

    size_t CpuCount() const {
        return NumCpu_;
    }

    void Join() {
        for (auto& thread : Threads_) {
            thread.join();
        }
        Threads_.clear();
    }

    ~NUMAContext() {
        Join();
    }

    static NUMAContext& CurrentCtx() {
        if (!NumaCtx)
            throw std::runtime_error("Context not set up");
        return *NumaCtx;
    }

    static int CurrentThreadId() {
        return ThreadId;
    }

    static int CurrentThreadNode() {
        return NumaNodeId;
    }

private:
    void SetupNewThread(int id) {
        NumaCtx = this;
        ThreadId = id;

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        int cpuId = id % (int) NumCpu_;
        CPU_SET(cpuId, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        NumaNodeId = TestingNumaIds_ ? (id * (int)NumNuma_ / (int)NumCpu_) : numa_node_of_cpu(cpuId);
    }

    std::vector<std::thread> Threads_;
    size_t NumCpu_;
    size_t NumNuma_;
    bool TestingNumaIds_;
};
