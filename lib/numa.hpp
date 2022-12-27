#pragma once

#include "util.hpp"

#include <vector>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <cmath>

#include <sched.h>
#include <numa.h>
#include <pthread.h>


bool IsNumaAvailable();

class NUMAContext;

//template <class T>
//class NUMAAllocator {
//public:
//    NUMAAllocator(NUMAContext* ctx) : Ctx_(ctx) {}
//
//    void* Allocate(size_t);
//
//    void Deallocate(void*, size_t);
//
//private:
//    NUMAContext* Ctx_;
//};

static thread_local NUMAContext* NumaCtx;
static thread_local int ThreadId;
static thread_local int NumaNodeId;

class NUMAContext {
public:
    NUMAContext(size_t fallbackNumNuma = 4)
            : NumCpu_(std::thread::hardware_concurrency())
            , NumNuma_(IsNumaAvailable() ? numa_max_node() + 1 : std::min(fallbackNumNuma, NumCpu_))
            , TestingNumaIds_(!IsNumaAvailable())
            , NumaAvailable_(IsNumaAvailable() && numa_max_node() > 0)
    {
    }

    void SetupForTests(size_t numCpu, size_t numNuma) {
        TestingNumaIds_ = true;
        NumCpu_ = numCpu;
        NumNuma_ = numNuma;
    }

    template <class R>
    void StartThread(R runnable) {
        int id = Threads_.size();
        Threads_.emplace_back(
                [this, runnable](int id) {
                    SetupNewThread(id);
                    ValidateTopology();
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

    size_t MaxConcurrency() const {
        return NumCpu_;
    }

    int NumaNodeForThread(int tid) const {
        return TestingNumaIds_ ? (tid * (int)NumNuma_ / (int)NumCpu_) : numa_node_of_cpu(tid % (int) NumCpu_);
    }

    void Join() {
        for (auto& thread : Threads_) {
            thread.join();
        }
        Threads_.clear();
    }

    void* Allocate(int nodeId, size_t size) const {
        if (NumaAvailable_) {
            int realNodeId = nodeId % (numa_max_node() + 1);
            return numa_alloc_onnode(size, realNodeId);
        } else {
            return ::malloc(size);
        }
    }

    void Free(void* ptr, size_t size) const {
        if (NumaAvailable_) {
            numa_free(ptr, size);
        } else {
            ::free(ptr);
        }
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

    void ValidateTopology() const {
        using namespace std::string_literals;
        VERIFY(NumaNodeForThread(CurrentThreadId()) == CurrentThreadNode(),
               "Actual NUMA node ("s
               + std::to_string(CurrentThreadNode()) +
               ") does not match the expected one ("s
               + std::to_string(NumaNodeForThread(CurrentThreadId())) +
               ")"s);
    }

    std::vector<std::thread> Threads_;
    size_t NumCpu_;
    size_t NumNuma_;
    bool TestingNumaIds_;
    bool NumaAvailable_;
};
