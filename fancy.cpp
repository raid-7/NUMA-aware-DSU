#include "lib/numa.hpp"
#include "lib/stats.hpp"
#include "lib/timer.hpp"
#include "lib/graphs.h"
#include "lib/util.hpp"

#include "DSU.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_ParallelUnions_no_imm.h"
#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Usual_noimm.h"

#include <CLI/App.hpp>
#include <CLI/Formatter.hpp>
#include <CLI/Config.hpp>

#include <iostream>
#include <iomanip>
#include <vector>
#include <thread>
#include <random>
#include <span>
#include <barrier>


struct Request {
    bool SameSetRequest;
    int u, v;

    void Apply(DSU* dsu) const {
        if (SameSetRequest) {
            Blackhole(dsu->SameSet(u, v));
        } else {
            dsu->Union(u, v);
        }
    }
};

struct StaticWorkload {
    std::vector<Request> PreHeatRequests;
    std::vector<std::vector<Request>> ThreadRequests;
    size_t N;
};

class Benchmark {
public:
    Benchmark(NUMAContext* ctx)
        : Ctx_(ctx)
    {}

    void Run(DSU* dsu, const StaticWorkload& workload) {
        size_t numThreads = workload.ThreadRequests.size();
        std::barrier barrier(numThreads);
        dsu->ReInit();
        ApplyRequests(dsu, workload.PreHeatRequests);
        dsu->setStepsCount(0);
        size_t resultsOffset = AverageTimeResults_.size();
        AverageTimeResults_.resize(resultsOffset + numThreads, 0.0);
        Ctx_->StartNThreads(
            [this, &barrier, &workload, dsu, resultsOffset]() {
                barrier.arrive_and_wait();
                double avgNs = ThreadWork(dsu, workload.ThreadRequests[NUMAContext::CurrentThreadId()]);
                AverageTimeResults_[resultsOffset + NUMAContext::CurrentThreadId()] = avgNs;
            },
            numThreads
        );
        Ctx_->Join();
    }

    double ThreadWork(DSU* dsu, std::span<const Request> requests) {
        Timer timer;
        ApplyRequests(dsu, requests);
        return timer.Get<std::chrono::nanoseconds>().count() * 1. / requests.size();
    }

    Stats<double> CollectAverageTimeStats() {
        Stats<double> res = stats(AverageTimeResults_.begin(), AverageTimeResults_.end());
        AverageTimeResults_.clear();
        return res;
    }

private:
    void ApplyRequests(DSU* dsu, std::span<const Request> requests) const {
        for (const auto& request : requests) {
            request.Apply(dsu);
            RandomAdditionalWork(AdditionalWork_);
        }
    }

private:
    NUMAContext* Ctx_;
    std::vector<double> AverageTimeResults_;
    double AdditionalWork_ = 2.0;
};

StaticWorkload BuildTotallyRandomWorkload(size_t N, size_t E,
                                          size_t numThreads, double sameSetFraction, double edgesPerThreadFraction) {
    Graph g = graphRandom(N, E);

    std::vector<std::vector<Request>> threadWork;
    std::bernoulli_distribution sameSetDistribution(sameSetFraction);
    std::uniform_int_distribution<size_t> edgeDistribution(0, E-1);
    for (size_t i = 0; i < numThreads; ++i) {
        threadWork.emplace_back();
        auto& requests = threadWork.back();
        for (size_t j = 0; j < static_cast<size_t>(E * edgesPerThreadFraction); ++j) {
            Edge e = g.Edges[edgeDistribution(TlRandom)];
            requests.push_back({sameSetDistribution(TlRandom), e.u, e.v});
        }
    }

    return StaticWorkload{
        {},
        std::move(threadWork),
        N
    };
}

StaticWorkload BuildComponentsRandomWorkload(size_t numComponents, size_t singleComponentN, size_t singleComponentE,
                                             size_t interPairE, double sameSetFraction) {
    std::vector<std::vector<Request>> threadWork(numComponents);
    std::bernoulli_distribution sameSetDistribution(sameSetFraction);
    for (size_t componentId = 0; componentId < numComponents; ++componentId) {
        size_t minComponentVertex = singleComponentN * componentId;
        std::uniform_int_distribution<int> uvDistribution(minComponentVertex, minComponentVertex + singleComponentN - 1);
        for (size_t i = 0; i < singleComponentE; ++i) {
            int u = uvDistribution(TlRandom);
            int v = uvDistribution(TlRandom);
            threadWork[componentId].push_back({sameSetDistribution(TlRandom), u, v});
        }
    }
    for (size_t c1 = 0; c1 < numComponents; ++c1) {
        size_t minC1Vertex = singleComponentN * c1;
        std::uniform_int_distribution<int> uDistribution(minC1Vertex, minC1Vertex + singleComponentN - 1);
        for (size_t c2 = c1 + 1; c2 < numComponents; ++c2) {
            size_t minC2Vertex = singleComponentN * c2;
            std::uniform_int_distribution<int> vDistribution(minC2Vertex, minC2Vertex + singleComponentN - 1);
            for (size_t i = 0; i < interPairE; ++i) {
                int u = uDistribution(TlRandom);
                int v = vDistribution(TlRandom);
                threadWork[c1].push_back({sameSetDistribution(TlRandom), u, v});
                threadWork[c2].push_back({sameSetDistribution(TlRandom), u, v});
            }
        }
    }
    for (auto& component : threadWork) {
        Shuffle(component);
    }
    Shuffle(threadWork);
    return StaticWorkload{
            {},
            std::move(threadWork),
            numComponents * singleComponentN
    };
}

int main() {
    NUMAContext ctx(8, 2, true);
    Benchmark benchmark(&ctx);
    size_t N = 1'000'000, E = 20'000'000;
    DSU_ParallelUnions dsuPU(N, ctx.NodeCount());
    for (size_t i = 0; i < 3; ++i) {
        StaticWorkload workload = BuildTotallyRandomWorkload(N, E, 8, 0.8, 1. / 8);
        benchmark.Run(&dsuPU, workload);
    }
    Stats<double> s = benchmark.CollectAverageTimeStats();
    std::cout << std::setprecision(3) << s.mean << " " << s.stddev << " ns/op" << std::endl;
    return 0;
}
