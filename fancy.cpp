#include "lib/numa.hpp"
#include "lib/stats.hpp"
#include "lib/timer.hpp"
#include "lib/graphs.h"
#include "lib/util.hpp"
#include "lib/csv.hpp"
#include "lib/parameters.hpp"

#include "DSU.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_ParallelUnions_no_imm.h"
#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Usual_noimm.h"
#include "implementations/SeveralDSU.h"

#include <CLI/App.hpp>
#include <CLI/Formatter.hpp>
#include <CLI/Config.hpp>

#include <iostream>
#include <iomanip>
#include <vector>
#include <thread>
#include <regex>
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
        ApplyRequests(dsu, workload.PreHeatRequests, false);
        size_t resultsOffset = ThroughputResults_[dsu].size();
        ThroughputResults_[dsu].resize(resultsOffset + numThreads, 0.0);
        Ctx_->StartNThreads(
            [this, &barrier, &workload, dsu, resultsOffset]() {
                barrier.arrive_and_wait();
                double avgNs = ThreadWork(dsu, workload.ThreadRequests[NUMAContext::CurrentThreadId()]);
                ThroughputResults_[dsu][resultsOffset + NUMAContext::CurrentThreadId()] = avgNs;
            },
            numThreads
        );
        Ctx_->Join();
    }

    double ThreadWork(DSU* dsu, std::span<const Request> requests) {
        constexpr size_t NS = 1'000'000'000ull;
        Timer timer;
        ApplyRequests(dsu, requests, true);
        return requests.size() * NS / timer.Get<std::chrono::nanoseconds>().count();
    }

    Stats<double> CollectThroughputStats(DSU* dsu) {
        Stats<double> res = stats(ThroughputResults_[dsu].begin(), ThroughputResults_[dsu].end());
        ThroughputResults_[dsu].clear();
        return res;
    }

private:
    void ApplyRequests(DSU* dsu, std::span<const Request> requests, bool useAdditionalWork) const {
        for (const auto& request : requests) {
            request.Apply(dsu);
            if (useAdditionalWork)
                RandomAdditionalWork(AdditionalWork_);
        }
    }

private:
    NUMAContext* Ctx_;
    std::map<DSU*, std::vector<double>> ThroughputResults_;
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
    // TODO create one component per node, not per thread
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

std::vector<std::unique_ptr<DSU>> GetAvailableDsus(size_t N, size_t node_count, const std::regex& filter) {
    std::vector<std::unique_ptr<DSU>> dsus;
    dsus.emplace_back(new DSU_Usual(N));
    //dsus.emplace_back(new DSU_Usual_NoImm(N));

    //dsus.emplace_back(new TwoDSU(N, node_count));
    dsus.emplace_back(new SeveralDSU(N, node_count));

    dsus.emplace_back(new DSU_ParallelUnions(N, node_count));
    //dsus.emplace_back(new DSU_ParallelUnions_NoImm(N, node_count));

//    dsus.emplace_back(new DSU_NO_SYNC(N, node_count));
    //dsus.emplace_back(new DSU_NO_SYNC_NoImm(N, node_count));

//    dsus.emplace_back(new DSU_Parts(N, node_count, owners));
    //dsus.emplace_back(new DSU_Parts_NoImm(N, node_count, owners));

//    dsus.emplace_back(new DSU_NoSync_Parts(N, node_count, owners));
    //dsus.emplace_back(new DSU_NoSync_Parts_NoImm(N, node_count, owners));

    auto end = std::remove_if(dsus.begin(), dsus.end(), [&filter](const std::unique_ptr<DSU>& dsu) {
        return !std::regex_match(dsu->ClassName(), filter);
    });
    dsus.resize(end - dsus.begin());
    return dsus;
}


void RunComponentsBenchmark(NUMAContext* ctx, CsvFile* out, const std::regex& filter,
                  size_t numWorkloads, size_t numIterationsPerWorkload,
                  const std::vector<ParameterSet>& parameters) {
    if (out) {
        *out << "DSU" << "componentN" << "componentE" << "interpairFraction" << "sameSetFraction" << "Score" << "Score Error";
    }

    Benchmark benchmark(ctx); // TODO pass additional work
    for (const auto& params : parameters) {
        size_t componentN = params.Get<size_t>("componentN");
        size_t componentE = params.Get<size_t>("componentE");
        double interpairFraction = params.Get<double>("ipf");
        double sameSetFraction = params.Get<double>("ssf");
        size_t interpairE = std::round(componentE * interpairFraction);

        std::vector<std::unique_ptr<DSU>> dsus = GetAvailableDsus(componentN * ctx->MaxConcurrency(), ctx->NodeCount(),
                                                                  filter);
        for (size_t i = 0; i < numWorkloads; ++i) {
            StaticWorkload workload = BuildComponentsRandomWorkload(ctx->MaxConcurrency(), componentN, componentE,
                                                                    interpairE, sameSetFraction);
            for (size_t j = 0; j < numIterationsPerWorkload; ++j) {
                for (auto& ptr: dsus) {
                    DSU* dsu = ptr.get();
                    std::cout << "Benchmark iteration #" << j << " for workload #" << i << "; DSU " << dsu->ClassName()
                              << std::endl;
                    benchmark.Run(dsu, workload);
                }
            }
        }
        for (auto& ptr: dsus) {
            DSU* dsu = ptr.get();
            Stats<double> result = benchmark.CollectThroughputStats(dsu);
            std::cout << std::fixed << std::setprecision(3)
                      << dsu->ClassName() << ": " << result.mean << "+-" << result.stddev << std::endl;
            if (out)
                *out << dsu->ClassName()
                     << componentN << componentE << interpairFraction << sameSetFraction
                     << result.mean << result.stddev;
        }
    }
}


int main(int argc, const char* argv[]) {
    CLI::App app("NUMA DSU Benchmark");
    std::vector<std::string> rawParameters;
    app.add_option("-p,--param", rawParameters, "Parameter in the form key=val1,val2,...,valN");

    ParameterSet defaultParams = ParseParameters({
         "componentN=1000000",
         "componentE=8000000",
         "ipf=0.2",
         "ssf=0.1"
    })[0];

    CLI11_PARSE(app, argc, argv);
    auto parameters = ParseParameters(rawParameters, &defaultParams);

    NUMAContext ctx(8, 2, true);
    CsvFile out("out.csv");

    RunComponentsBenchmark(&ctx, &out, std::regex(".*"), 2, 3, parameters);

//    Stats<double> s = benchmark.CollectThroughputStats();
//    std::cout << std::fixed << std::setprecision(3) << s.mean << " " << s.stddev << " op/s" << std::endl;
    return 0;
}
