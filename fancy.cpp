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
#include "implementations/DSU_Adaptive.h"
#include "implementations/DSU_AdaptiveLocks.h"
#include "implementations/DSU_LazyUnion.h"
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

/*
 * For each vertex find a node which most frequently accesses the vertex and sets it as an owner of the vertex.
 */
template <bool Halfing>
void PrepareDSUForWorkload(DSU_Adaptive<Halfing>* dsu, const StaticWorkload& workload, auto threadNodeLayout) {
    std::unordered_map<int, std::vector<uint32_t>> stats;
    for (size_t tId = 0; tId < workload.ThreadRequests.size(); ++tId) {
        int node = threadNodeLayout((int)tId);
        for (const auto& edge : workload.ThreadRequests[tId]) {
            for (int u : std::array{edge.u, edge.v}) {
                auto& uStats = stats[u];
                if (uStats.size() <= 16)
                    uStats.resize(16);
                ++uStats[node];
            }
        }
    }
    for (const auto& [u, nStats] : stats) {
        auto maxIt = std::max_element(nStats.begin(), nStats.end());
        int maxId = maxIt - nStats.begin();
        dsu->SetOwner(u, maxId);
    }
}

class Benchmark {
public:
    Benchmark(NUMAContext* ctx)
        : Ctx_(ctx)
    {}

    void Run(DSU* dsu, const StaticWorkload& workload, bool ignoreMeasurements = false) {
        // FIXME This is a hack to test the conjecture.
        if (auto* adsu = dynamic_cast<DSU_Adaptive<false>*>(dsu); adsu) {
            PrepareDSUForWorkload(adsu, workload, [ctx = Ctx_](int tid) {
                return ctx->NumaNodeForThread(tid);
            });
        }
        if (auto* adsu = dynamic_cast<DSU_Adaptive<true>*>(dsu); adsu) {
            PrepareDSUForWorkload(adsu, workload, [ctx = Ctx_](int tid) {
                return ctx->NumaNodeForThread(tid);
            });
        }

        size_t numThreads = workload.ThreadRequests.size();
        std::barrier barrier(numThreads);
        dsu->ReInit();
        ApplyRequests(dsu, workload.PreHeatRequests, false);
        size_t resultsOffset = ThroughputResults_[dsu].size();
        if (!ignoreMeasurements)
            ThroughputResults_[dsu].resize(resultsOffset + numThreads, 0.0);
        Ctx_->StartNThreads(
            [this, &barrier, &workload, dsu, resultsOffset, ignoreMeasurements]() {
                barrier.arrive_and_wait();
                double avgNs = ThreadWork(dsu, workload.ThreadRequests[NUMAContext::CurrentThreadId()]);
                if (!ignoreMeasurements)
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


StaticWorkload BuildComponentsRandomWorkloadV2(size_t numThreads, size_t numComponents, size_t N, size_t E,
                                             double intercomponentEFraction, double sameSetFraction,
                                             auto threadNodeLayout) {
    // E is the number of union requests
    // so we transform to the number of all requests
    E = static_cast<size_t>(std::round(E / (1. - sameSetFraction)));

    REQUIRE(numThreads % numComponents == 0, "Number of threads must be divisible by number of components");
    REQUIRE(numComponents > 1, "Number of components must be greater than 1");

    // assign a component # tid/numComponents to each thread
    // independently generate random edges for each thread inside the component
    // independently generate random edges between components for each thread
    // shuffle edges of each thread
    size_t componentN = N / numComponents;
    size_t intercomponentE = std::round(intercomponentEFraction * E);
    size_t internalE = E - intercomponentE;
    size_t internalThreadE = internalE / numThreads;

    std::bernoulli_distribution sameSetDistribution(sameSetFraction);
    std::vector<std::vector<Request>> threadWork(numThreads);

    for (size_t tid = 0; tid < numThreads; ++tid) {
        size_t componentId = threadNodeLayout(tid);
        size_t minComponentVertex = componentN * componentId;
        std::uniform_int_distribution<int> uvDistribution(minComponentVertex, minComponentVertex + componentN - 1);
        for (size_t i = 0; i < internalThreadE; ++i) {
            int u = uvDistribution(TlRandom);
            int v = uvDistribution(TlRandom);
            threadWork[tid].push_back({sameSetDistribution(TlRandom), u, v});
        }
    }

    std::vector<Request> interpairRequests;
    size_t interPairE = intercomponentE / ((numComponents - 1) * numComponents / 2);
    for (size_t c1 = 0; c1 < numComponents; ++c1) {
        size_t minC1Vertex = componentN * c1;
        std::uniform_int_distribution<int> uDistribution(minC1Vertex, minC1Vertex + componentN - 1);
        for (size_t c2 = c1 + 1; c2 < numComponents; ++c2) {
            size_t minC2Vertex = componentN * c2;
            std::uniform_int_distribution<int> vDistribution(minC2Vertex, minC2Vertex + componentN - 1);
            for (size_t i = 0; i < interPairE; ++i) {
                int u = uDistribution(TlRandom);
                int v = vDistribution(TlRandom);
                interpairRequests.push_back({sameSetDistribution(TlRandom), u, v});
            }
        }
    }

    Shuffle(interpairRequests);
    size_t intercomponentThreadE = interpairRequests.size() / numThreads;
    size_t intercomponentThreadERem = interpairRequests.size() % numThreads;
    for (size_t tid = 0; tid < numThreads; ++tid) {
        size_t thisThreadE = intercomponentThreadE + (tid < intercomponentThreadERem ? 1 : 0);
        for (size_t j = 0; j < thisThreadE; ++j) {
            threadWork[tid].push_back(interpairRequests.back());
            interpairRequests.pop_back();
        }
    }

    for (auto& work : threadWork) {
        Shuffle(work);
    }
    return StaticWorkload{
            {},
            std::move(threadWork),
            numComponents * componentN
    };
}

std::vector<std::unique_ptr<DSU>> GetAvailableDsus(NUMAContext* ctx, size_t N, const std::regex& filter) {
    std::vector<std::unique_ptr<DSU>> dsus;

    dsus.emplace_back(new DSU_Usual(N));
    dsus.emplace_back(new SeveralDSU(ctx, N));

    auto construct = [&]<class T> (T) {
        dsus.emplace_back(new DSU_ParallelUnions<T::value>(ctx, N));
        dsus.emplace_back(new DSU_Adaptive<T::value>(ctx, N));
        dsus.emplace_back(new DSU_AdaptiveLocks<T::value>(ctx, N));
        dsus.emplace_back(new DSU_LazyUnions<T::value>(ctx, N));
    };

    construct(std::true_type{});
    construct(std::false_type{});


    //dsus.emplace_back(new DSU_Usual_NoImm(N));

    //dsus.emplace_back(new TwoDSU(N, node_count));
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
        *out << "DSU" << "N" << "E" << "interpairFraction" << "sameSetFraction" << "Score" << "Score Error";
    }

    Benchmark benchmark(ctx); // TODO pass additional work
    for (const auto& params : parameters) {
        size_t N = params.Get<size_t>("N");
        size_t E = params.Get<size_t>("E");
        double interpairFraction = params.Get<double>("ipf");
        double sameSetFraction = params.Get<double>("ssf");

        std::vector<std::unique_ptr<DSU>> dsus = GetAvailableDsus(ctx, N, filter);
        for (size_t i = 0; i < numWorkloads; ++i) {
            std::cout << "Preparing workload #" << i << std::endl;
            StaticWorkload workload = BuildComponentsRandomWorkloadV2(ctx->MaxConcurrency(), ctx->NodeCount(), N, E,
                        interpairFraction, sameSetFraction, [ctx](int tid) { return ctx->NumaNodeForThread(tid); });
            for (auto& ptr: dsus) {
                DSU* dsu = ptr.get();

                if (i == 0) {
                    // warmup for the given parameter set
                    std::cout << "Warmup iteration for workload #" << i << "; DSU " << dsu->ClassName() << std::endl;
                    benchmark.Run(dsu, workload, true);
                }

                for (size_t j = 0; j < numIterationsPerWorkload; ++j) {
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
                     << N << E << interpairFraction << sameSetFraction
                     << result.mean << result.stddev;
        }
    }
}


int main(int argc, const char* argv[]) {
    CLI::App app("NUMA DSU Benchmark");

    std::vector<std::string> rawParameters;
    app.add_option("-p,--param", rawParameters, "Parameter in the form key=val1,val2,...,valN");

    bool testing = false;
    app.add_flag("--testing", testing, "Setup NUMA context for testing with 8 CPUs on 4 nodes");

    ParameterSet defaultParams = ParseParameters({
         "N=4000000",
         "E=64000000",
         "ipf=0.2",
         "ssf=0.1"
    })[0];

    CLI11_PARSE(app, argc, argv);
    auto parameters = ParseParameters(rawParameters, &defaultParams);

    NUMAContext ctx(4);
    if (testing) {
        ctx.SetupForTests(8, 4);
    }
    CsvFile out("out.csv");

    RunComponentsBenchmark(&ctx, &out, std::regex(".*"), 2, 3, parameters);

//    Stats<double> s = benchmark.CollectThroughputStats();
//    std::cout << std::fixed << std::setprecision(3) << s.mean << " " << s.stddev << " op/s" << std::endl;
    return 0;
}
