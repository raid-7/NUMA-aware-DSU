#pragma once

#include "workload.hpp"
#include "numa.hpp"
#include "util.hpp"
#include "stats.hpp"
#include "../DSU.h"

#include <barrier>
#include <span>
#include <map>


/*
 * For each vertex find a node which most frequently accesses the vertex and sets it as an owner of the vertex.
 */
template <class DSUType>
void PrepareDSUForWorkload(NUMAContext* ctx, DSU* someDsu, const StaticWorkload& workload) {
    DSUType* dsu = dynamic_cast<DSUType*>(someDsu);
    if (!dsu)
        return;

    const int* cMapping = workload.GetMeta<ComponentMappingMd>().Mapping.data();

    // Fast setup
    for (int u = 0; u < (int)workload.N; ++u) {
        int expId = cMapping[u];
        dsu->SetOwner(u, expId);
    }

    // Precise setup (slow)
//    std::unordered_map<int, std::vector<uint32_t>> stats;
//    for (size_t tId = 0; tId < workload.ThreadRequests.size(); ++tId) {
//        int node = threadNodeLayout((int)tId);
//        for (const auto& edge : workload.ThreadRequests[tId]) {
//            for (int u : std::array{edge.u, edge.v}) {
//                auto& uStats = stats[u];
//                if (uStats.size() <= 16)
//                    uStats.resize(16, 0);
//                ++uStats[node];
//            }
//        }
//    }
//
//    std::vector<std::pair<int, int>> r;
//    for (const auto& [u, nStats] : stats) {
//        auto maxIt = std::max_element(nStats.begin(), nStats.end());
//        int maxId = maxIt - nStats.begin();
//        dsu->SetOwner(u, maxId);
//
//        if (nStats[maxId] > 0)
//            r.push_back({u, maxId});
//    }
//
//    size_t rate = 0;
//    for (size_t i = 0; i < 32; ++i) {
//        auto [u, maxId] = r[i];
//        int expId = u * 4 / workload.N;
//        if (expId != maxId)
//            ++rate;
//    }
}

class Benchmark {
public:
    Benchmark(NUMAContext* ctx)
            : Ctx_(ctx)
    {}

    void Run(DSU* dsu, const StaticWorkload& workload, bool ignoreMeasurements = false) {
        size_t numThreads = workload.ThreadRequests.size();
        std::barrier barrier(numThreads);
        ApplyRequests(dsu, workload.PreHeatRequests, false);
        size_t resultsOffset = ThroughputResults_[dsu].size();
        if (!ignoreMeasurements)
            ThroughputResults_[dsu].resize(resultsOffset + numThreads, 0.0);

        dsu->resetMetrics();

        Ctx_->StartNThreads(
                [this, &barrier, &workload, dsu, resultsOffset, ignoreMeasurements]() {
                    barrier.arrive_and_wait();
                    double avgThrpt = ThreadWork(dsu, workload.ThreadRequests[NUMAContext::CurrentThreadId()]);
                    if (!ignoreMeasurements)
                        ThroughputResults_[dsu][resultsOffset + NUMAContext::CurrentThreadId()] = avgThrpt;
                },
                numThreads
        );
        Ctx_->Join();
        if (!ignoreMeasurements)
            Metrics_[dsu].emplace_back(dsu->collectMetrics());
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

    std::unordered_map<std::string, Stats<long double>> CollectMetricStats(DSU* dsu) {
        auto res = metricStats(Metrics_[dsu].begin(), Metrics_[dsu].end());
        Metrics_[dsu].clear();
        return res;
    }

    std::vector<double> CollectRawThroughputStats(DSU* dsu) {
        return std::exchange(ThroughputResults_[dsu], {});
    }

    std::vector<Metrics> CollectRawMetricStats(DSU* dsu) {
        return std::exchange(Metrics_[dsu], {});
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
    std::map<DSU*, std::vector<Metrics>> Metrics_;
    double AdditionalWork_ = 2.0;
};
