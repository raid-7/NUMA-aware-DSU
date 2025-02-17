#pragma once

#include "workload.hpp"
#include "numa.hpp"
#include "util.hpp"
#include "stats.hpp"
#include "../DSU.h"

#include <barrier>
#include <span>
#include <string_view>
#include <array>
#include <map>


/*
 * For each vertex find a node which most frequently accesses the vertex and sets it as an owner of the vertex.
 */
template <class DSUType>
void PrepareDSUForWorkload(DSU* someDsu, const StaticWorkload& workload) {
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
        if (!ignoreMeasurements) {
            Metrics_[dsu].emplace_back(dsu->collectMetrics());
            if (DSU::EnableMetrics)
                ProduceSecondaryMetrics(Metrics_[dsu].back());
            HistMetrics_[dsu].emplace_back(dsu->collectHistMetrics());
        }
    }

    double ThreadWork(DSU* dsu, std::span<const Request> requests) {
        constexpr size_t NS = 1'000'000'000ull;
        Timer timer;
        ApplyRequests(dsu, requests, true);
        auto duration = timer.Get<std::chrono::nanoseconds>();
        dsu->GoAway();
        return requests.size() * NS / duration.count();
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

    std::vector<HistMetrics> CollectRawHistMetricStats(DSU* dsu) {
        return std::exchange(HistMetrics_[dsu], {});
    }

private:
    void ApplyRequests(DSU* dsu, std::span<const Request> requests, bool useAdditionalWork) const {
        for (const auto& request : requests) {
            request.Apply(dsu);
            if (useAdditionalWork)
                RandomAdditionalWork(AdditionalWork_);
        }
    }

    static void ProduceSecondaryMetrics(Metrics& metrics) {
        using namespace std::string_literals;

        metrics["same_set_requests"] = metrics["same_set_requests_true"] + metrics["same_set_requests_false"];
        metrics["requests"] = metrics["same_set_requests"] + metrics["union_requests"];

        metrics["cross_node_accesses"] = metrics["cross_node_read"] + metrics["cross_node_write"] +
                metrics["global_data_read_write"];


        metrics["cross_node_read_in_same_set"] = metrics["cross_node_read_in_false_same_set"] + metrics["cross_node_read_in_true_same_set"];
        metrics["cross_node_write_in_same_set"] = metrics["cross_node_write_in_false_same_set"] + metrics["cross_node_write_in_true_same_set"];
        metrics["global_data_read_write_in_same_set"] = metrics["global_data_read_write_in_false_same_set"] + metrics["global_data_read_write_in_true_same_set"];

        std::vector<std::string> perOpMetrics = {
                "cross_node_read",
                "cross_node_write",
                "this_node_read",
                "this_node_read_success",
                "this_node_write",
                "global_data_read_write",
                "cross_node_accesses"
        };

        std::vector<std::string> perFalseSSMetrics = {
                "cross_node_read_in_false_same_set",
                "cross_node_write_in_false_same_set",
                "global_data_read_write_in_false_same_set"
        };

        std::vector<std::string> perTrueSSMetrics = {
                "cross_node_read_in_true_same_set",
                "cross_node_write_in_true_same_set",
                "global_data_read_write_in_true_same_set"
        };

        std::vector<std::string> perSSMetrics = {
                "cross_node_read_in_same_set",
                "cross_node_write_in_same_set",
                "global_data_read_write_in_same_set"
        };

        std::vector<std::string> perUnionMetrics = {
                "cross_node_read_in_union",
                "cross_node_write_in_union",
                "global_data_read_write_in_union"
        };

        for (const std::string& m : perOpMetrics) {
            metrics[m + "_per_op"] = metrics[m] / metrics["requests"];
        }
        for (const std::string& m : perFalseSSMetrics) {
            metrics[m + "_per_op"] = metrics[m] / metrics["same_set_requests_false"];
        }
        for (const std::string& m : perTrueSSMetrics) {
            metrics[m + "_per_op"] = metrics[m] / metrics["same_set_requests_true"];
        }
        for (const std::string& m : perSSMetrics) {
            metrics[m + "_per_op"] = metrics[m] / metrics["same_set_requests"];
        }
        for (const std::string& m : perUnionMetrics) {
            metrics[m + "_per_op"] = metrics[m] / metrics["union_requests"];
        }
    }

private:
    NUMAContext* Ctx_;
    std::map<DSU*, std::vector<double>> ThroughputResults_;
    std::map<DSU*, std::vector<Metrics>> Metrics_;
    std::map<DSU*, std::vector<HistMetrics>> HistMetrics_;
    double AdditionalWork_ = 2.0;
};
