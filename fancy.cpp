#include "lib/stats.hpp"
#include "lib/timer.hpp"
#include "lib/graphs.h"
#include "lib/csv.hpp"
#include "lib/parameters.hpp"
#include "lib/benchmark.hpp"
#include "lib/workload_provider.hpp"

#include "DSU.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_ParallelUnions_no_imm.h"
#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Usual_noimm.h"
#include "implementations/DSU_Adaptive.h"
#include "implementations/DSU_AdaptiveSmart.h"
#include "implementations/DSU_AdaptiveLocks.h"
#include "implementations/DSU_LazyUnion.h"
#include "implementations/SeveralDSU.h"

#include "workloads/components_v2.hpp"

#include <CLI/App.hpp>
#include <CLI/Formatter.hpp>
#include <CLI/Config.hpp>

#include <iostream>
#include <iomanip>
#include <thread>
#include <regex>


std::vector<std::unique_ptr<DSU>> GetAvailableDsus(NUMAContext* ctx, size_t N, const std::regex& filter) {
    std::vector<std::unique_ptr<DSU>> dsus;

    dsus.emplace_back(new DSU_Usual(N));
    dsus.emplace_back(new SeveralDSU(ctx, N));

    auto construct = [&]<class T> (T) {
        dsus.emplace_back(new DSU_ParallelUnions<T::value>(ctx, N));
        dsus.emplace_back(new DSU_Adaptive<T::value, false>(ctx, N));
        dsus.emplace_back(new DSU_Adaptive<T::value, true>(ctx, N));
        dsus.emplace_back(new DSU_AdaptiveLocks<T::value>(ctx, N));
        dsus.emplace_back(new DSU_AdaptiveSmart<T::value>(ctx, N));
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

void PrepareDSUForWorkload(NUMAContext* ctx, DSU* dsu, const StaticWorkload& workload) {
    dsu->ReInit();

    // FIXME This is a hack to test the conjecture.
    PrepareDSUForWorkload<DSU_Adaptive<false, false>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_Adaptive<true, false>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_Adaptive<false, true>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_Adaptive<true, true>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_AdaptiveLocks<false>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_AdaptiveLocks<true>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_AdaptiveSmart<false>>(ctx, dsu, workload);
    PrepareDSUForWorkload<DSU_AdaptiveSmart<true>>(ctx, dsu, workload);
}


void RunBenchmark(NUMAContext* ctx, CsvFile& out, const std::regex& filter,
                  size_t numWorkloads, size_t numIterationsPerWorkload,
                  WorkloadProvider* wlProvider, const std::vector<ParameterSet>& parameters) {
    { // write CSV header
        auto writer = out << "DSU";
        for (const std::string& param : wlProvider->GetParameterNames()) {
            writer << param;
        }
        writer << "Score" << "Score Error";
    }

    Benchmark benchmark(ctx); // TODO pass additional work
    for (const auto& params : parameters) {
        std::vector<std::unique_ptr<DSU>> dsus = GetAvailableDsus(ctx, params.Get<size_t>("N"), filter);
        if (dsus.empty())
            continue;

        for (size_t i = 0; i < numWorkloads; ++i) {
            std::cout << "Preparing workload #" << i << std::endl;
            StaticWorkload workload = wlProvider->MakeWorkload(ctx, params);
            for (auto& ptr: dsus) {
                DSU* dsu = ptr.get();

                if (i == 0) {
                    PrepareDSUForWorkload(ctx, dsu, workload);
                    DSU::EnableCompaction = params.Get<bool>("compact");

                    // warmup for the given parameter set
                    std::cout << "Warmup iteration for workload #" << i << "; DSU " << dsu->ClassName() << std::endl;
                    benchmark.Run(dsu, workload, true);
                }

                for (size_t j = 0; j < numIterationsPerWorkload; ++j) {
                    PrepareDSUForWorkload(ctx, dsu, workload);
                    DSU::EnableCompaction = params.Get<bool>("compact");

                    std::cout << "Benchmark iteration #" << j << " for workload #" << i << "; DSU " << dsu->ClassName()
                              << std::endl;
                    benchmark.Run(dsu, workload);
                }
            }
        }
        for (auto& ptr: dsus) {
            DSU* dsu = ptr.get();
            Stats<double> result = benchmark.CollectThroughputStats(dsu);
            auto metrics = benchmark.CollectMetricStats(dsu);
            std::cout << std::fixed << std::setprecision(3)
                      << dsu->ClassName() << ": " << result.mean << "+-" << result.stddev << std::endl;
            for (const auto& [metric, value] : metrics) {
                std::cout << std::fixed << std::setprecision(3)
                     << "  :" << metric << ": "
                     << value.mean << "+-" << value.stddev << std::endl;
            }

            { // write results in CSV
                auto writer = out << dsu->ClassName();
                for (const std::string& param: wlProvider->GetParameterNames()) {
                    writer << params.Get<std::string>(param);
                }
                writer << result.mean << result.stddev;
            }

            for (const auto& [metric, value] : metrics) { // write metrics in CSV
                auto writer = out << (dsu->ClassName() + ":" + metric);
                for (const std::string& param : wlProvider->GetParameterNames()) {
                    writer << params.Get<std::string>(param);
                }
                writer << value.mean << value.stddev;
            }
        }
    }
}

namespace std {
template<class X, class Y>
struct hash<std::pair<X, Y>> {
    [[no_unique_address]] std::hash<X> xHash_{};
    [[no_unique_address]] std::hash<Y> yHash_{};

    auto operator()(const std::pair<X, Y>& pair) const noexcept {
        return xHash_(pair.first) * 11 + yHash_(pair.second) * 17;
    }
};
}

void RunStagedBenchmark(NUMAContext* ctx, CsvFile& out, const std::regex& filter,
                  size_t numWorkloads, size_t numIterationsPerWorkload,
                  WorkloadProvider* wlProvider, const std::vector<std::vector<ParameterSet>>& parameterSets) {
    { // write CSV header
        auto writer = out << "DSU" << "Stage";
        for (const std::string& param : wlProvider->GetParameterNames()) {
            writer << param;
        }
        writer << "Score" << "Score Error";
    }

    Benchmark benchmark(ctx); // TODO pass additional work

    for (const std::vector<ParameterSet>& parameters : parameterSets) {
        size_t n = parameters[0].Get<size_t>("N");
        REQUIRE(std::all_of(parameters.begin(), parameters.end(), [n](const ParameterSet& params) {
            return params.Get<size_t>("N") == n;
        }), "All stage parameter sets must have equal N");

        std::vector<std::unique_ptr<DSU>> dsus = GetAvailableDsus(ctx, n, filter);
        if (!dsus.empty()) {
            std::unordered_map<std::pair<DSU*, size_t>, std::vector<double>> throughputRes;
            std::unordered_map<std::pair<DSU*, size_t>, std::vector<Metrics>> metricRes;

            for (size_t i = 0; i < numWorkloads; ++i) {
                std::cout << "Preparing workload #" << i << std::endl;
                std::vector<StaticWorkload> stages;
                wlProvider->PrepareSeries(); // initialize mappings for shuffle if used
                for (const auto& params: parameters) {
                    stages.push_back(wlProvider->MakeWorkload(ctx, params));
                }
                wlProvider->EndSeries();
                for (auto& ptr: dsus) {
                    DSU* dsu = ptr.get();

                    if (i == 0) {
                        PrepareDSUForWorkload(ctx, dsu, stages[0]);

                        for (size_t stageIndex = 0; stageIndex < stages.size(); ++stageIndex) {
                            DSU::EnableCompaction = parameters[stageIndex].Get<bool>("compact");

                            // warmup for the given parameter set
                            std::cout << "Warmup iteration for workload #" << i << "; DSU " << dsu->ClassName()
                                      << std::endl;

                            benchmark.Run(dsu, stages[stageIndex], true);
                        }
                    }

                    for (size_t j = 0; j < numIterationsPerWorkload; ++j) {
                        PrepareDSUForWorkload(ctx, dsu, stages[0]);

                        for (size_t stageIndex = 0; stageIndex < stages.size(); ++stageIndex) {
                            DSU::EnableCompaction = parameters[stageIndex].Get<bool>("compact");

                            std::cout << "Benchmark iteration #" << j << " for workload #" << i << ", stage #"
                                      << stageIndex
                                      << "; DSU "
                                      << dsu->ClassName()
                                      << std::endl;
                            benchmark.Run(dsu, stages[stageIndex]);
                            std::pair<DSU*, size_t> key = {dsu, stageIndex};
                            auto throughput = benchmark.CollectRawThroughputStats(dsu);
                            auto metrics = benchmark.CollectRawMetricStats(dsu);
                            throughputRes[key].insert(throughputRes[key].begin(), throughput.begin(), throughput.end());
                            metricRes[key].insert(metricRes[key].begin(), metrics.begin(), metrics.end());
                        }
                    }
                }
            }
            for (auto& ptr: dsus) {
                for (size_t stageIndex = 0; stageIndex < parameters.size(); ++stageIndex) {
                    DSU* dsu = ptr.get();
                    std::pair<DSU*, size_t> key = {dsu, stageIndex};
                    Stats<double> result = stats(throughputRes[key].begin(), throughputRes[key].end());
                    auto metrics = metricStats(metricRes[key].begin(), metricRes[key].end());
                    std::cout << std::fixed << std::setprecision(3)
                              << dsu->ClassName() << "/" << stageIndex << ": " << result.mean << "+-" << result.stddev
                              << std::endl;
                    for (const auto& [metric, value]: metrics) {
                        std::cout << std::fixed << std::setprecision(3)
                                  << "  :" << metric << ": "
                                  << value.mean << "+-" << value.stddev << std::endl;
                    }

                    { // write results in CSV
                        auto writer = out << dsu->ClassName() << stageIndex;
                        for (const std::string& param: wlProvider->GetParameterNames()) {
                            writer << parameters[stageIndex].Get<std::string>(param);
                        }
                        writer << result.mean << result.stddev;
                    }

                    for (const auto& [metric, value]: metrics) { // write metrics in CSV
                        auto writer = out << (dsu->ClassName() + ":" + metric) << stageIndex;
                        for (const std::string& param: wlProvider->GetParameterNames()) {
                            writer << parameters[stageIndex].Get<std::string>(param);
                        }
                        writer << value.mean << value.stddev;
                    }
                }
            }
        }
    }
}


int main(int argc, const char* argv[]) {
    std::vector<std::shared_ptr<WorkloadProvider>> wlProviders = {
            std::make_shared<ComponentsRandomWorkloadV2>()
    };

    CLI::App app("NUMA DSU Benchmark");

    std::vector<std::string> rawParameters;
    app.add_option("-p,--param", rawParameters, "Parameter in the form key=val1,val2,...,valN");

    bool testing = false;
    app.add_flag("--testing", testing, "Setup NUMA context for testing with 8 CPUs on 4 nodes");

    bool enableMetrics = false;
    app.add_flag("-m,--metrics", enableMetrics, "Enable metrics (may affect throughput!)");

    std::string dsuFilter = ".*";
    app.add_option("-d,--dsu", dsuFilter, "ECMAScript regular expression specifying DSUs to benchmark");

    std::string workloadName = "components";
    app.add_option("-l,--workload", workloadName, "Benchmark workload type");

    std::string outFileName = "out.csv";
    app.add_option("-o,--out", outFileName, "Output CSV file");

    size_t numWorkloads = 2;
    app.add_option("-n,--num-workloads", numWorkloads, "Number of workloads participating in each experiment");

    size_t numIterationsPerWorkload = 3;
    app.add_option("-i,--num-iterations", numIterationsPerWorkload, "Number of iterations per workloads");

    std::vector<std::string> rawStageParameters;
    app.add_option("--sp,--stage-param", rawStageParameters, "For staged benchmark: stage parameter in the form stageId:param=value");

    CLI11_PARSE(app, argc, argv);

    auto wlProviderIt = std::find_if(wlProviders.begin(), wlProviders.end(), [&workloadName](const auto& provider) {
        return provider->Name() == workloadName;
    });
    REQUIRE(wlProviderIt != wlProviders.end(), "Invalid workload");
    WorkloadProvider* wlProvider = wlProviderIt->get();

    ParameterSet commonDefaults = ParseParameters({
        "N=4000000",
        "compact=true"
    })[0];
    ParameterSet defaultParams = wlProvider->GetDefaultParameters(&commonDefaults);

    auto parameters = ParseParameters(rawParameters, &defaultParams);
    std::vector<std::vector<ParameterSet>> stageParameters;
    for (const ParameterSet& baseParameters : parameters) {
        auto stageParams = ParseStageParameters(rawStageParameters, &baseParameters);
        if (!stageParams.empty())
            stageParameters.push_back(std::move(stageParams));
    }
    auto filter = std::regex(dsuFilter, std::regex::ECMAScript | std::regex::icase | std::regex::nosubs);

    DSU::EnableMetrics = enableMetrics;

    NUMAContext ctx(4);
    if (testing) {
        ctx.SetupForTests(8, 4);
    }
    CsvFile out(outFileName);

    if (!stageParameters.empty()) {
        RunStagedBenchmark(&ctx, out, filter, numWorkloads, numIterationsPerWorkload, wlProvider, stageParameters);
    } else {
        RunBenchmark(&ctx, out, filter, numWorkloads, numIterationsPerWorkload, wlProvider, parameters);
    }
    return 0;
}
