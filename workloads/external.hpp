#pragma once

#include "../lib/workload_provider.hpp"
#include "../lib/util.hpp"
#include "../lib/mtx_reader.hpp"

#include <random>


class ExternalGraphWorkload : public WorkloadProvider {
public:
    StaticWorkload MakeWorkload(NUMAContext* ctx, const ParameterSet& params) override {
        std::string filename = params.Get<std::string>("graph");
        double interpairFraction = params.Get<double>("ipf");
        double sameSetFraction = params.Get<double>("ssf");
        bool shuffleVertices = params.Get<bool>("shuffle");

        MtxReader reader(filename);
        auto [N, N_unused] = reader.readSize();
        std::vector<Request> unionRequests;
        reader.readEdges([&](size_t i, size_t j) {
            unionRequests.push_back(Request{
                false, static_cast<int>(i), static_cast<int>(j)
            });
        });

        size_t numSameSetRequests = sameSetFraction / (1. - sameSetFraction) * unionRequests.size();
        return BuildComponentsRandomWorkloadV2(
                ctx->MaxConcurrency(), ctx->NodeCount(),
                N, unionRequests, numSameSetRequests,
                interpairFraction,
                [ctx](int tid) { return ctx->NumaNodeForThread(tid); }
        );
    }

    std::string_view Name() const override {
        return "ext_components";
    }

    std::vector<std::string> GetParameterNames() const override {
        return {
            "graph", "ipf", "ssf", "shuffle"
        };
    }

    ParameterSet GetDefaultParameters(const ParameterSet* commonDefaults) const override {
        return ParseParameters({
               "ipf=0.2",
               "ssf=0.1",
               "shuffle=true"
       }, commonDefaults)[0];
    }

    StaticWorkload BuildComponentsRandomWorkloadV2(size_t numThreads, size_t numComponents, size_t N,
                                                   const std::vector<Request>& unionRequests, size_t numSameSetRequests,
                                                   double intercomponentSameSetRequestsFraction,
                                                   auto threadNodeLayout) {
        REQUIRE(numThreads % numComponents == 0, "Number of threads must be divisible by number of components");
        REQUIRE(numComponents > 1, "Number of components must be greater than 1");

        std::vector<std::vector<Request>> requests(numThreads);
        for (size_t i = 0; i < unionRequests.size(); ++i) {
            size_t threadId = i % numThreads;
            requests[threadId].push_back(unionRequests[i]);
        }

        std::vector<int> mappings = ComputeComponentMappings(requests, threadNodeLayout);
        std::vector<std::vector<int>> componentVertices(numComponents);
        for (size_t i = 0; i < mappings.size(); ++i) {
            componentVertices[mappings[i]].push_back(static_cast<int>(i));
        }

        // assign a component # tid/numComponents to each thread
        // independently generate random edges for each thread inside the component
        // independently generate random edges between components for each thread
        // shuffle edges of each thread
        size_t componentN = N / numComponents;
        size_t intercomponentE = std::round(intercomponentEFraction * E);
        size_t internalE = E - intercomponentE;
        size_t internalThreadE = internalE / numThreads;

        std::vector<int> componentMapping(numComponents * componentN);
        for (size_t i = 0; i < vertexPermutation.size(); ++i) {
            size_t expId = i * numComponents / N;
            componentMapping[vertexPermutation[i]] = (int) expId;
        }

        std::bernoulli_distribution sameSetDistribution(sameSetFraction);
        std::vector<std::vector<Request>> threadWork(numThreads);

        for (size_t tid = 0; tid < numThreads; ++tid) {
            size_t componentId = threadNodeLayout(tid);
            size_t minComponentVertex = componentN * componentId;
            std::uniform_int_distribution<int> uvDistribution(minComponentVertex, minComponentVertex + componentN - 1);
            for (size_t i = 0; i < internalThreadE; ++i) {
                int u = uvDistribution(TlRandom);
                int v = uvDistribution(TlRandom);
                threadWork[tid].push_back({
                                                  sameSetDistribution(TlRandom),
                                                  vertexPermutation[u],
                                                  vertexPermutation[v]
                                          });
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
                    interpairRequests.push_back({
                                                        sameSetDistribution(TlRandom),
                                                        vertexPermutation[u],
                                                        vertexPermutation[v]
                                                });
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

        for (auto& work: threadWork) {
            Shuffle(work);
        }
        return StaticWorkload{
                {},
                std::move(threadWork),
                numComponents * componentN,
                {ComponentMappingMd{std::move(mappings)}}
        };
    }

    std::vector<int> ComputeComponentMappings(const std::vector<std::vector<Request>>& requests, auto threadNodeLayout) {
        // TODO
        return {};
    }
};
