#pragma once

#include "../lib/workload_provider.hpp"
#include "../lib/util.hpp"

#include <random>


class ComponentsRandomWorkloadV2 : public WorkloadProvider {
public:
    StaticWorkload MakeWorkload(NUMAContext* ctx, const ParameterSet& params) override {
        size_t N = params.Get<size_t>("N");
        size_t E = params.Get<size_t>("E");
        double interpairFraction = params.Get<double>("ipf");
        double sameSetFraction = params.Get<double>("ssf");
        bool shuffleVertices = params.Get<bool>("shuffle");
        return BuildComponentsRandomWorkloadV2(ctx->MaxConcurrency(), ctx->NodeCount(), N, E,
                                               interpairFraction, sameSetFraction,
                                               [ctx](int tid) { return ctx->NumaNodeForThread(tid); }, shuffleVertices);
    }

    std::string_view Name() const override {
        return "components";
    }

    std::vector<std::string> GetParameterNames() const override {
        return {
                "N", "E", "ipf", "ssf", "shuffle"
        };
    }

    ParameterSet GetDefaultParameters() const override {
        return ParseParameters({
                                       "N=4000000",
                                       "E=64000000",
                                       "ipf=0.2",
                                       "ssf=0.1",
                                       "shuffle=true"
                               })[0];
    }

private:
    StaticWorkload BuildComponentsRandomWorkloadV2(size_t numThreads, size_t numComponents, size_t N, size_t E,
                                                   double intercomponentEFraction, double sameSetFraction,
                                                   auto threadNodeLayout, bool shuffle) {
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

        std::vector<int> vertexPermutation(numComponents * componentN);
        std::generate(vertexPermutation.begin(), vertexPermutation.end(), [i = 0]() mutable {
            return i++;
        });
        if (shuffle) {
            Shuffle(vertexPermutation);
        }

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
                {ComponentMappingMd{std::move(componentMapping)}}
        };
    }
};
