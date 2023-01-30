#pragma once

// workloads for future use

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

