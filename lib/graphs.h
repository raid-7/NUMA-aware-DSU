#pragma once


#include "util.hpp"

#include <vector>
#include <random>
#include <fstream>
#include <iostream>

class Edge {
public:
    int u, v, w;

    Edge() {};
    Edge(int u, int v, int w) : u(u), v(v), w(w) {};
    Edge(int u, int v) : u(u), v(v), w(0) {};
};

class Graph {
public:
    int N, E;
    std::vector<Edge> Edges;

    Graph(int N, int E, std::vector<Edge> edges)
        : N(N), E(E), Edges(std::move(edges))
    {}
};


static thread_local std::uniform_int_distribution<int> TlWeightDistribution{0, 100000};

int randomWeight() {
    return TlWeightDistribution(TlRandom);
}

Graph graphRandom(int N, int E) {
    std::vector<Edge> g;
    std::uniform_int_distribution vertexDistribution(0, N - 1);

    // TODO deduplicate?
    // TODO remove loops?
    for (int i = 0; i < E ; i++) {
        int x = vertexDistribution(TlRandom);
        int y = vertexDistribution(TlRandom);
        g.emplace_back(x, y, randomWeight());
    }

    return Graph(N, E, g);
}

Graph graphFromFile(std::string filename) {
    // TODO revise
    std::ifstream file;
    file.open(filename);

    int N, E;
    if (filename[filename.size() - 1] == 'x') {
        std::string tmp;
        std::getline(file, tmp);
        std::getline(file, tmp);
        file >> N;
    }
    file >> N >> E;
    std::cerr << "got N=" << N << " and E=" << E << std::endl;
    std::vector<Edge> g;

    int a, b, w;
    char c;

    // TODO: fix the check
    if (filename[filename.size() - 1] == 'r') {
        for (int i = 0; i < E; i++) {
            file >> c;
            file >> a >> b >> w;
            N = std::max(N, std::max(a, b) + 1);
            g.emplace_back(Edge(a, b, w));
        }
    } else {
        if (filename[filename.size() - 1] == 'y') {
            for (int i = 0; i < E; i++) {
                file >> a >> b >> w;
                N = std::max(N, std::max(a, b) + 1);
                g.emplace_back(Edge(a, b, randomWeight()));
            }
        } else {
            for (int i = 0; i < E; i++) {
                file >> a >> b;
                N = std::max(N, std::max(a, b) + 1);
                g.emplace_back(Edge(a, b, randomWeight()));
            }
        }
    }

    Shuffle(g);
    return Graph(N, E, g);
}

int getIndex(int x, int componentId, int componentsNumber, int componentSize, bool shuffle) {
    if (shuffle) {
        return x * componentsNumber + componentId;
    } else {
        return x + componentId * componentSize;
    }
}

//Graph generateComponentsForNodesWithIntersection(int node_count, int N, int E, int intersection_percent) {
//    std::vector<int> perm(N);
//    for (int i = 0; i < N; i++) {
//        perm[i] = i;
//    }
//    std::random_device rd;
//    std::mt19937 q(rd());
//
//    auto g = new std::vector<Edge>();
//    g->resize(node_count * E);
//    for (int i = 0; i < node_count; i++) {
//        // get permutation
//        std::shuffle(perm.begin(), perm.end(), q);
//        for (int j = 0 ; j < N - 1; j++) {
//            g->at(j*node_count + i) = (Edge(
//                    getIndex(perm[j], i, node_count, N, shuffle),
//                    getIndex(perm[j + 1], i, node_count, N, shuffle),
//                    randomWeight()
//            ));
//        }
//        for (int j = N - 1; j < E; j++) {
//            int x = rand() % N;
//            int y = rand() % N;
//            x = getIndex(x, i, node_count, N, shuffle);
//            y = getIndex(y, i, node_count, N, shuffle);
//            g->at(j*node_count + i) = (Edge(x, y, randomWeight()));
//        }
//    }
//
//    int sole_n = N / 100 * intersection_percent;
//
//}

Graph generateComponents(int n, int N, int E, bool shuffle) {
    std::vector<int> perm(N);
    for (int i = 0; i < N; i++) {
        perm[i] = i;
    }

    std::vector<Edge> g;
    g.resize(n * E);
    for (int i = 0; i < n; i++) {
        // get permutation
        Shuffle(perm);
        //std::cerr << "before 1st\n";
        for (int j = 0 ; j < N - 1; j++) {
            g[j*n + i] = Edge(
                    getIndex(perm[j], i, n, N, shuffle), getIndex(perm[j + 1], i, n, N, shuffle), randomWeight()
            );
        }
        //std::cerr << "before 2d\n";
        for (int j = N - 1; j < E; j++) {
            int x = rand() % N;
            int y = rand() % N;
            x = getIndex(x, i, n, N, shuffle);
            y = getIndex(y, i, n, N, shuffle);
            g[j*n + i] = Edge(x, y, randomWeight());
        }
        //std::cerr << "after 2d\n";
    }

    return Graph(N * n, E * n, g);
}

Graph generateComponentsSequentially(int n, int N, int E) {
    return generateComponents(n, N, E, false);
}

Graph generateComponentsShuffled(int n, int N, int E) {
    return generateComponents(n, N, E, true);
}
