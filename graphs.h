#ifndef TRY_GRAPHS_H
#define TRY_GRAPHS_H

#include <vector>
#include <random>
#include <fstream>

class Edge {
public:
    int u, v, w;

    Edge() {};
    Edge(int u, int v, int w) : u(u), v(v), w(w) {};
    Edge(int u, int v) : u(u), v(v) {};
};

class Graph {
public:
    int N, E;
    std::vector<Edge>* edges;

    Graph(int N, int E, std::vector<Edge>* edges) : N(N), E(E) {
        this->edges = edges;
    }
};

void shuffle(std::vector<Edge>* edges) {
    std::random_device rd;
    std::mt19937 q(rd());
    std::shuffle(edges->begin(), edges->end(), q);
}

int random_weight() {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, 100000);
    return distribution(generator);
}

Graph graphRandom(int N, int E) {
    auto g = new std::vector<Edge>();

    for (int i = 0; i < E ; i++) {
        int x = rand() % N;
        int y = rand() % N;
        g->emplace_back(Edge(x, y, random_weight()));
    }

    return Graph(N, E, g);
}

Graph graphFromFile(std::string filename) {
    std::ifstream file;
    file.open(filename);

    int N, E;
    file >> N >> E;
    auto g = new std::vector<Edge>();

    int a, b, w;
    char c;

    // TODO: fix the check
    if (filename[filename.size() - 1] == 'r') {
        for (int i = 0; i < E; i++) {
            file >> c;
            file >> a >> b >> w;
            N = std::max(N, std::max(a, b) + 1);
            g->emplace_back(Edge(a, b, w));
        }
    } else {
        for (int i = 0; i < E; i++) {
            file >> a >> b;
            N = std::max(N, std::max(a, b) + 1);
            g->emplace_back(Edge(a, b, random_weight()));
        }
    }

    shuffle(g);
    return Graph(N, E, g);
}

int getIndex(int x, int componentId, int componentsNumber, int componentSize, bool shuffle) {
    if (shuffle) {
        return x * componentsNumber + componentId;
    } else {
        return x + componentId * componentSize;
    }
}

Graph generateComponentsForNodesWithIntersection(int node_count, int N, int E, int intersection_percent) {
    std::vector<int> perm(N);
    for (int i = 0; i < N; i++) {
        perm[i] = i;
    }
    std::random_device rd;
    std::mt19937 q(rd());

    auto g = new std::vector<Edge>();
    g->resize(node_count * E);
    for (int i = 0; i < node_count; i++) {
        // get permutation
        std::shuffle(perm.begin(), perm.end(), q);
        for (int j = 0 ; j < N - 1; j++) {
            g->at(j*node_count + i) = (Edge(
                    getIndex(perm[j], i, node_count, N, shuffle),
                    getIndex(perm[j + 1], i, node_count, N, shuffle),
                    random_weight()
            ));
        }
        for (int j = N - 1; j < E; j++) {
            int x = rand() % N;
            int y = rand() % N;
            x = getIndex(x, i, node_count, N, shuffle);
            y = getIndex(y, i, node_count, N, shuffle);
            g->at(j*node_count + i) = (Edge(x, y, random_weight()));
        }
    }

    int sole_n = N / 100 * intersection_percent;

}

Graph generateComponents(int n, int N, int E, bool shuffle) {
    std::vector<int> perm(N);
    for (int i = 0; i < N; i++) {
        perm[i] = i;
    }
    std::random_device rd;
    std::mt19937 q(rd());

    auto g = new std::vector<Edge>();
    g->resize(n * E);
    for (int i = 0; i < n; i++) {
        // get permutation
        std::shuffle(perm.begin(), perm.end(), q);
        for (int j = 0 ; j < N - 1; j++) {
            g->at(j*n + i) = (Edge(
                    getIndex(perm[j], i, n, N, shuffle),getIndex(perm[j + 1], i, n, N, shuffle), random_weight()
            ));
        }
        for (int j = N - 1; j < E; j++) {
            int x = rand() % N;
            int y = rand() % N;
            x = getIndex(x, i, n, N, shuffle);
            y = getIndex(y, i, n, N, shuffle);
            g->at(j*n + i) = (Edge(x, y, random_weight()));
        }
    }

    return Graph(N * n, E * n, g);
}

Graph generateComponentsSequentially(int n, int N, int E) {
    return generateComponents(n, N, E, false);
}

Graph generateComponentsShuffled(int n, int N, int E) {
    return generateComponents(n, N, E, true);
}

#endif //TRY_GRAPHS_H
