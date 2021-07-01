#ifndef TRY_GRAPHS_H
#define TRY_GRAPHS_H

#include <vector>
#include <random>
#include <fstream>

void shuffle(std::vector<std::pair<int, int>>* edges) {
    std::random_device rd;
    std::mt19937 q(rd());
    std::shuffle(edges->begin(), edges->end(), q);
}

class Graph {
public:
    int N, E;
    std::vector<std::pair<int, int>>* edges;

    Graph(int N, int E, std::vector<std::pair<int, int>>* edges) : N(N), E(E) {
        this->edges = edges;
    }
};

Graph graphRandom(int N, int E) {
    auto g = new std::vector<std::pair<int, int>>();

    for (int i = 0; i < E ; i++) {
        int x = rand() % N;
        int y = rand() % N;
        g->emplace_back(std::make_pair(x, y));
    }

    return Graph(N, E, g);
}

Graph graphFromFile(std::string filename) {
    std::ifstream file;
    file.open(filename);

    int N, E;
    file >> N >> E;
    auto g = new std::vector<std::pair<int, int>>();

    int a, b;
    char c;

    // TODO: fix the check
    if (filename[filename.size() - 1] == 'r') {
        for (int i = 0; i < E; i++) {
            file >> c;
            file >> a >> b;
            N = std::max(N, std::max(a, b) + 1);
            g->emplace_back(std::make_pair(a, b));
            file >> a;
        }
    } else {
        for (int i = 0; i < E; i++) {
            file >> a >> b;
            N = std::max(N, std::max(a, b) + 1);
            g->emplace_back(std::make_pair(a, b));
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

Graph generateComponents(int n, int N, int E, bool shuffle) {
    std::vector<int> perm(N);
    for (int i = 0; i < N; i++) {
        perm[i] = i;
    }
    std::random_device rd;
    std::mt19937 q(rd());

    auto g = new std::vector<std::pair<int, int>>();
    for (int i = 0; i < n; i++) {
        // get permutation
        std::shuffle(perm.begin(), perm.end(), q);
        for (int j = 0 ; j < N - 1; j++) {
            g->emplace_back(std::make_pair(
                    getIndex(perm[j], i, n, N, shuffle),getIndex(perm[j + 1], i, n, N, shuffle)
            ));
        }
        for (int j = N - 1; j < E; j++) {
            int x = rand() % N;
            int y = rand() % N;
            x = getIndex(x, i, n, N, shuffle);
            y = getIndex(y, i, n, N, shuffle);
            g->emplace_back(std::make_pair(x, y));
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
