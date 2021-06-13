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

Graph generateComponents(int n, int N, int E) {
    auto g = new std::vector<std::pair<int, int>>();
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < E; j++) {
            int x = rand() % N;
            int y = rand() % N;
            x = x * n + i; // чтобы вершинки перемешались
            y = y * n + i;
            g->emplace_back(std::make_pair(x, y));
        }
    }
//    std::random_device rd;
//    std::mt19937 q(rd());
//    std::shuffle(g->begin(), g->end(), q);
    return Graph(N * n, E * n, g);
}

#endif //TRY_GRAPHS_H
