#include <iostream>
#include <thread>
#include <fstream>

#include "DSU.h"

int N = 100000;
int E = 100000000;
int THREADS = 100;
int RATIO = 80;

//std::vector< std::vector<int>> a;

struct Context {
    std::vector<std::vector<int>> graph;
    DSU* dsu;
    int ratio; // процент SameSet среди всех запросов

    Context(std::vector<std::vector<int>>* graph, DSU* dsu, int ratio) : graph(*graph), dsu(dsu), ratio(ratio) {};
};

void doSmth() {

}

void thread_routine(Context* ctx, int v1, int v2) {
    for (int v = v1; v < v2; v++) {
        for (int i = 0; i < int(ctx->graph[v].size()); i++) {
            if (i % 100 < ctx->ratio) {
                ctx->dsu->SameSet(v, ctx->graph[v][i]);
            } else {
                ctx->dsu->Union(v, ctx->graph[v][i]);
            }

            //doSmth();
        }
    }
}

void run(Context* ctx) {
    std::vector<std::thread> threads;

    int step = N / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step, std::min(i*step + step, N)));
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float runWithTime(Context* ctx) {
    auto start = std::chrono::high_resolution_clock::now();
    run(ctx);
    auto stop = std::chrono::high_resolution_clock::now();
    auto durationNUMA = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return float(durationNUMA.count()) / 1000;
}

std::vector<std::vector<int>> graphRandom() {
    std::vector<std::vector<int>> g;
    g.resize(N);

    for (int i = 0; i < E ; i++) {
        int x = rand() % N;
        int y = rand() % N;
        g[x].emplace_back(y);
        g[y].emplace_back(x);
    }

    return g;
}

std::vector<std::vector<int>> graphLJ() {
    std::ifstream file;
    file.open("LG.txt");

    file >> N >> E;
    std::vector<std::vector<int>> g;
    g.resize(N);
    for (int i = 0; i < E; i++) {
        int a, b;
        file >> a >> b;
        if (a > N) {
            N = a;
            g.resize(N);
        }
        if (b > N) {
            N = b;
            g.resize(N);
        }
        g[a].emplace_back(b);
        g[b].emplace_back(a);
    }
    return g;
}

void benchmark() {
    auto g = graphRandom();
    //auto g = graphLJ();

    int node_count = numa_num_configured_nodes();
    auto dsuNUMA = new DSU(N, node_count);
    auto dsuUsual = new DSU(N, 1);
    auto ctxNUMA = new Context(&g, dsuNUMA, RATIO);
    auto ctxUsual = new Context(&g, dsuUsual, RATIO);

    std::cout << runWithTime(ctxNUMA) << std::endl;
    std::cout << runWithTime(ctxUsual) << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        auto nStr = argv[1];
        auto eStr = argv[2];
        N = std::stoi(nStr);
        E = std::stoi(eStr);

        if (argc > 3) {
            auto threadsStr = argv[3];
            THREADS = std::stoi(threadsStr);
        }
    }

    benchmark();

    return 0;
}