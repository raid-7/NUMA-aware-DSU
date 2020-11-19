#include <iostream>
#include <thread>
#include <fstream>

#include "DSU.h"

const std::string RANDOM = "random";
const int RUNS = 3;

int N = 100000;
int E = 100000000;
int THREADS = 192;
int RATIO = 80;

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
    return float(durationNUMA.count());// / 1000;
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

std::vector<std::vector<int>> graphFromFile(std::string filename) {
    std::ifstream file;
    file.open(filename);

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

void runSequential(DSU* dsu) {
    std::vector<std::vector<int>> g;

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < int(g.size()); i++) {
        for (int j = 0; j < int(g[i].size()); j++) {
            if (i % 100 < RATIO) {
                dsu->SameSet(i, g[i][j]);
            } else {
                dsu->Union(i, g[i][j]);
            }
        }
    }
    auto stop = std::chrono::high_resolution_clock::now();
    auto durationNUMA = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    std::cout << "Sequential " << float(durationNUMA.count()) << std::endl;
}

void benchmark(std::string graph) {
    std::vector<std::vector<int>> g;
    if (graph == RANDOM) {
        g = graphRandom();
    } else {
        g = graphFromFile(graph);
    }

    int node_count = numa_num_configured_nodes();
    auto dsuNUMA = new DSU_Helper(N, node_count);
    auto dsuUsual = new DSU_Helper(N, 1);
    auto ctxNUMA = new Context(&g, dsuNUMA, RATIO);
    auto ctxUsual = new Context(&g, dsuUsual, RATIO);

    auto dsuSeq = new DSU_Sequential(N);
    runSequential(dsuSeq);

    float resultNUMA = 0;
    float resultUsual = 0;
    // три раза повторяем запуск чтобы получить более правдивый результат
    for (int i = 0; i < RUNS; i++) {
        //std::cout << runWithTime(ctxNUMA) << std::endl;
        resultNUMA += runWithTime(ctxNUMA);
        //std::cout << runWithTime(ctxUsual) << std::endl;
        resultUsual += runWithTime(ctxUsual);
    }

    std::cout << "NUMA " << resultNUMA / RUNS << "\n";
    std::cout << "Usual " << resultUsual / RUNS << "\n";
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        auto graph = argv[1];
        if (graph == RANDOM) {
            auto nStr = argv[2];
            auto eStr = argv[3];
            N = std::stoi(nStr);
            E = std::stoi(eStr);
            if (argc > 4) {
                auto threadsStr = argv[4];
                THREADS = std::stoi(threadsStr);
            }
        } else {
            if (argc > 2) {
                auto threadsStr = argv[2];
                THREADS = std::stoi(threadsStr);
            }
        }

        benchmark(graph);
    }
    return 0;
}