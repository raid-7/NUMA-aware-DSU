#include <iostream>
#include <thread>
#include <fstream>
#include <random>
#include <algorithm>

#include "DSU.h"

const std::string RANDOM = "random";
const int RUNS = 10;

int N = 10000;
int E = 100000;
int THREADS = 192;
int node_count = numa_num_configured_nodes();
int RATIO = 80;
bool RUN_ALL_RATIOS = false;

struct Context {
    //std::vector<std::vector<int>> graph;
    std::vector<std::pair<int, int>>* edges;
    DSU* dsu;
    int ratio; // процент SameSet среди всех запросов

    Context(std::vector<std::pair<int, int>>* edges, DSU* dsu, int ratio) : edges(edges), dsu(dsu), ratio(ratio) {};
};

void doSmth() {

}

void thread_routine(Context* ctx, int v1, int v2) {
    int cnt = 0;
//    for (int v = v1; v < v2; v++) {
//        for (int i = 0; i < int(ctx->graph[v].size()); i++) {
//            if (cnt % 100 < ctx->ratio) {
//                ctx->dsu->SameSet(v, ctx->graph[v][i]);
//            } else {
//                ctx->dsu->Union(v, ctx->graph[v][i]);
//            }
//            cnt = cnt + 1;
//            //doSmth();
//        }
//    }

    for (int i = v1; i < v2; i++) {
        auto e = ctx->edges->at(i);
        if (cnt % 100 < ctx->ratio) {
            ctx->dsu->SameSet(e.first, e.second);
        } else {
            ctx->dsu->Union(e.first, e.second);
        }
        cnt = 1 + cnt;
    }
}

void run(Context* ctx) {
    std::vector<std::thread> threads;

    int step = E / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step, std::min(i*step + step, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float runWithTime(Context* ctx) {
    float result = 0;

    for (int i = 0; i < RUNS; i++) {
        auto start = std::chrono::high_resolution_clock::now();
        run(ctx);
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
        result += duration.count();
    }

    result = result / RUNS;
    return result;
}

std::vector<std::pair<int, int>>* graphRandom() {
    auto g = new std::vector<std::pair<int, int>>();

    for (int i = 0; i < E ; i++) {
        int x = rand() % N;
        int y = rand() % N;
        g->emplace_back(std::make_pair(x, y));
    }

    return g;
}

std::vector<std::pair<int, int>>* graphFromFile(std::string filename) {
    std::ifstream file;
    file.open(filename);

    file >> N >> E;
    auto g = new std::vector<std::pair<int, int>>();

    if (filename[0] == 'W') {
        for (int i = 0; i < E; i++) {
            char c;
            file >> c;
            int a, b;
            file >> a >> b;
            if (a > N) {
                N = a;
            }
            if (b > N) {
                N = b;
            }
            g->emplace_back(std::make_pair(a, b));
            file >> a;
        }
    } else {
        for (int i = 0; i < E; i++) {
            int a, b;
            file >> a >> b;
            if (a > N) {
                N = a;
            }
            if (b > N) {
                N = b;
            }
            g->emplace_back(std::make_pair(a, b));
        }
    }

    std::random_device rd;
    std::mt19937 q(rd());
    std::shuffle(g->begin(), g->end(), q);
    return g;
}

void runSequential(DSU* dsu, std::vector<std::vector<int>> g) {
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

void preUnite(DSU* dsu) {
    for (int i = 0; i < E / 3; i++) {
        int x = rand() % N;
        int y = rand() % N;
        dsu->Union(x, y);
    }
}

void benchmark(std::string graph) {
    std::vector<std::pair<int, int>>* g;
    if (graph == RANDOM) {
        g = graphRandom();
    } else {
        g = graphFromFile(graph);
    }



    if (RUN_ALL_RATIOS) {
        std::ofstream out;
        out.open("results.txt");

        for (int i = 40; i <= 100; i += 5) {
            RATIO = i;
            std::cerr << i << std::endl;

            auto dsuNUMAHelper = new DSU_Helper(N, node_count);
            preUnite(dsuNUMAHelper);
            auto ctxNUMAHelper = new Context(g, dsuNUMAHelper, RATIO);
            out << "NUMAHelper " << RATIO << " " << runWithTime(ctxNUMAHelper) << "\n";

            auto dsuUsual = new DSU_Helper(N, 1);
            preUnite(dsuUsual);
            auto ctxUsual = new Context(g, dsuUsual, RATIO);
            out << "Usual " << RATIO << " " << runWithTime(ctxUsual) << "\n";

//            auto dsuNUMAMSQueue = new DSU_MSQ(N, node_count);
//            auto ctxNUMAMSQueue = new Context(&g, dsuNUMAMSQueue, RATIO);
//            out << "NUMAMSQueue " << RATIO << " " << runWithTime(ctxNUMAMSQueue) << "\n";
        }

        out.close();
    } else {
        auto dsuNUMAHelper = new DSU_Helper(N, node_count);
        auto ctxNUMAHelper = new Context(g, dsuNUMAHelper, RATIO);
        std::cout << "NUMAHelper " << runWithTime(ctxNUMAHelper) << "\n";

        auto dsuUsual = new DSU_Helper(N, 1);
        auto ctxUsual = new Context(g, dsuUsual, RATIO);
        std::cout << "Usual " << runWithTime(ctxUsual) << "\n";

//        auto dsuNUMAMSQueue = new DSU_MSQ(N, node_count);
//        auto ctxNUMAMSQueue = new Context(&g, dsuNUMAMSQueue, RATIO);
//        std::cout << "NUMAMSQueue " << runWithTime(ctxNUMAMSQueue) << "\n";
    }

    // auto dsuSeq = new DSU_Sequential(N);
    // runSequential(dsuSeq, g);
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

                if (argc > 3) {
                    RUN_ALL_RATIOS = (strcmp(argv[3], "all") == 0);
                }
            }
        }

        benchmark(graph);
    } else {
        benchmark(RANDOM);
    }
    return 0;
}