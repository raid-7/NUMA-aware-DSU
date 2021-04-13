#include <iostream>
#include <thread>
#include <fstream>
#include <random>
#include <algorithm>

#include "DSU.h"
#include "implementations/DSU_No_Sync.h"
#include "implementations/DSU_Usual.h"
#include "graphs.h"
#include "implementations/DSU_Helper.h"
#include "implementations/DSU_CircularBuffer.h"
#include "implementations/DSU_SyncOnNode.h"
#include "implementations/DSU_ParallelUnions.h"

const std::string RANDOM = "random";
const std::string SPLIT = "split";

const int RUNS = 3;

int N = 10000;
int E = 100000;
int THREADS = std::thread::hardware_concurrency();
int node_count = numa_num_configured_nodes();

int RATIO = 90;
int FIRST_RATIO = 5000;
int LAST_RATIO = 10000;
int RATIO_STEP = 100;

struct ContextRatio {
    std::vector<std::pair<int, int>>* edges;
    DSU* dsu;
    int ratio; // процент SameSet среди всех запросов

    ContextRatio(std::vector<std::pair<int, int>>* edges, DSU* dsu, int ratio) : edges(edges), dsu(dsu), ratio(ratio) {};
};

int intRand(const int & min, const int & max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}

void doSmth() {
    while (true) {
        if (intRand(0, 1000) < 10)
            break;
    }
}

void thread_routine(ContextRatio* ctx, int v1, int v2) {
    int node = numa_node_of_cpu(sched_getcpu());
    //numa_run_on_node(node);
    for (int i = v1; i < v2; i++) {
        auto e = ctx->edges->at(i);
        if (i % 10000 < ctx->ratio) {
            ctx->dsu->SameSet(e.first, e.second);
        } else {
            ctx->dsu->Union(e.first, e.second);
        }
        // doSmth();
    }
}

void run(ContextRatio* ctx, int e) {
    std::vector<std::thread> threads;

    int step = e / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step, std::min(i*step + step, e)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float runWithTime(ContextRatio* ctx, int e) {
    auto start = std::chrono::high_resolution_clock::now();
    run(ctx, e);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return duration.count();
}

float getAverageTime(ContextRatio* ctx, int e) {
    auto result = 0;
    for (int i = 0; i < RUNS; i++) {
        result += runWithTime(ctx, e);
        ctx->dsu->ReInit();
    }
    return result / RUNS;
}

void benchmarkSplittedGraph() {
    N = 40000000;
    E = 144000000;

    auto g1 = graphRandom(N, E).edges;
    auto g2 = graphRandom(N, E).edges;
    for (int i = 0; i < E; i++) {
        g1->at(i).first *= 2;
        g1->at(i).second *= 2;
    }
    for (int i = 0; i < E; i++) {
        g2->at(i).first *= 2;
        g2->at(i).second *= 2;
        g2->at(i).first += 1;
        g2->at(i).second += 1;
    }
    std::vector<std::pair<int, int>> G;
    std::copy(g1->begin(), g1->end(), std::back_inserter(G));
    std::copy(g2->begin(), g2->end(), std::back_inserter(G));

    N = N * 2;
    E = E * 2;

    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        auto dsuUsual = new DSU_USUAL(N);
        auto ctx = new ContextRatio(&G, dsuUsual, RATIO);
        auto res = getAverageTime(ctx, E);
        std::cout << "Usual " << i << " " << res << "\n";

        auto dsuNoSync = new DSU_NO_SYNC(N, node_count);
        ctx->dsu = dsuNoSync;
        res = getAverageTime(ctx, E);
        std::cout << "NoSync " << i << " " << res << "\n";
    }
}

void preUnite(ContextRatio* ctx, int e) {
    int start = E - e;
    int ratio = ctx->ratio;
    ctx->ratio = 10;
    std::vector<std::thread> threads;
    int step = e / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step + start, std::min(i*step + step + start, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
    ctx->ratio = ratio;
}

float median(int ratio, std::vector<std::vector<float>>* v) {
    std::vector<float> to_sort;
    for (int i = 0; i < RUNS; i++) {
        to_sort.emplace_back(v->at(i)[ratio]);
    }
    std::sort(to_sort.begin(), to_sort.end());
    return to_sort[RUNS / 2];
}

void benchmark(const std::string& graph_filename, const std::string& outfile) {
    std::vector<std::pair<int, int>>* g;
    if (graph_filename == RANDOM) {
        auto graph = graphRandom(N, E);
        g = graph.edges;
    } else {
        auto graph = graphFromFile(graph_filename);
        N = graph.N;
        E = graph.E;
        g = graph.edges;
    }

    std::ofstream out;
    out.open(outfile);

    int edges_to_pre_unite = 0;//E / 4;
    int edges_to_test = E - edges_to_pre_unite;
    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        RATIO = i;
        std::cerr << i << std::endl;

        auto dsuUsual = new DSU_USUAL(N);
        auto ctx = new ContextRatio(g, dsuUsual, RATIO);
        //preUnite(ctx, edges_to_pre_unite);
        auto res = getAverageTime(ctx, edges_to_test);
        out << "Usual " << RATIO << " " << res << "\n";

        auto dsuCircular = new DSU_CircularBuffer(N, node_count);
        ctx->dsu = dsuCircular;
        //preUnite(ctx, edges_to_pre_unite);
        res = getAverageTime(ctx, edges_to_test);
        out << "CircularBuffer " << RATIO << " " << res << "\n";

        auto dsuParallelUnions = new DSU_ParallelUnions(N, node_count);
        ctx->dsu = dsuParallelUnions;
        //preUnite(ctx, edges_to_pre_unite);
        res = getAverageTime(ctx, edges_to_test);
        out << "ParallelUnions " << RATIO << " " << res << "\n";

//        auto dsuCircular = new DSU_CircularBuffer(N, node_count);
//        ctx->dsu = dsuCircular;
//        //preUnite(ctx, edges_to_pre_unite);
//        res = getAverageTime(ctx, edges_to_test);
//        out << "CircularBuffer " << RATIO << " " << res << "\n";

//        auto dsuNUMAHelper = new DSU_Helper(N, node_count);
//        ctx->dsu = dsuNUMAHelper;
//        preUnite(ctx, edges_to_pre_unite);
//        res = getAverageTime(ctx, edges_to_test);
//        out << "NUMAHelper " << RATIO << " " << res << "\n";
//
//        auto dsuNoSync = new DSU_NO_SYNC(N, node_count);
//        ctx->dsu = dsuNoSync;
//        preUnite(ctx, edges_to_pre_unite);
//        res = getAverageTime(ctx, edges_to_test);
//        out << "NoSync " << RATIO << " " << res << "\n";
    }

    out.close();
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cout << "check readme.md" << std::endl;
        return 0;
    }

    std::string graph = argv[1];
    if (graph == SPLIT) {
        if (argc > 2) {
            THREADS = std::stoi(argv[2]);
        }
        benchmarkSplittedGraph();
    }

    std::string outfile = "default";
    if (graph == RANDOM) {
        if (argc > 2) {
            N = std::stoi(argv[2]);
            E = std::stoi(argv[3]);
        }
        benchmark(graph, outfile);
        return 0;
    }

    if (argc > 2) {
        outfile = argv[3];
    }
    benchmark(graph, outfile);

    return 0;
}
