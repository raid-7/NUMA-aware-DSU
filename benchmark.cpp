#include <iostream>
#include <thread>
#include <fstream>
#include <random>
#include <algorithm>
#include <typeinfo>

#include "graphs.h"
#include "DSU.h"
#include "implementations/DSU_CircularBuffer.h"
#include "implementations/DSU_Helper.h"
#include "implementations/DSU_No_Sync.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_Usual.h"

const std::string RANDOM = "random";
const std::string SPLIT = "split";
const std::string COMPONENTS = "components";

const int RUNS = 3;

int n = 100;
int N = 100000;
int E = 100000;
int THREADS = std::thread::hardware_concurrency();
int node_count = numa_num_configured_nodes();

int RATIO = 90;
int FIRST_RATIO = 40;
int LAST_RATIO = 100;
int RATIO_STEP = 5;

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
        if (intRand(0, 100) < ctx->ratio) {
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

float runWithTime(ContextRatio* ctx, int e) {
    auto start = std::chrono::high_resolution_clock::now();
    run(ctx, e);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return duration.count();
}

float getAverageTime(ContextRatio* ctx, int e, int to_pre_unite) {
    auto result = 0;
    for (int i = 0; i < RUNS; i++) {
        preUnite(ctx, to_pre_unite);
        result += runWithTime(ctx, e);
        ctx->dsu->ReInit();
    }
    return result / RUNS;
}

//void benchmarkSplittedGraph() {
//    N = 40000000;
//    E = 144000000;
//
//    auto g1 = graphRandom(N, E).edges;
//    auto g2 = graphRandom(N, E).edges;
//    for (int i = 0; i < E; i++) {
//        g1->at(i).first *= 2;
//        g1->at(i).second *= 2;
//    }
//    for (int i = 0; i < E; i++) {
//        g2->at(i).first *= 2;
//        g2->at(i).second *= 2;
//        g2->at(i).first += 1;
//        g2->at(i).second += 1;
//    }
//    std::vector<std::pair<int, int>> G;
//    std::copy(g1->begin(), g1->end(), std::back_inserter(G));
//    std::copy(g2->begin(), g2->end(), std::back_inserter(G));
//
//    N = N * 2;
//    E = E * 2;
//
//    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
//        auto dsuUsual = new DSU_USUAL(N);
//        auto ctx = new ContextRatio(&G, dsuUsual, RATIO);
//        auto res = getAverageTime(ctx, E);
//        std::cout << "Usual " << i << " " << res << "\n";
//
//        auto dsuNoSync = new DSU_NO_SYNC(N, node_count);
//        ctx->dsu = dsuNoSync;
//        res = getAverageTime(ctx, E);
//        std::cout << "NoSync " << i << " " << res << "\n";
//    }
//}



std::string getLastPartOfFilename(std::string filename) {
    std::string result = "";
    for (int i = filename.size() - 1; i--; i >= 0) {
        if (filename[i] == '/') {
            break;
        }
        result = filename[i] + result;
    }
    return result;
}

void benchmark(const std::string& graph_filename) {
    std::string outfile = "usual_" + getLastPartOfFilename(graph_filename);
    std::vector<std::pair<int, int>>* g;
    if (graph_filename == RANDOM) {
        auto graph = graphRandom(N, E);
        g = graph.edges;
        outfile = outfile + "_" + std::to_string(N) + "_" + std::to_string(E);
    } else {
        auto graph = (graph_filename == COMPONENTS)? generateComponentsShuffled(n, N, E) : graphFromFile(graph_filename);
        N = graph.N;
        E = graph.E;
        g = graph.edges;
    }

    std::ofstream out;
    out.open(outfile);


    std::vector<DSU*> dsus;
    dsus.push_back(new DSU_USUAL(N));
    dsus.push_back(new DSU_ParallelUnions(N, node_count));
    dsus.push_back(new DSU_Helper(N, node_count));
    dsus.push_back(new DSU_NO_SYNC(N, node_count));

    int edges_to_pre_unite = E / 2;
    int edges_to_test = E - edges_to_pre_unite;
    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        RATIO = i;
        std::cerr << i << std::endl;
        auto edges_to_pre_unite_on_step = edges_to_pre_unite / 100 * RATIO;

        auto ctx = new ContextRatio(g, dsus[0], RATIO);

        for (int j = 0; j < dsus.size(); j++) {
            ctx->dsu = dsus[j];
            auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite_on_step);
            out << typeid(dsus[j]).name() << " " << RATIO << " " << res << "\n";
        }


//        auto dsuParallelUnions = new DSU_ParallelUnions(N, node_count);
//        ctx->dsu = dsuParallelUnions;
//        res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite_on_step);
//        out << "ParallelUnions " << RATIO << " " << res << "\n";
//
//        auto dsuCircular = new DSU_CircularBuffer(N, node_count);
//        ctx->dsu = dsuCircular;
//        res = getAverageTime(ctx, edges_to_test);
//        out << "CircularBuffer " << RATIO << " " << res << "\n";
//
//        auto dsuNUMAHelper = new DSU_Helper(N, node_count);
//        ctx->dsu = dsuNUMAHelper;
//        res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite_on_step);
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
        //benchmarkSplittedGraph();
        return 0;
    }

    if (graph == RANDOM) {
        if (argc > 2) {
            N = std::stoi(argv[2]);
            E = std::stoi(argv[3]);
        }
        benchmark(graph);
        return 0;
    }

    if (graph == COMPONENTS) {
        if (argc > 2) {
            n = std::stoi(argv[2]);
            if (argc > 3) {
                N = std::stoi(argv[3]);
                E = std::stoi(argv[4]);
            }
        }
        benchmark(graph);
        return 0;
    }

    benchmark(graph);

    return 0;
}
