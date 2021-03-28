#include <iostream>
#include <thread>
#include <fstream>
#include <random>
#include <algorithm>

#include "DSU.h"
#include "implementations/DSU_No_Sync.h"
#include "implementations/DSU_Usual.h"
#include "graphs.h"

const std::string RANDOM = "random";
const int RUNS = 3;

int N = 10000;
int E = 100000;
int THREADS = 96;
int node_count = numa_num_configured_nodes();

int RATIO = 90;
bool RUN_ALL_RATIOS = false;
int FIRST_RATIO = 0;
int LAST_RATIO = 100;
int RATIO_STEP = 4;

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
        if (i % 100 < ctx->ratio) {
            ctx->dsu->SameSet(e.first, e.second);
        } else {
            ctx->dsu->Union(e.first, e.second);
        }
        doSmth();
    }
}

void run(ContextRatio* ctx) {
    std::vector<std::thread> threads;

    int step = (E / 2) / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step, std::min(i*step + step, E / 2)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float runWithTime(ContextRatio* ctx) {
    auto start = std::chrono::high_resolution_clock::now();
    run(ctx);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return duration.count();
}

void benchmarkSplittedGraph() {
    N = 100000000;
    E = 100000000;

    auto g1 = graphRandom(N, E);
    auto g2 = graphRandom(N, E);
    for (auto e : *g1) {
        e.first = e.first * 2;
        e.second = e.second * 2;
    }
    for (auto e : *g2) {
        e.first = e.first * 2 + 1;
        e.second = e.second * 2 + 1;
    }
    std::vector<std::pair<int, int>> G;
    std::copy(g1->begin(), g1->end(), std::back_inserter(G));
    std::copy(g2->begin(), g2->end(), std::back_inserter(G));

    N = N * 2;
    E = E * 2;

    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        auto dsuUsual = new DSU_USUAL(N);
        auto ctx = new ContextRatio(&G, dsuUsual, RATIO);
        auto res = runWithTime(ctx);
        std::cout << "Usual " << i << " " << res << "\n";

        auto dsuNoSync = new DSU_NO_SYNC(N, node_count);
        ctx->dsu = dsuNoSync;
        res = runWithTime(ctx);
        std::cout << "NoSync " << i << " " << res << "\n";
    }
}

void preUnite(ContextRatio* ctx) {
//    for (int i = E2; i < E; i++) {
//        dsu->Union(edges->at(i).first, edges->at(i).second);
//    }
    ctx->ratio = 10;
    std::vector<std::thread> threads;
    int step = (E - E / 2) / THREADS;
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step + E / 2, std::min(i*step + step + E / 2, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float median(int ratio, std::vector<std::vector<float>>* v) {
    std::vector<float> to_sort;
    for (int i = 0; i < RUNS; i++) {
        to_sort.emplace_back(v->at(i)[ratio]);
    }
    std::sort(to_sort.begin(), to_sort.end());
    return to_sort[RUNS / 2];
}

void benchmark(const std::string& graph, const std::string& outfile) {
    std::vector<std::pair<int, int>>* g;
    if (graph == RANDOM) {
        g = graphRandom(N, E);
    } else {
        g = graphFromFile(graph);
    }

    std::vector<std::vector<float>> resultsNUMA(RUNS);
    std::vector<std::vector<float>> resultsUsual(RUNS);
    std::vector<std::vector<float>> resultsNoSync(RUNS);
    for (int r = 0; r < RUNS; r++) {
        if (RUN_ALL_RATIOS) {
            std::ofstream out;
            out.open(outfile + std::to_string(r));
            //shuffle(g);

            for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
                RATIO = i;
                std::cerr << i << std::endl;

                auto dsuUsual = new DSU_USUAL(N);
                auto ctx = new ContextRatio(g, dsuUsual, RATIO);
                //ctx->dsu = dsuUsual;// = new ContextRatio(g, dsuUsual, RATIO);
                auto res = runWithTime(ctx);
                out << "Usual " << RATIO << " " << res << "\n";
                //std::cerr << res << "\n";
                resultsUsual[r].emplace_back(res);

//                auto dsuNUMAHelper = new DSU_Helper(N, node_count);
//                //auto ctx = new ContextRatio(g, dsuNUMAHelper, RATIO);
//                ctx->dsu = dsuNUMAHelper;
//                res = runWithTime(ctx);
//                out << "NUMAHelper " << RATIO << " " << res << "\n";
//                std::cerr << res << " ";
//                resultsNUMA[r].emplace_back(res);

                auto dsuNoSync = new DSU_NO_SYNC(N, node_count);
                ctx->dsu = dsuNoSync;
                res = runWithTime(ctx);
                out << "NoSync " << RATIO << " " << res << "\n";
                resultsNoSync[r].emplace_back(res);
            }

            out.close();
        }
    }
    std::ofstream out_avg;
    out_avg.open(outfile + "_average");
    std::ofstream out_median;
    out_median.open(outfile + "_median");
    int id = 0;
    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        //float avgNUMA = 0;
        float avgUsual = 0;
        float avgNoSync = 0;
        for (int r = 0; r < RUNS; r++) {
            //avgNUMA += resultsNUMA[r][id];
            avgUsual += resultsUsual[r][id];
            avgNoSync += resultsNoSync[r][id];
        }
        //avgNUMA = avgNUMA / RUNS;
        avgUsual = avgUsual / RUNS;
        avgNoSync = avgNoSync / RUNS;
        //out_avg << "NUMAHelper " << i << " " << avgNUMA << "\n";
        out_avg << "Usual " << i << " " << avgUsual << "\n";
        out_avg << "NoSync " << i << " " << avgNoSync << "\n";

        //out_median << "NUMAHelper " << i << median(id, &resultsNUMA) << "\n";
        out_median << "Usual " << i << " " << median(id, &resultsUsual) << "\n";
        out_median << "NoSync " << i << " " << median(id, &resultsNoSync) << "\n";

        id++;
    }
    out_avg.close();
}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        THREADS = std::stoi(argv[1]);
    }
    benchmarkSplittedGraph();
//    std::string graph = RANDOM;
//    std::string outfile = "default";
//    if (argc > 1) {
//        graph = argv[1];
//        if (graph == RANDOM) {
//            auto nStr = argv[2];
//            auto eStr = argv[3];
//            N = std::stoi(nStr);
//            E = std::stoi(eStr);
//            if (argc > 4) {
//                outfile = argv[4];
//            }
//        } else {
//            if (argc > 2) {
//                RUN_ALL_RATIOS = (strcmp(argv[2], "all") == 0);
//                outfile = argv[3];
//            }
//        }
//    }

//    benchmark(graph, outfile);
    return 0;
}
