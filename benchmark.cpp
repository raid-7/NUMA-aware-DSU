#include <iostream>
#include <thread>
#include <pthread.h>
#include <random>
#include <algorithm>

#include "graphs.h"
#include "DSU.h"
#include "implementations/DSU_Helper.h"
#include "implementations/DSU_No_Sync.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Parts.h"
#include "implementations/DSU_NoSync_Parts.h"
#include "implementations/TwoDSU.h"

const std::string RANDOM = "random";
const std::string SPLIT = "split";
const std::string COMPONENTS = "components";

const int RUNS = 3;

int components_number = 100;
int N = 100000;
int E = 100000;
int THREADS = std::thread::hardware_concurrency();
int node_count = numa_num_configured_nodes();

int RATIO = 90;
int FIRST_RATIO = 0;
int LAST_RATIO = 100;
int RATIO_STEP = 10;

struct ContextRatio {
    std::vector<Edge>* edges;
    DSU* dsu;
    pthread_barrier_t* barrier;
    int ratio; // процент SameSet среди всех запросов

    ContextRatio(std::vector<Edge>* edges, DSU* dsu, int ratio) : edges(edges), dsu(dsu), ratio(ratio) {};
};

int intRand(const int & min, const int & max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}

void doSmth(int x) {
    while (true) {
        if (intRand(0, 100) < x)
            break;
    }
}

void thread_routine(ContextRatio* ctx, int v1, int v2) {
    pthread_barrier_wait(ctx->barrier);
    int node = numa_node_of_cpu(sched_getcpu());

    for (int i = v1; i < v2; i++) {
        if ((i % 2) == node) {
            continue;
        }
        auto e = ctx->edges->at(i);
//std::cerr << "union " + std::to_string(e.first) + " and " + std::to_string(e.second) + " on node " + std::to_string(node) + "\n";
        if ((e.u % 2 == node) || (e.v % 2 == node)) {
            std::cerr << "mredor\n";
        }
        if (intRand(1, 100) <= ctx->ratio) {
            ctx->dsu->SameSet(e.u, e.v);
        } else {
            ctx->dsu->Union(e.u, e.v);
        }
        doSmth(20);
    }
}

void run(ContextRatio* ctx, int e) {
    std::vector<std::thread> threads;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, THREADS+1);
    ctx->barrier = &barrier;

    int step = e / THREADS;
    step = step * 2;
/*
    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step, std::min(i*step + step, e)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }
*/
    int start = 0;
    int start2 = e/2;
    for (int i = 0; i < THREADS / 4; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step + start, std::min(i*step + step + start, E)));
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i*2].native_handle(), sizeof(cpu_set_t), &cpuset);

        threads.emplace_back(std::thread(thread_routine, ctx, i*step + start, std::min(i*step + step + start, E)));
        cpu_set_t cpuset2;
        CPU_ZERO(&cpuset2);
        CPU_SET((i + THREADS/4), &cpuset2);
        pthread_setaffinity_np(threads[i*2 + 1].native_handle(), sizeof(cpu_set_t), &cpuset2);
    }

    for (int i = THREADS/2; i < (THREADS/2 + THREADS/4); i++) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        threads.emplace_back(std::thread(thread_routine, ctx, (i - THREADS/2)*step + start2, std::min((i - THREADS/2)*step + step + start2, E)));
        pthread_setaffinity_np(threads[(2*i - THREADS/2)].native_handle(), sizeof(cpu_set_t), &cpuset);

        cpu_set_t cpuset2;
        CPU_ZERO(&cpuset2);
        CPU_SET((i + THREADS/4), &cpuset2);
        threads.emplace_back(std::thread(thread_routine, ctx, (i - THREADS/2)*step + start, std::min((i - THREADS/2)*step + step + start, E)));
        pthread_setaffinity_np(threads[i*2 - THREADS/2 + 1].native_handle(), sizeof(cpu_set_t), &cpuset2);
    }
    pthread_barrier_wait(&barrier);

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

void preUnite(ContextRatio* ctx, int e) {
    int start = E - e;
    int start2 = E - (e/2);
    int ratio = 0;//ctx->ratio;
    //ctx->ratio = 10;
    std::vector<std::thread> threads;
    int step = e / THREADS;
    step = step * 2;
    for (int i = 0; i < THREADS / 4; i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, i*step + start, std::min(i*step + step + start, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i*2].native_handle(), sizeof(cpu_set_t), &cpuset);

        threads.emplace_back(std::thread(thread_routine, ctx, i*step + start, std::min(i*step + step + start, E)));
        cpu_set_t cpuset2;
        CPU_ZERO(&cpuset2);
        CPU_SET((i + THREADS/4), &cpuset2);
        pthread_setaffinity_np(threads[i*2 + 1].native_handle(), sizeof(cpu_set_t), &cpuset2);
    }

    for (int i = THREADS/2; i < (THREADS/2 + THREADS/4); i++) {
        threads.emplace_back(std::thread(thread_routine, ctx, (i - THREADS/2)*step + start2, std::min((i - THREADS/2)*step + step + start2, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[(2*i - THREADS/2)].native_handle(), sizeof(cpu_set_t), &cpuset);

        threads.emplace_back(std::thread(thread_routine, ctx, (i-THREADS/2)*step + start, std::min((i - THREADS/2)*step + step + start, E)));
        cpu_set_t cpuset2;
        CPU_ZERO(&cpuset2);
        CPU_SET((i + THREADS/4), &cpuset2);
        pthread_setaffinity_np(threads[i*2 - THREADS/2 + 1].native_handle(), sizeof(cpu_set_t), &cpuset2);
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
    for (int i = 0; i < 1; i++) {
        // preUnite(ctx, to_pre_unite);
//ctx->dsu->setStepsCount(0);
        result += runWithTime(ctx, e);
        std::cerr << ctx->dsu->ClassName() + "in ratio " + std::to_string(ctx->ratio) + ": " << ctx->dsu->getStepsCount() << std::endl;
        ctx->dsu->ReInit();
    }
    return result;// / RUNS;
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
    for (int i = filename.size(); i--; i >= 0) {
        if (filename[i] == '/') {
            break;
        }
        result = filename[i] + result;
    }
    return result;
}

void benchmark(const std::string& graph_filename) {
    std::string outfile = "not_usual_" + getLastPartOfFilename(graph_filename);
    std::vector<Edge>* g;
    if (graph_filename == RANDOM) {
        auto graph = graphRandom(N, E);
        g = graph.edges;
        outfile = outfile + "_" + std::to_string(N) + "_" + std::to_string(E);
    } else {
        auto graph = (graph_filename == COMPONENTS) ? generateComponentsShuffled(components_number, N, E) : graphFromFile(graph_filename);
        N = graph.N;
        E = graph.E;
        g = graph.edges;
    }

    std::ofstream out;
    out.open(outfile);


    std::vector<DSU*> dsus;
    dsus.push_back(new DSU_Usual(N));
    dsus.push_back(new DSU_ParallelUnions(N, node_count));
    dsus.push_back(new DSU_Parts(N, node_count));
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
            out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
        }
    }

    out.close();

/////////////////////////////////////////////////

    for (int pu = 0; pu < 100; pu += 20) {
        std::string new_outfile = outfile + "_" + std::to_string(pu);
        out.open(new_outfile);

        for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
            RATIO = i;
            std::cerr << i << std::endl;

            auto ctx = new ContextRatio(g, dsus[0], RATIO);

            for (int j = 0; j < dsus.size(); j++) {
                ctx->dsu = dsus[j];
                auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite / 100 * pu);
                out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
            }
        }
        out.close();
    }
}

void benchmark_components(const std::string& graph_filename) {
    std::string outfile = "test_components_with_status_updates" + std::to_string(N) + "_" + std::to_string(E);

    components_number = 2;
    for (int i = 0; i < 1; i++) {
        auto graph = generateComponentsShuffled(components_number, N / components_number, E / components_number);
        N = graph.N;
        E = graph.E;
        std::cerr << "E:: " << E << "\n";
        std::vector<Edge>* g = graph.edges;

        std::ofstream out;
        out.open(outfile + "_" + std::to_string(components_number));

        std::vector<DSU*> dsus;
        dsus.push_back(new DSU_Usual(N));
        dsus.push_back(new TwoDSU(N, node_count));
        dsus.push_back(new DSU_ParallelUnions(N, node_count));
        dsus.push_back(new DSU_NO_SYNC(N, node_count));
        dsus.push_back(new DSU_Parts(N, node_count));
        dsus.push_back(new DSU_NoSync_Parts(N, node_count));

        int edges_to_pre_unite = 0;//E / 10 * 4;
        int edges_to_test = E - edges_to_pre_unite;
        for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
            RATIO = i;
            std::cerr << i << std::endl;
            auto edges_to_pre_unite_on_step = edges_to_pre_unite;// / 100 * RATIO;

            auto ctx = new ContextRatio(g, dsus[0], RATIO);

            for (int j = 0; j < dsus.size(); j++) {
                ctx->dsu = dsus[j];
                for (int r = 0; r < RUNS; r++) {
                    auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite_on_step);
                    out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
                }
            }
        }

        out.close();

/////////////////////////////////////////////////
/*        for (int pu = 0; pu < 100; pu += 40) {
std::cerr << "preunion: " + std::to_string(pu) << std::endl;
            std::string new_outfile = outfile + "_" + std::to_string(n) + "_" + std::to_string(pu);
            out.open(new_outfile);

            for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
                RATIO = i;
                std::cerr << i << std::endl;

                auto ctx = new ContextRatio(g, dsus[0], RATIO);

                for (int j = 0; j < dsus.size(); j++) {
                    ctx->dsu = dsus[j];
                    auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite / 100 * pu);
                    out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
                }
            }
            out.close();
        }
*/
        out.close();
        components_number = components_number * 4;
    }
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
            N = std::stoi(argv[2]);
            E = std::stoi(argv[3]);
        }
        benchmark_components(graph);
        return 0;
    }

    benchmark(graph);

    return 0;
}
/*
#include <iostream>
#include <thread>
#include <random>
#include <algorithm>

#include "graphs.h"
#include "DSU.h"
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
    for (int i = filename.size(); i--; i >= 0) {
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
    dsus.push_back(new DSU_Usual(N));
    dsus.push_back(new DSU_ParallelUnions(N, node_count));
    // dsus.push_back(new DSU_Helper(N, node_count));
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
            out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
        }
    }

    out.close();

/////////////////////////////////////////////////

    for (int pu = 0; pu < 100; pu += 20) {
        std::string new_outfile = outfile + "_" + std::to_string(pu);
        out.open(new_outfile);

        for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
            RATIO = i;
            std::cerr << i << std::endl;

            auto ctx = new ContextRatio(g, dsus[0], RATIO);

            for (int j = 0; j < dsus.size(); j++) {
                ctx->dsu = dsus[j];
                auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite / 100 * pu);
                out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
            }
        }
        out.close();
    }

}

void benchmark_components(const std::string& graph_filename) {
    std::string outfile = "components_" + std::to_string(N) + "_" + std::to_string(E);

    n = 1;
    for (int i = 0; i < 6; i++) {
        auto graph = generateComponentsShuffled(n, N / n, E / n);
        N = graph.N;
        E = graph.E;
        std::vector<std::pair<int, int>>* g = graph.edges;

        std::ofstream out;
        out.open(outfile + "_" + std::to_string(n));

        std::vector<DSU*> dsus;
        dsus.push_back(new DSU_Usual(N));
        dsus.push_back(new DSU_ParallelUnions(N, node_count));
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
                out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
            }
        }

        out.close();

/////////////////////////////////////////////////

        for (int pu = 0; pu < 100; pu += 20) {
            std::string new_outfile = outfile + "_" + std::to_string(n) + "_" + std::to_string(pu);
            out.open(new_outfile);

            for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
                RATIO = i;
                std::cerr << i << std::endl;

                auto ctx = new ContextRatio(g, dsus[0], RATIO);

                for (int j = 0; j < dsus.size(); j++) {
                    ctx->dsu = dsus[j];
                    auto res = getAverageTime(ctx, edges_to_test, edges_to_pre_unite / 100 * pu);
                    out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
                }
            }
            out.close();
        }

        out.close();
        n = n * 4;
    }
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
            N = std::stoi(argv[2]);
            E = std::stoi(argv[3]);
        }
        benchmark_components(graph);
        return 0;
    }

    benchmark(graph);

    return 0;
}
*/