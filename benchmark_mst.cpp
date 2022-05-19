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
const std::string COMPONENTS = "components";

const int RUNS = 3;

int components_number = 100;
int N = 100000;
int E = 100000;

int THREADS = 36;//std::thread::hardware_concurrency();
int node_count = 2;//numa_num_configured_nodes();

struct Context {
    std::vector<Edge>* edges;
    DSU* dsu;
    std::vector<std::atomic_int*> best_edges; // для каждой вершины хранит индекс лучшего ребра
    std::vector<std::atomic_int*> is_root;
    std::atomic_int* roots_count;

    Context(std::vector<Edge>* edges, DSU* dsu) : edges(edges), dsu(dsu) {
        std::cerr << "in context init\n";
        edges->push_back(Edge(0, 0, 1e9));
        best_edges.resize(N);
        is_root.resize(N);
        std::cerr << "before for\n";
        for (int i = 0; i < N; i++) {
            best_edges[i] = (std::atomic_int*) malloc(sizeof(std::atomic_int));
            best_edges[i]->store(E);
            is_root[i] = (std::atomic_int*) malloc(sizeof(std::atomic_int));
            is_root[i]->store(0);
        }
        std::cerr << "after for\n";
        roots_count = (std::atomic_int*) malloc(sizeof(std::atomic_int));
        roots_count->store(N);
    };

    void ReInit() {
        for (int i = 0; i < N; i++) {
            best_edges[i]->store(E);
            is_root[i]->store(0);
        }
        roots_count->store(N);
    }
};

// для каждой вершины с v1 до v2 находит лучшее ребро, которое не добавляет в цикл
void thread_routine_first(Context* ctx, int from, int to) {
    for (int i = from; i < to; i++) {
        auto e = ctx->edges->at(i);
        if (!ctx->dsu->SameSet(e.u, e.v)) {
            int prev_best_id;
            int u = ctx->dsu->Find(e.u); ctx->is_root[u]->store(1);
            int v = ctx->dsu->Find(e.v); ctx->is_root[v]->store(1);
            // меняем лучшее ребро для u
            while (true) {
                prev_best_id = ctx->best_edges[u]->load(std::memory_order_acquire);
                if (ctx->edges->at(prev_best_id).w > e.w) {
                    if (ctx->best_edges[u]->compare_exchange_weak(prev_best_id, i)) {
                        break;
                    }
                } else {
                    break;
                }
            }
            // меняем лучшее ребро для u
            while (true) {
                prev_best_id = ctx->best_edges[v]->load(std::memory_order_acquire);
                if (ctx->edges->at(prev_best_id).w > e.w) {
                    if (ctx->best_edges[v]->compare_exchange_weak(prev_best_id, i)) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }
}

// добавляем выбранные ребра в дсу и поддерживаем компоненты
void thread_routine_second(Context* ctx, int from, int to) {
    for (int i = from; i < to; i++) {
        if (ctx->is_root[i]->load(std::memory_order_acquire) == 0) {
            continue;
        }
        ctx->roots_count->fetch_add(-1);
        auto e = ctx->edges->at(ctx->best_edges[i]->load(std::memory_order_acquire));
        ctx->dsu->Union(e.u, e.v);
    }
}

void run_first_routine(Context* ctx) {
    std::vector<std::thread> threads;
    int step = E / THREADS;

    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine_first, ctx, i*step, std::min(i*step + step, E)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (auto & thread : threads) {
        thread.join();
    }
}

void run_second_routine(Context* ctx) {
    std::vector<std::thread> threads;
    int step = N / THREADS;

    for (int i = 0; i < THREADS; i++) {
        threads.emplace_back(std::thread(thread_routine_second, ctx, i*step, std::min(i*step + step, N)));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
    }

    for (auto & thread : threads) {
        thread.join();
    }
}

void mst(Context* ctx) {
    int prev_roots = N;
    while (true) {
        for (int i = 0; i <  N; i++) {
            ctx->best_edges[i]->store(E);
            ctx->is_root[i]->store(0);
        }
        //std::cerr << "before 1st routine\n";
        run_first_routine(ctx);
        //std::cerr << "after 1st routine\n";
        run_second_routine(ctx);
        //std::cerr << "after 2d routine\n";
        int now_roots = ctx->roots_count->load();
        //std::cerr << now_roots << "\n";
        if (now_roots== prev_roots) {break;}
        prev_roots = now_roots;
        if (now_roots< (THREADS * 2)) {
            break;
        }
    }
}

float runWithTime(Context* ctx) {
    auto start = std::chrono::high_resolution_clock::now();
    mst(ctx);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return duration.count();
}

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
    std::string outfile = "mst_" + getLastPartOfFilename(graph_filename);
    std::vector<Edge>* g;
    if (graph_filename == RANDOM) {
        auto graph = graphRandom(N, E);
        g = graph.edges;
    } else {
        auto graph = (graph_filename == COMPONENTS) ? generateComponentsShuffled(components_number, N/components_number, E/components_number) : graphFromFile(graph_filename);
        N = graph.N;
        E = graph.E;
        g = graph.edges;
    }
    std::cerr << "N:: " << N << " and E:: " << E << "\n";
    outfile = outfile + "_" + std::to_string(N) + "_" + std::to_string(E);
    std::ofstream out;
    out.open(outfile);
std::cerr << "before dsus\n";
    std::vector<DSU*> dsus;
    dsus.push_back(new DSU_Usual(N));
    //dsus.push_back(new TwoDSU(N, node_count));
    dsus.push_back(new DSU_ParallelUnions(N, node_count));
    dsus.push_back(new DSU_NO_SYNC(N, node_count));
    // dsus.push_back(new DSU_Parts(N, node_count));
    // dsus.push_back(new DSU_NoSync_Parts(N, node_count));
std::cerr << "after dsus\n";

    Context* ctx = new Context(g, dsus[0]);
std::cerr << "after context\n";

    for (int i = 0; i < dsus.size(); i++) {
        ctx->dsu = dsus[i];
        for (int run = 0; run < RUNS; run++) {
            ctx->dsu->ReInit();
            ctx->ReInit();
            auto result = runWithTime(ctx);
            out << ctx->dsu->ClassName() << " " << result << "\n";
            std::cerr << ctx->dsu->ClassName() << "done\n";
        }
    }

    out.close();
}

int main(int argc, char* argv[]) {
    std::string graph = argv[1];

    if (graph == "full") {
        N = 10000000;
        E = 10000000;
        for (int i = 0; i < 7; i++) {
            std::cerr << i << std::endl;
            benchmark(RANDOM);
            E = E * 2;
        }
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
        benchmark(graph);
        return 0;
    }

    benchmark(graph);

    return 0;
}
