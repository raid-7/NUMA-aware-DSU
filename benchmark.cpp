#include <iostream>
#include <thread>
#include <pthread.h>
#include <random>
#include <algorithm>
#include <set>

#include "graphs.h"
#include "DSU.h"
#include "implementations/DSU_Helper.h"
#include "implementations/DSU_No_Sync.h"
#include "implementations/DSU_ParallelUnions.h"
#include "implementations/DSU_Usual.h"
#include "implementations/DSU_Parts.h"
#include "implementations/DSU_NoSync_Parts.h"
#include "implementations/TwoDSU.h"
#include "implementations/DSU_FC.h"
#include "implementations/DSU_FC_honest.h"
#include "implementations/DSU_FC_on_seq.h"

const std::string RANDOM = "random";
const std::string SPLIT = "split";
const std::string COMPONENTS = "components";

const int RUNS = 3;

int components_number = 100;
int N = 100000;
int E = 100000;
int THREADS = 32;//64;//std::thread::hardware_concurrency();
int node_count = 4;//2;//4;//numa_num_configured_nodes();

int RATIO = 90;
int FIRST_RATIO = 0;
int LAST_RATIO = 100;
int RATIO_STEP = 10;

struct ContextRatio {
    std::vector<std::vector<Edge>>* edges;
    DSU* dsu;
    pthread_barrier_t* barrier;
    int ratio; // процент SameSet среди всех запросов

    ContextRatio(std::vector<std::vector<Edge>>* edges, DSU* dsu, int ratio) : edges(edges), dsu(dsu), ratio(ratio) {};
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
        auto e = ctx->edges->at(node).at(i);
//std::cerr << "union " + std::to_string(e.first) + " and " + std::to_string(e.second) + " on node " + std::to_string(node) + "\n";
        if (intRand(1, 100) <= ctx->ratio) {
            ctx->dsu->SameSet(e.u, e.v);
        } else {
            ctx->dsu->Union(e.u, e.v);
        }
        doSmth(20);
    }
}

void run(ContextRatio* ctx, int percent) {
    std::vector<std::thread> threads;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, THREADS+1);
    ctx->barrier = &barrier;

    std::vector<int> steps(node_count);
    std::vector<int> ends(node_count);
    std::vector<int> th_done(node_count);
    for (int i = 0; i < node_count; i++) {
        int e = (ctx->edges->at(i).size() / 100) * percent;
        th_done[i] = 0;
        ends[i] = e;
        steps[i] = e / THREADS;
    }

    int th_number = 0;
    for (int i = 0; i < THREADS; i++) {
        int node;
        while (true) {
            node = numa_node_of_cpu(th_number);
            if (node > node_count - 1) {
                th_number++;
            } else {
                break;
            }
        }

        threads.emplace_back(std::thread(thread_routine, ctx,
                                         th_done[node]*steps[node],
                                         std::min(th_done[node]*steps[node] + steps[node], ends[node])));

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(th_number, &cpuset);
        pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
        th_done[node]++;
    }
    pthread_barrier_wait(&barrier);

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

void preUnite(ContextRatio* ctx, int percent) {
    std::vector<int> starts(node_count);
    std::vector<int> steps(node_count);
    for (int i = 0; i < node_count; i++) {
        int e = (ctx->edges->at(i).size() / 100) * percent;
        starts[i] = ctx->edges->at(i).size() - e;
        steps[i] = e / THREADS;
    }
    int ratio = ctx->ratio;
    ctx->ratio = 0;
    std::vector<std::thread> threads;
    for (int i = 0; i < THREADS; i++) {
        int node = numa_node_of_cpu(i);
        threads.emplace_back(std::thread(thread_routine, ctx,
                                         i*steps[node] + starts[node],
                                         std::min(i*steps[node] + steps[node] + starts[node], int(ctx->edges->at(i).size()))));

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

float runWithTime(ContextRatio* ctx, int edges_percent) {
    auto start = std::chrono::high_resolution_clock::now();
    run(ctx, edges_percent);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return duration.count();
}

float getAverageTime(ContextRatio* ctx, int pre_unite_percent) {
    auto result = 0;
    for (int i = 0; i < 1; i++) {
        // preUnite(ctx, pre_unite_percent);
//ctx->dsu->setStepsCount(0);
        result += runWithTime(ctx, (100 - pre_unite_percent));
        std::cerr << ctx->dsu->ClassName() + " in ratio " + std::to_string(ctx->ratio) + ": " << ctx->dsu->getStepsCount() << std::endl;
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

std::vector<int> parts(N);
std::vector<int> parts_sizes(node_count);
std::vector<int> wrongs(N);

std::vector<std::vector<int>> edges_from_to(node_count);

// val + id
std::vector<std::set<std::pair<int, int>, std::greater<std::pair<int, int>>>> wrongs_sets(node_count);

std::vector<std::vector<std::set<std::pair<int, int>, std::greater<std::pair<int, int>>>>> wrongs_sets_from_to(node_count);

bool partIsTooSmall(int node) {
    int mx = 0;
    for (int i = 0; i < node_count; i++) { mx = std::max(mx, parts_sizes[i]); }
    return (parts_sizes[node] < mx * 0.9);
}

void make_parts_with_intersection_on4nodes(std::vector<Edge>* g) {
    std::vector<std::vector<int>> gg(N);

    for (int i = 0; i < node_count; i++) {
        parts_sizes[i] = 0;
    }
    parts.resize(N);
    for (int i = 0; i < node_count; i++) {
        edges_from_to[i].resize(N);
    }
    gg.resize(N);

    for (int i = 0; i < N; i++) {
        parts[i] = i % node_count;
        for (int j = 0; j < node_count; j++) {
            edges_from_to[j][i] = 0;
        }
        parts_sizes[i % node_count]++;
    }

    int cnt = 0;
    for (int i = 0; i < E; i++) {
        int u = g->at(i).u;
        int v = g->at(i).v;
        gg[u].emplace_back(v);
        gg[v].emplace_back(u);

        edges_from_to[parts[v]][u]++;
        edges_from_to[parts[u]][v]++;
        if (parts[v] != parts[u]) {
            cnt++;
        }
    }

    for (int i = 0; i < gg[83516].size(); i++) {
        std::cout << gg[83516][i] << " ";
    }
    std::cout << "\n";

    for (int i = 0; i < node_count; i++) {
        wrongs_sets_from_to[i].resize(node_count);
    }


    for (int i = 0; i < N; i++) {
        for (int node = 0; node < node_count; node++) {
            if (node == parts[i]) { continue;}
            wrongs_sets_from_to[parts[i]][node].insert(
                    std::make_pair(edges_from_to[node][i] - edges_from_to[parts[i]][i], i));
            if (i == 83801 && node == 0) {
                std::cout << "!!!! " << edges_from_to[node][i] - edges_from_to[parts[i]][i] << " " << i << "\n";
            }
        }
    }

    std::cout << "parts ininted\n";
    int small_part = -1;
    int it = 0;
    while (true) {
        it++;
        //if (it % 1000 == 0) {
            std::cout << "on iteration " << it << " with E = " << E << " cnt is " << cnt << std::endl;
        //}

        int id = -1;
        int id_val = 0;
        int id_to = 0;
        for (int node1 = 0; node1 < node_count; node1++) {
            for (int node2 = 0; node2 < node_count; node2++) {
                std::cout << node1 << "+" << node2 << "\n";
                if (node1 == node2) {continue;}
                if (partIsTooSmall(node1)) {continue;}

                std::pair<int, int> best = *wrongs_sets_from_to[node1][node2].begin();
                if (best.first <= 0) {continue;}
                if (id == -1) {
                    id = best.second;
                    id_val = best.first;
                    id_to = node2;
                    continue;
                }
                if (best.first > id_val) {
                    id = best.second;
                    id_val = best.first;
                    id_to = node2;
                }
            }
        }
        if (id == -1) {
            std::cout << ":(" << std::endl;
            break;
        }

//std::cout << id << " " << id_val << " " << id_to << "\n";
        wrongs_sets_from_to[parts[id]][id_to].erase(wrongs_sets_from_to[parts[id]][id_to].begin());
//std::cout << "erased\n";
//std::cout << gg[id].size() << "\n";
        for (int i = 0; i < int(gg[id].size()); i++) {
            //std::cout << i << "\n";
            int u = gg[id][i];
            if (parts[u] != id_to) { //b
                //std::cout << "in first if\n";
                //std::cout << "!!!! " << edges_from_to[id_to][u] - edges_from_to[parts[u]][u]<< " " << u << "\n";
                if (wrongs_sets_from_to[parts[u]][id_to].find(
                        std::make_pair(edges_from_to[id_to][u] - edges_from_to[parts[u]][u], u)) != wrongs_sets_from_to[parts[u]][id_to].end()) {

                    wrongs_sets_from_to[parts[u]][id_to].erase(
                            wrongs_sets_from_to[parts[u]][id_to].find(
                                    std::make_pair(edges_from_to[id_to][u] - edges_from_to[parts[u]][u], u)
                            )
                    );

                    edges_from_to[id_to][u]++;
                    cnt--;

                    wrongs_sets_from_to[parts[u]][id_to].insert(
                            std::make_pair(edges_from_to[id_to][u] - edges_from_to[parts[u]][u], gg[id][i]));
                }
            }

            if (parts[u] != parts[id]) { // a
                //std::cout << "in second if\n";
                //std::cout << "!!!! " << edges_from_to[id_to][u] - edges_from_to[parts[u]][u]<< " " << u << "\n";
                if (wrongs_sets_from_to[parts[u]][parts[id]].find(
                        std::make_pair(edges_from_to[parts[id]][u] - edges_from_to[parts[u]][u], u))
                        != wrongs_sets_from_to[parts[gg[id][i]]][parts[id]].end()) {

                    wrongs_sets_from_to[parts[u]][parts[id]].erase(
                            wrongs_sets_from_to[parts[u]][parts[id]].find(
                                    std::make_pair(edges_from_to[parts[id]][u] - edges_from_to[parts[u]][u], u)
                            )
                    );
                   // std::cout << "erased here\n";

                    edges_from_to[parts[id]][i]++;
                    cnt++;

                    wrongs_sets[parts[u]].insert(
                            std::make_pair(edges_from_to[parts[id]][u] - edges_from_to[parts[u]][u], u));
                }
            }

        }

        parts_sizes[parts[id]]--;
        int prev_part = parts[id];
        parts[id] = id_to;
        parts_sizes[parts[id]]++;

        wrongs_sets_from_to[parts[id]][prev_part].insert(
                std::make_pair(edges_from_to[prev_part][id] - edges_from_to[parts[id]][id], id));

        if (E / cnt >= 5) {
            break;
        }
    }

    int cnt_check = 0;
    for (int i = 0; i < E; i++) {
        int u = g->at(i).u;
        int v = g->at(i).v;
        if ((parts[v]) != (parts[u])) {
            cnt_check++;
        }
    }
    std::cout << "with E = " << E << " cnt_check is " << cnt_check << std::endl;
    for (int i = 0; i < node_count; i++) {
        std::cout << "size of part #" << i << " is " << parts_sizes[i] << "\n";
    }
    //std::cout << "sizes: left=" << parts_sizes[0] << " right=" << parts_sizes[1] << std::endl;
}

void make_parts_with_intersection_on2nodes(std::vector<Edge>* g) {
    std::vector<std::vector<int>> gg(N);

    for (int i = 0; i < node_count; i++) {
        parts_sizes[i] = 0;
    }
    parts.resize(N);
    wrongs.resize(N);
    gg.resize(N);
    for (int i = 0; i < N; i++) {
        parts[i] = i % 2;
        wrongs[i] = 0;
        parts_sizes[i%2]++;
    }
    int cnt = 0;
    for (int i = 0; i < E; i++) {
        int u = g->at(i).u;
        int v = g->at(i).v;
        gg[u].emplace_back(v);
        gg[v].emplace_back(u);
        if ((v % 2) != (u % 2)) {
            wrongs[u]++;
            wrongs[v]++;
            cnt++;
        }
    }
    for (int i = 0; i < N; i++) {
        wrongs_sets[parts[i]].insert(std::make_pair(2*wrongs[i] - gg[i].size(), i));
    }
    std::cout << "parts ininted\n";
    int small_part = -1;
    int it = 0;
    while (true) {
        it++;
        if (it % 1000 == 0) {
            std::cout << "on iteration " << it << " with E = " << E << " cnt is " << cnt << std::endl;
        }
        int id = -1;
        for (int node = 0; node < node_count; node++) {
            if (node == small_part) {continue;}
            std::pair<int, int> best = *wrongs_sets[node].begin();
            if (best.first <= 0) {continue;}
            if (id == -1) {
                id = best.second;
                continue;
            }
            if (best.first > (2*wrongs[id] - gg[id].size())) {
                id = best.second;
            }
        }
        if (id == -1) {
            std::cout << ":(" << std::endl;
            break;
        }

        wrongs_sets[parts[id]].erase(wrongs_sets[parts[id]].begin());

        for (int i = 0; i < int(gg[id].size()); i++) {
            wrongs_sets[parts[gg[id][i]]].erase(
                    wrongs_sets[parts[gg[id][i]]].find(
                            std::make_pair(2*wrongs[gg[id][i]] - gg[gg[id][i]].size(), gg[id][i])
                            )
            );
            if (parts[id] != parts[gg[id][i]]) {
                wrongs[gg[id][i]]--;
                cnt--;
            } else {
                wrongs[gg[id][i]]++;
                cnt++;
            }
            wrongs_sets[parts[gg[id][i]]].insert(std::make_pair(2*wrongs[gg[id][i]] - gg[gg[id][i]].size(), gg[id][i]));
        }
        wrongs[id] = gg[id].size() - wrongs[id];
        parts_sizes[parts[id]]--;
        parts[id] = 1 - parts[id];
        parts_sizes[parts[id]]++;
        wrongs_sets[parts[id]].insert(std::make_pair(2 * wrongs[id] - gg[id].size(), id));
        // std::cout << "with E = " << E << " cnt is " << cnt << std::endl;
        if (E / cnt >= 6) {
            break;
        }
        if (parts_sizes[0] + parts_sizes[0] / 10 < parts_sizes[1]) {
            small_part = 0;
        } else {
            if (parts_sizes[1] + parts_sizes[1] / 10 < parts_sizes[0]) {
                small_part = 1;
            } else {
                small_part = -1;
            }
        }
    }

    int cnt_check = 0;
    for (int i = 0; i < E; i++) {
        int u = g->at(i).u;
        int v = g->at(i).v;
        if ((parts[v]) != (parts[u])) {
            cnt_check++;
        }
    }
    std::cout << "with E = " << E << " cnt_check is " << cnt_check << std::endl;
    std::cout << "sizes: left=" << parts_sizes[0] << " right=" << parts_sizes[1] << std::endl;
}

void make_parts_with_intersection20(std::vector<Edge>* g) {
    if (node_count == 2) {
        make_parts_with_intersection_on2nodes(g);
    } else {
        make_parts_with_intersection_on4nodes(g);
    }
}

void benchmark(const std::string& graph_filename) {
    std::string outfile = "new2 _" + getLastPartOfFilename(graph_filename);
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

    std::cerr << "graph read\n";

    make_parts_with_intersection20(g);

    return;

    std::vector<int> owners(N);
    for (int i = 0; i < N; i++) {
        owners[i] = (1 << parts[i]);
    }
    std::vector<std::vector<Edge>> edges(node_count);
    for (int i = 0; i < E; i++) {
        auto e = g->at(i);
        if (parts[e.u] == parts[e.v]) {
            edges[parts[e.u]].emplace_back(e);
        } else {
            if (rand() % 2) {
                edges[parts[e.u]].emplace_back(e);
            } else {
                edges[parts[e.v]].emplace_back(e);
            }
        }
        //int r = rand() % 2;
        //edges[r].emplace_back(e);
    }

    std::ofstream out;
    out.open(outfile);

    std::vector<DSU*> dsus;
    dsus.push_back(new DSU_Usual(N));
    dsus.push_back(new DSU_ParallelUnions(N, node_count));
    dsus.push_back(new DSU_Parts(N, node_count, owners));
    dsus.push_back(new DSU_NO_SYNC(N, node_count));
    dsus.push_back(new DSU_NoSync_Parts(N, node_count, owners));
    std::cerr << "DSUS done\n";
    dsus.push_back(new DSU_FC(N, node_count));
    std::cerr << "fc done\n";

    int edges_to_pre_unite = E / 10 * 4;
    int edges_to_test = E - edges_to_pre_unite;
    int pre_unite_percent = 40;
    for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
        RATIO = i;
        std::cerr << i << std::endl;
        auto edges_to_pre_unite_on_step = edges_to_pre_unite;// / 100 * RATIO;

        auto ctx = new ContextRatio(&edges, dsus[0], RATIO);

        for (int j = 0; j < dsus.size(); j++) {
            ctx->dsu = dsus[j];
            auto res = getAverageTime(ctx, pre_unite_percent);
            out << dsus[j]->ClassName() << " " << RATIO << " " << res << "\n";
        }
    }

    out.close();

    return;
/////////////////////////////////////////////////

    for (int pu = 0; pu < 100; pu += 20) {
        std::string new_outfile = outfile + "_" + std::to_string(pu);
        out.open(new_outfile);

        for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
            RATIO = i;
            std::cerr << i << std::endl;

            auto ctx = new ContextRatio(&edges, dsus[0], RATIO);

            for (int j = 0; j < dsus.size(); j++) {
                ctx->dsu = dsus[j];
                auto res = getAverageTime(ctx, pu);
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

        std::vector<int> owners(N);
        for (int i = 0; i < N; i++) {
            if (i % 2 == 0) {
                owners[i] = 2;
            } else {
                owners[i] = 1;
            }
        }

        std::vector<std::vector<Edge>> edges(node_count);
        for (int i = 0; i < E; i++) {
            auto e = g->at(i);
            if (parts[e.u] == parts[e.v]) {
                edges[parts[e.u]].emplace_back(e);
            } else {
                if (rand() % 2) {
                    edges[parts[e.u]].emplace_back(e);
                } else {
                    edges[parts[e.v]].emplace_back(e);
                }
            }
        }

        std::ofstream out;
        out.open(outfile + "_" + std::to_string(components_number));

        std::vector<DSU*> dsus;
        dsus.push_back(new DSU_Usual(N));
        dsus.push_back(new TwoDSU(N, node_count));
        dsus.push_back(new DSU_ParallelUnions(N, node_count));
        dsus.push_back(new DSU_NO_SYNC(N, node_count));
        dsus.push_back(new DSU_Parts(N, node_count, owners));
        dsus.push_back(new DSU_NoSync_Parts(N, node_count, owners));

        int edges_to_pre_unite = 0;//E / 10 * 4;
        int edges_to_test = E - edges_to_pre_unite;
        int pre_unite_percent = 0;
        for (int i = FIRST_RATIO; i <= LAST_RATIO; i += RATIO_STEP) {
            RATIO = i;
            std::cerr << i << std::endl;
            auto edges_to_pre_unite_on_step = edges_to_pre_unite;// / 100 * RATIO;

            auto ctx = new ContextRatio(&edges, dsus[0], RATIO);

            for (int j = 0; j < dsus.size(); j++) {
                ctx->dsu = dsus[j];
                for (int r = 0; r < RUNS; r++) {
                    auto res = getAverageTime(ctx, pre_unite_percent);
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