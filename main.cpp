#include <iostream>
#include <numa.h>
#include <vector>
#include <thread>
#include <algorithm>
#include <chrono>

#include "DSU.h"

const int N = 1000;
const int THREADS = 100;

int node_count;

void go(DSU* dsu) {
    for (int i = 0; i < 10; i++) {
        int a = rand() % N;
        int b = rand() % N;
        //std::string output = std::to_string(sched_getcpu()) + " Go union " + std::to_string(a) + " " + std::to_string(b) + "\n";
        //std::cerr << output;
        dsu->Union(a, b);

        //output = std::to_string(sched_getcpu()) + " Union done \n";
        //std::cerr << output;
    }

    //std::string output = std::to_string(sched_getcpu()) + " thread done\n";
    //std::cerr << output;
}


void testDSU() {
    node_count = numa_num_configured_nodes();
    auto dsu = new DSU(N, node_count);

    std::cerr << "dsu inited \n";

    std::vector<std::thread> threads(THREADS);

    for (int i = 0; i < THREADS; i++) {
        threads[i] = std::thread(go, dsu);
    }

    for (int i = 0; i < THREADS; i++) {
        threads[i].join();
    }

    std::cerr << "threads done \n";

    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            auto ans = dsu->__SameSetOnNode(i, j, 0);
            for (int nd = 1; nd < node_count; nd++) {
                if (dsu->__SameSetOnNode(i, j, nd) != ans) {
                    std::cout << ":(";
                    return;
                }
            }
        }
    }
    std::cout << "OK\n";
}

void push(Queue* q, int t) {
    for (int i = 0; i < 5; i++) {
        q->Push(std::make_pair(t, t));
    }
}

void pop(Queue* q, int t, std::vector<int>* result) {
    for (int i = 0; i < 5; i++) {
        q->Push(std::make_pair(t, t));
    }
    auto got = q->List();
    for (int i = 0; i < int(got.size()); i++) {
        result->emplace_back(got[i].first);
    }
}

bool testQueue() {
    auto q = new Queue();
    q->Init(0);
    std::vector<std::thread> threads(THREADS);
    std::vector<int> results[THREADS / 5];

    for (int i = 0; i < THREADS; i++) {
        if (i % 5 == 0) {
            threads[i] = std::thread(pop, q, i, &results[i / 5]);
        } else {
            threads[i] = std::thread(push, q, i);
        }
    }

    for (int i = 0; i < THREADS; i++) {
        threads[i].join();
    }

    std::vector<int> result;
    auto got = q->List();
    for (auto & i : got) {
        result.emplace_back(i.first);
    }

    for (int i = 0; i < THREADS / 5; i++) {
        for (int & j : results[i]) {
            result.emplace_back(j);
        }
    }

    std::sort(result.begin(), result.end());
    for (int i = 0; i < int(result.size()); i++) {
        std::cout << result[i] << " ";
    }
    std::cout << "\n";

    for (int i = 0; i < THREADS; i++) {
        for (int j = 0; j < 5; j++) {
            if (result[i * 5 + j] != i) {
                std::cout << ":(";
                return false;
            }
        }
    }

    std::cout << "OK";
    return true;
}

void test() {
    testQueue();
    testDSU();
}



void thread_routine(std::vector<int> a, int v, DSU* dsu) {
    for (int i = 0; i < int(a.size()); i++) {
        if (a[i] == 1) {
            dsu->Union(i, v);
        }

        for (int j = 0; j < 1000; j++) {
            auto x = dsu->Find(v);
            if (x == v && j > 500) {
                break;
            }
        }
    }
}

void benchmark() {
    int n = 1000;
    std::vector<int> a[n];
    for (int i = 0; i < n; i++) {
        a[i].resize(n);
        for (int j = 0; j < n; j++) {
            a[i][j] = 0;
        }
    }
    for (int i = 0; i < 1000000; i++) {
        int x = rand() % 1000;
        int y = rand() % 1000;
        a[x][y] = 1;
        a[y][x] = 1;
    }

    node_count = numa_num_configured_nodes();
    auto dsu = new DSU(N, node_count);
    std::vector<std::thread> threads(n/10);

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < n; i+=10) {
        threads[i / 10] = std::thread(thread_routine, a[i], i, dsu);
    }

    for (int i = 0; i < n; i+=10) {
        threads[i / 10].join();
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    std::cout << duration.count() << std::endl;
}

int main() {
    //test();

    benchmark();

    return 0;
}
