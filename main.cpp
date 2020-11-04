#include <iostream>
#include <numa.h>
#include <vector>
#include <sched.h>
#include <thread>
#include <algorithm>

#include "DSU.h"

const int N = 50;
const int THREADS = 100;
int node_count;

void go(DSU* dsu) {
    for (int i = 0; i < 10; i++) {
        int a = rand() % N;
        int b = rand() % N;
        std::cerr << "Go union " << a << " " << b << "\n";
        //std::cout << sched_getcpu() << " " <<  dsu->Find(i) << std::endl;
        dsu->Union(a, b);
        std::cerr << sched_getcpu() << " " << "Union done \n";
    }

    std::cerr << sched_getcpu() << " " << "thread done\n";
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
//    auto got = q->List();
//    for (int i = 0; i < int(got.size()); i++) {
//        result->emplace_back(got[i].first);
//    }
}

void testQueue() {
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

    for (int i = 0; i < int(results->size()); i++) {
        for (int & j : results[i]) {
            result.emplace_back(j);
        }
    }

    std::sort(result.begin(), result.end());
    for (int i = 0; i < result.size(); i++) {
        std::cout << result[i] << " ";
    }
    std::cout << "\n";

    for (int i = 0; i < THREADS; i++) {
        for (int j = 0; j < 5; j++) {
            if (result[i * 5 + j] != i) {
                std::cout << ":(";
                return;
            }
        }
    }

    std::cout << "OK";
}

int main() {
    testQueue();
    //testDSU();

    return 0;
}
