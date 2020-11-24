#include <iostream>
#include <algorithm>

#include "DSU.h"
#include "Queue.h"

const int N = 1000;
const int THREADS = 100;

int node_count;

void go(DSU_Queue* dsu) {
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
    auto dsu = new DSU_Queue(N, node_count);

    // std::cerr << "dsu inited \n";

    std::vector<std::thread> threads(THREADS);

    for (int i = 0; i < THREADS; i++) {
        threads[i] = std::thread(go, dsu);
    }

    for (int i = 0; i < THREADS; i++) {
        threads[i].join();
    }

    //std::cerr << "threads done \n";

    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            auto ans = dsu->SameSetOnNode(i, j, 0);
            for (int nd = 1; nd < node_count; nd++) {
                if (dsu->SameSetOnNode(i, j, nd) != ans) {
                    std::cout << "DSU :(\n";
                    return;
                }
            }
        }
    }
    std::cout << "DSU OK\n";
}

void push(MSQueue* q, int t) {
    for (int i = 0; i < 5; i++) {
        q->Push(std::make_pair(t, t));
    }
}
void pop(MSQueue* q, int t, std::vector<int>* result) {
    for (int i = 0; i < 5; i++) {
        q->Push(std::make_pair(t, t));
    }

    while(true) {
        auto p = q->Pop();
        if (p == nullptr) {
            break;
        }
        result->emplace_back(p->first);
    }
}

bool testQueue() {
    auto q = new MSQueue();
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
    while (true) {
        auto p = q->Pop();
        if (p == nullptr) {
            break;
        }
        result.emplace_back(p->first);
    }

    for (int i = 0; i < THREADS / 5; i++) {
        for (int & j : results[i]) {
            result.emplace_back(j);
        }
    }

    std::sort(result.begin(), result.end());

    for (int i = 0; i < THREADS; i++) {
        for (int j = 0; j < 5; j++) {
            if (result[i * 5 + j] != i) {
                std::cout << "Queue :(\n";
                return false;
            }
        }
    }

    std::cout << "Queue OK\n";
    return true;
}

int main() {
    testQueue();
    testDSU();

    return 0;
}