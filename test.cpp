#include <iostream>
#include <algorithm>

#include "DSU.h"
#include "implementations/DSU_Queue.h"

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


int main() {
    testDSU();

    return 0;
}
