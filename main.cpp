#include <iostream>
#include <numa.h>
#include <vector>
#include <sched.h>
#include <thread>

#include "DSU.h"

const int N = 1000;
int node_count;

void go(DSU* dsu) {
    for (int i = 0; i < 10; i++) {
        int a = rand() % N;
        int b = rand() % N;
        //std::cout << sched_getcpu() << " " <<  dsu->Find(i) << std::endl;
        dsu->Union(a, b);
    }
}

bool check(DSU* dsu) {
    for (int i = 0; i < N; i++) {
        for (int j = 0; j < N; j++) {
            auto ans = dsu->__SameSetOnNode(i, j, 0);
            for (int nd = 1; nd < node_count; nd++) {
                if (dsu->__SameSetOnNode(i, j, nd) != ans) {
                    return false;
                }
            }
        }
    }
    return true;
}

void test() {
    node_count = numa_num_configured_nodes();
    auto dsu = new DSU(N, node_count);

    std::vector<std::thread> threads(100);

    for (int i = 0; i < 100; i++) {
        threads[i] = std::thread(go, dsu);
    }

    for (int i = 0; i < 100; i++) {
        threads[i].join();
    }

    if (check(dsu)) {
        std::cout << "OK\n";
    } else {
        std::cout << ":(";
    }
}

int main() {
    test();

    return 0;
}
