#include <iostream>
#include <numa.h>
#include <vector>
#include <sched.h>
#include <thread>

#include "DSU.h"

const int N = 50;
const int THREADS = 10;
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

    std::cerr << "dsu inited \n";

    std::vector<std::thread> threads(THREADS);

    for (int i = 0; i < THREADS; i++) {
        threads[i] = std::thread(go, dsu);
    }

    for (int i = 0; i < THREADS; i++) {
        threads[i].join();
    }

    std::cerr << "threads done \n";

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
