#include <iostream>
#include <numa.h>
#include <vector>
#include <sched.h>
#include <thread>

#include "DSU.h"


void go(DSU* dsu) {
    for (int i = 0; i < 10; i++) {
        std::cout << sched_getcpu() << " " <<  dsu->Find(i) << std::endl;
    }
}

int main() {
    int node_count = numa_num_configured_nodes();
    auto dsu = new DSU(10, node_count);

    std::vector<std::thread> threads(100);

    for (int i = 0; i < 100; i++) {
        threads[i] = std::thread(go, dsu);
    }

    for (int i = 0; i < 100; i++) {
        threads[i].join();
    }

    std::cout << sched_getcpu() << std::endl;
    return 0;
}
