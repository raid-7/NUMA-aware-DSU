#include <iostream>
#include <thread>

#include "DSU.h"

int N = 1000;
int THREADS = 100;

std::vector< std::vector<int>> a;

void thread_routine(std::vector<int> a, int v, DSU* dsu) {
    for (int i = 0; i < int(a.size()); i++) {
        if (a[i] == 1) {
            dsu->Union(i, v);
        }

        for (int j = 0; j < 1000; j++) {
            a[i] = j;
            if (rand() % 100 == 0) {
                break;
            }
        }
    }
}

void run(DSU *dsu) {
    std::vector<std::thread> threads;

    for (int i = 0; i < N; i += (N / THREADS)) {
        threads.emplace_back(std::thread(thread_routine, a[i], i, dsu));
    }

    for (int i = 0; i < int(threads.size()); i++) {
        threads[i].join();
    }
}

float runWithTime(DSU* dsu) {
    auto start = std::chrono::high_resolution_clock::now();
    run(dsu);
    auto stop = std::chrono::high_resolution_clock::now();
    auto durationNUMA = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    return float(durationNUMA.count()) / 1000;
}

void benchmark() {
    a.resize(N);

    for (int i = 0; i < N; i++) {
        a[i].resize(N);
        for (int j = 0; j < N; j++) {
            a[i][j] = 0;
        }
    }
    for (int i = 0; i < 1000000; i++) {
        int x = rand() % N;
        int y = rand() % N;
        a[x][y] = 1;
        a[y][x] = 1;
    }

    int node_count = numa_num_configured_nodes();
    auto dsuNUMA = new DSU(N, node_count);
    auto dsuUsual = new DSU(N, 1);

    std::cout << runWithTime(dsuNUMA) << std::endl;
    std::cout << runWithTime(dsuUsual) << std::endl;


}

int main(int argc, char* argv[]) {
    if (argc > 1) {
        auto nStr = argv[1];
        auto threadsStr = argv[2];

        N = std::stoi(nStr);
        THREADS = std::stoi(threadsStr);
    }

    benchmark();

    return 0;
}