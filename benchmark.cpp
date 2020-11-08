#include <iostream>
#include <thread>

#include "DSU.h"

//const int N = 1000;
const int THREADS = 100;

int node_count;

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
    std::vector< std::vector<int>> a;
    a.resize(n);

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
    auto dsu = new DSU(n, node_count);
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
    benchmark();

    return 0;
}