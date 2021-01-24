#include <iostream>
#include <atomic>
#include <thread>
#include <vector>

#include "numa.h"


const int N = 1e9;
const int THREADS = 10;

void thread_routine(std::atomic_int* counter) {
    for (int i = 0; i < N / THREADS; i++) {
        counter->fetch_add(1);
    }
}

//here we believe that configured nodes numbered with 0 to n (number of them)
void test(int node_count) {
    // mask for available nodes
    auto getMemBindMask = numa_get_membind();

    for (int mem_node = 0; mem_node < node_count; mem_node++) {
        // mem_node -- the node to allocate the counter
        std::cout << "Test memory on node " << mem_node << ": " << std::endl;
        for (int run_node = 0; run_node < node_count; run_node++) {
            //run_node -- the node for running
            std::atomic_int* counter = (std::atomic_int*) numa_alloc_onnode(sizeof(std::atomic_int), mem_node);
            int* times = (int *) numa_alloc_onnode(sizeof(int) * THREADS, mem_node);

            bitmask* bindMaskToRun = numa_bitmask_alloc(getMemBindMask->size);
            numa_bitmask_setbit(bindMaskToRun, run_node);
            numa_bind(bindMaskToRun);

            int runs = 3;
            int sum_time = 0;
            for (int r = 0; r < runs; r++) {
                std::vector<std::thread> threads;
                auto start = std::chrono::high_resolution_clock::now();
                for (int i = 0; i < THREADS; i++) {
                    threads.emplace_back(std::thread(thread_routine, counter));
                }
                for (int i = 0; i < THREADS; i++) {
                    threads[i].join();
                }
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
                sum_time += duration.count();

                counter->store(0);
            }
            std::cout << "Time for run on node " << run_node << ": " << float(sum_time) / runs << " milliseconds" << std::endl;

            numa_free(counter, sizeof(std::atomic_int));
            numa_free(times, sizeof(int) * THREADS);
        }

        numa_bind(getMemBindMask);
    }
}

int main() {
    if (numa_available() == -1) {
        std::cout << "NO NUMA HERE" << std::endl;
        return 0;
    } else {
        test(numa_num_configured_nodes());
    }
}
