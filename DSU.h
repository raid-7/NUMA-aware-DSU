#ifndef TRY_DSU_H
#define TRY_DSU_H

#include <sched.h>
#include <thread>
#include <numa.h>
#include <atomic>
#include <vector>

#include "Queue.h"

class DSU{
public:
    DSU(int size, int node_count) : size(size), node_count(node_count) {
        data.resize(node_count);
        queues.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j] = j;
            }

            queues[i] = (Queue*) numa_alloc_onnode(sizeof(Queue), i);
            queues[i]->Init(i);
        }
    }

    ~DSU() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            numa_free(queues[i], sizeof(Queue));
        }
    }

    void Union(int u, int v) {
        auto cpu = sched_getcpu();
        auto node = numa_node_of_cpu(cpu);

        auto u_p = find(u, node);
        auto v_p = find(v, node);
        if (u_p == v_p)
            return;

        for (int i = 0; i < node_count; i++) {
            if (i == node)
                continue;
            queues[i]->Push(std::make_pair(u_p, v_p));
        }

        union_(u_p, v_p, node);
    }

    bool SameSet(int u, int v) {
        auto cpu = sched_getcpu();
        auto node = numa_node_of_cpu(cpu);
        return find(u, node) == find(v, node);
    }

    int Find(int u) {
        auto cpu = sched_getcpu();
        auto node = numa_node_of_cpu(cpu);
        return find(u, node);
    }

    bool __SameSetOnNode(int u, int v, int node) {
        return find(u, node) == find(v, node);
    }

private:
    int find(int u, int node) {

        // old unions
        auto unions = queues[node]->List();
        for (auto u : unions) {
            union_(u.first, u.second, node);
        }

        auto par = data[node][u].load();
        while (par != data[node][par]) {
            par = data[node][par];
        }
        return par;
    }

    void union_(int u, int v, int node) {
        if (rand() % 2) {
            while (!data[node][u].compare_exchange_weak(u, v)) {}
        } else {
            while (!data[node][v].compare_exchange_weak(v, u)) {}
        }
    }

private:
    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;

    std::vector<Queue*> queues;
};


#endif //TRY_DSU_H
