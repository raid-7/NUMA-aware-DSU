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
            //data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            data[i] = (int*) numa_alloc_onnode(sizeof(int) * size, i);
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
        //std::string output = std::to_string(sched_getcpu()) + " in union \n";
        //std::cerr << output;
        m.lock();
        auto cpu = sched_getcpu();
        auto node = numa_node_of_cpu(cpu);

        //std::cerr << sched_getcpu() << " " << "got cpu and node \n";

        for (int i = 0; i < node_count; i++) {
            if (i == node)
                continue;
            //std::cerr << sched_getcpu() << " " << "want to push in queue \n";
            queues[i]->Push(std::make_pair(u, v));
        }

        auto u_p = find(u, node);
        auto v_p = find(v, node);
        //std::cerr << sched_getcpu() << " " << "found parents \n";
        if (u_p == v_p) {
            m.unlock();
            return;
        }

        //std::cerr << sched_getcpu() << " " << "found parents \n";

        union_(u_p, v_p, node);
        m.unlock();
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
        m.lock();
        auto res = (find(u, node) == find(v, node));
        m.unlock();
        return res;
    }

private:
    int find(int u, int node) {
        //std::cerr << sched_getcpu() << " " << "in find_ \n";
        // old unions
        auto unions = queues[node]->List();
        //std::cerr << "unions got \n";
        for (auto u : unions) {
            union_(u.first, u.second, node);
        }
        //std::cerr << sched_getcpu() << " " << "old unions done \n";

        return parent(u, node);
    }

    int parent(int u, int node) {
        auto par = data[node][u];//.load();
        while (par != data[node][par]) {

            par = data[node][par];
        }

        //std::cerr << sched_getcpu() << " " << "parent found \n";
        return par;
    }

    void union_(int u, int v, int node) {
        int u_p = parent(u, node);
        int v_p = parent(v, node);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[node][u_p] = v_p;
            //while (!data[node][u].compare_exchange_weak(u, v)) {}
        } else {
            //while (!data[node][v].compare_exchange_weak(v, u)) {}
            data[node][v_p] = u_p;
        }
    }

private:
    int size;
    int node_count;
    //std::vector<std::atomic<int>*> data;
    std::vector<int*> data;
    std::vector<Queue*> queues;

    std::mutex m;
};


#endif //TRY_DSU_H
