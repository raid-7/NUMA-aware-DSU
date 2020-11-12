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
        mutexes.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }

            queues[i] = (Queue*) numa_alloc_onnode(sizeof(Queue), i);
            queues[i]->Init(i);

            mutexes[i] = (std::mutex*) numa_alloc_onnode(sizeof(std::mutex), i);
        }
    }

    ~DSU() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            numa_free(queues[i], sizeof(Queue));
            numa_free(mutexes[i], sizeof(std::mutex));
        }
    }

    void Union(int u, int v) {
        //std::string output = std::to_string(sched_getcpu()) + " in union \n";
        //std::cerr << output;

        auto cpu = sched_getcpu();
        auto node = numa_node_of_cpu(cpu);
        if (node_count == 1) {
            node = 0;
        }
        //if (node_count > 1) {
            for (int i = 0; i < node_count; i++) {
                if (i == node)
                    continue;
                queues[i]->Push(std::make_pair(u, v));
            }
        //}
        mutexes[node]->lock();
        union_(u, v, node);
        mutexes[node]->unlock();
    }

    bool SameSet(int u, int v) {
        if (node_count > 1) {
            auto cpu = sched_getcpu();
            auto node = numa_node_of_cpu(cpu);
            mutexes[node]->lock();
            old_unions(node);
            auto u_p = find(u, node);
            auto v_p = find(v, node);
            mutexes[node]->unlock();
            return u_p == v_p;
        } else {
            mutexes[0]->lock();
            auto u_p = find(u, 0);
            auto v_p = find(v, 0);
            mutexes[0]->unlock();
            return u_p == v_p;
        }
    }

//    int Find(int u) {
//        if (node_count > 1) {
//            auto cpu = sched_getcpu();
//            auto node = numa_node_of_cpu(cpu);
//            mutexes[node]->lock();
//            old_unions(node);
//            mutexes[node]->unlock();
//            return find(u, node);
//        } else {
//            return find(u, 0);
//        }
//    }

    bool __SameSetOnNode(int u, int v, int node) {
        mutexes[node]->lock();
        old_unions(node);
        auto res = (find(u, node) == find(v, node));
        mutexes[node]->unlock();
        return res;
    }

private:
    // перед всеми приватными операциями должен быть взят лок
    void old_unions(int node) {
        while (true) {
            auto p = queues[node]->Pop();
            if (p == nullptr) {
                break;
            }
            union_(p->first, p->second, node);
            numa_free(p, sizeof(std::pair<int, int>));
        }
    }

    int find(int u, int node) {
        auto par = data[node][u].load();
        while (par != data[node][par].load()) {
            par = data[node][par].load();
        }
        auto res = par;

        par = data[node][u].load();
        while (par != data[node][par].load()) {
            auto next = data[node][par].load();
            data[node][par].store(res);
            par = next;
        }

        return res;
    }

    void union_(int u, int v, int node) {
        int u_p = find(u, node);
        int v_p = find(v, node);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[node][u_p].store(v_p);
        } else {
            data[node][v_p].store(u_p);
        }
    }

private:
    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<Queue*> queues;

    std::vector<std::mutex*> mutexes;
};


#endif //TRY_DSU_H
