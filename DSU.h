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
                data[i][j].store(j);
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
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        for (int i = 0; i < node_count; i++) {
            if (i == node) {
                continue;
            }
            queues[i]->Push(std::make_pair(u, v));
        }

        union_(u, v, node);
    }

    bool SameSet(int u, int v) {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        return SameSetOnNode(u, v, node);
    }

    int Find(int u) {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count > 1) {
            old_unions(node);
        } else {
            node = 0;
        }

        return find(u, node);
    }

    bool SameSetOnNode(int u, int v, int node) {
        if (node_count > 1) {
            old_unions(node);
        }

        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node);
            v_p = find(v_p, node);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load() == u_p) {
                return false;
            }
        }
    }
private:
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
        auto cur = u;
        while (true) {
            auto par = data[node][cur].load();
            auto grand = data[node][par].load();
            if (par == grand) {
                return par;
            } else {
                data[node][cur].compare_exchange_weak(par, grand);
            }
            cur = par;
        }

    }

    void union_(int u, int v, int node) {
        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = find(u_p, node);
            v_p = find(v_p, node);
            if (u_p == v_p) {
                return;
            }
            if (rand() % 2) {
                if (data[node][u_p].compare_exchange_weak(u_p, v_p)) {
                    return;
                }
            } else {
                if (data[node][v_p].compare_exchange_weak(v_p, u_p)) {
                    return;
                }
            }

        }
    }

private:
    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<Queue*> queues;
};


#endif //TRY_DSU_H
