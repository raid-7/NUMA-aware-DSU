//
// Created by Mariia Chukhareva on 4/13/21.
//

#ifndef TRY_DSU_PARALLELUNIONS_H
#define TRY_DSU_PARALLELUNIONS_H

#include "../DSU.h"

class DSU_ParallelUnions : public DSU {
public:
    DSU_ParallelUnions(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        unions.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        unions.store(0);
    }

    ~DSU_ParallelUnions() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = getNode();
        unions.fetch_add(1);
        for (int i = 0; i < node_count; i++) {
            union_(u, v, i, (i == node), node);
        }
        unions.fetch_add(-1);
    }

    bool SameSet(int u, int v) override {
        return SameSetOnNode(u, v, getNode());
    }

    int Find(int u) override {
        while (unions.load() > 0) {
        }
        return find(u, getNode(), true);
    }

    bool SameSetOnNode(int u, int v, int node) {
        while (unions.load() > 0) {
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node, true);
            v_p = find(v_p, node, true);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

private:
    int find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                auto grand = data[node][par].load(std::memory_order_relaxed);
                if (par == grand) {
                    return par;
                } else {
                    data[node][cur].compare_exchange_weak(par, grand);
                }
                cur = par;
            }
        } else {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local, int cur_node) {
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        int u_p = u;
        int v_p = v;
        if (!is_local) {
            u_p = find(u_p, cur_node, true);
            v_p = find(v_p, cur_node, true);
            if (u_p < v_p) {
                if (u_p < v_p) {
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
        while (true) {
            u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }
            if (u_p < v_p) {
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

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::atomic<int> unions;
};

#endif //TRY_DSU_PARALLELUNIONS_H
