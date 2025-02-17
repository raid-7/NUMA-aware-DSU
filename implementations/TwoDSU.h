#ifndef TRY_TWODSU_H
#define TRY_TWODSU_H

#include "../DSU.h"

class TwoDSU : public DSU {
public:
    std::string ClassName() {
        return "TwoDSU";
    };

    TwoDSU(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * (size / 2 + 1), i);
            for (int j = 0; j < (size / 2 + 1); j++) {
                data[i][j].store(j);
            }
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < (size / 2 + 1); j++) {
                data[i][j].store(j);
            }
        }
    }

    ~TwoDSU() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * (size / 2 + 1));
        }
    }

    void DoUnion(int u, int v) override {
        auto node = u & 1;
        u = (u >> 1); v = (v >> 1);
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        auto u_p = find(u, node);
        auto v_p = find(v, node);
        if (u_p == v_p) {return;}

        union_(u_p, v_p, node);
    }

    bool DoSameSet(int u, int v) override {
        int node = u & 1;
        u = (u >> 1); v = (v >> 1);
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return true;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node);
            v_p = find(v_p, node);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return find(u, u & 1);
    }


    int find(int u, int node) {
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

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
};

#endif //TRY_TWODSU_H
