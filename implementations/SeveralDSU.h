#ifndef TRY_SEVERAL_DSU_H
#define TRY_SEVERAL_DSU_H

#include "../DSU.h"

class SeveralDSU : public DSU {
public:
    std::string ClassName() override {
        return "SeveralDSU";
    };

    SeveralDSU(NUMAContext* ctx, int size)
        : DSU(ctx)
        , size(size), node_count(ctx->NodeCount()) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) Ctx_->Allocate(i, sizeof(std::atomic<int>) * (size / node_count + 1));
            for (int j = 0; j < (size / node_count + 1); j++) {
                data[i][j].store(j);
            }
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < (size / node_count + 1); j++) {
                data[i][j].store(j);
            }
        }
    }

    ~SeveralDSU() override {
        for (int i = 0; i < node_count; i++) {
            Ctx_->Free(data[i], sizeof(std::atomic<int>) * (size / node_count + 1));
        }
    }

    void Union(int u, int v) override {
        auto node = u % node_count;//u & 1; // TODO WTF???
        //u = (u >> 1); v = (v >> 1);
        u = u / node_count; v = v / node_count;
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        auto u_p = find(u, node);
        auto v_p = find(v, node);
        if (u_p == v_p) {return;}

        union_(u_p, v_p, node);
    }

    bool SameSet(int u, int v) override {
        int node = u % node_count;//u & 1;
        //u = (u >> 1); v = (v >> 1);
        u = u / node_count; v = v / node_count;
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
        return find(u, u % node_count);
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

#endif
