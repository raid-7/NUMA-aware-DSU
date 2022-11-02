#include "../DSU.h"

class DSU_NO_SYNC_NoImm : public DSU {
public:
    std::string ClassName() {
        return "NoSync_NoImm";
    };

    long long getStepsCount() {
        return steps_count.load();
    }

    void setStepsCount(int x) {
        steps_count.store(x);
    }

    DSU_NO_SYNC_NoImm(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        steps_count.store(0);
    }

    ~DSU_NO_SYNC_NoImm() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        auto u_p = find(u, node, true);
        auto v_p = find(v, node, true);
        if (u_p == v_p) {
            return;
        }
        //union_(u, v, node, true);
        for (int i = 0; i < node_count; i++) {
            union_(u_p, v_p, i, (i == node));
        }
    }

    bool SameSet(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
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

    int Find(int u) override {
        return find(u, NUMAContext::CurrentThreadNode(), true);
    }

    bool SameSetOnNode(int u, int v, int node) {
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
                auto par = data[node][cur].load(std::memory_order_acquire);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;
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

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::atomic<long long> steps_count;
};