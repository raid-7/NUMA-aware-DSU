#ifndef TRY_DSU_PARALLELUNIONS_NOIMM_H
#define TRY_DSU_PARALLELUNIONS_NOIMM_H


#include "../DSU.h"

class DSU_ParallelUnions_NoImm : public DSU {
public:
    std::string ClassName() override {
        return "ParallelUnions_NoImm";
    };

    DSU_ParallelUnions_NoImm(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        to_union = std::vector<std::atomic<int>>(size);;
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i].store(1);
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i].store(1);
        }
    }

    ~DSU_ParallelUnions_NoImm() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void DoUnion(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u, node, true);
            v_p = find(v, node, true);
            if (u_p == v_p) {
                return;
            }
            if (u_p < v_p) {
                std::swap(u_p, v_p);
            }
            auto u_data = to_union[u_p].load(std::memory_order_acquire);
            if (u_data % 2 == 0) {
                // union_(u_p, v_p, node, true);
                // __builtin_ia32_pause()
                continue;
            } else {
                if (to_union[u_p].compare_exchange_strong(u_data, v_p * 2)) {
                    for (int i = 0; i < node_count; i++) {
                        union_(u_p, v_p, i, (node == i));
                    }
                    u_data = v_p * 2;
                    if (to_union[u_p].compare_exchange_strong(u_data, v_p * 2 + 1)) {
                        for (int i = 0; i < node_count; i++) {
                            auto par = data[i][u_p].load(std::memory_order_acquire);
                            data[i][u_p].compare_exchange_strong(par, par | 1);
                        }
                    }
                    break;
                }
            }
        }
    }

    bool DoSameSet(int u, int v) override {
        int node = NUMAContext::CurrentThreadNode();
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node, true);
            v_p = find(v_p, node, true);
            if (u_p == v_p) {
                return true;
            }
            if (getParent(node, u_p) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return find(u, NUMAContext::CurrentThreadNode(), true);
    }

private:

    int find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
                auto grand = getParent(node, par);
                if (par == grand) {
                    return par;
                } else {
                    auto par_data = (par << 1) | 1;
                    data[node][cur].compare_exchange_weak(par_data, ((grand << 1) | 1));
                }
                cur = par;
            }
        } else {
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        auto u_p = u;
        auto v_p = v;
        auto u_p_data = u_p * 2 + 1;
        while (true) {
            //u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }

            //auto u_p_data = u_p*2 + 1;
            if (data[node][u_p].compare_exchange_weak(u_p_data, v_p * 2)) {
                return;
            }
        }
    }

    int getParent(int node, int u) {
        auto par = data[node][u].load(std::memory_order_acquire);

        if ((par & 1) == 1) {
            return (par >> 1);
        } else {
            par = par >> 1;
            auto data = to_union[u].load(std::memory_order_acquire);
            if (par == (data >> 1)) {
                if ((data & 1) == 1) {
                    return par;
                } else {
                    return u;
                }
            } else {
                return par;
            }
        }
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<std::atomic<int>> to_union;
};


#endif //TRY_DSU_PARALLELUNIONS_NOIMM_H
