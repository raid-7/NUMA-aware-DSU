#ifndef TRY_DSU_NOSYNC_PARTS_H
#define TRY_DSU_NOSYNC_PARTS_H

#include "../DSU.h"

class DSU_NoSync_Parts : public DSU {
public:
    std::string ClassName() {
        return "NoSync_Parts";
    };

    long long getStepsCount() {
        return steps_count.load();
    }

    void setStepsCount(int x) {
        steps_count.store(x);
    }

    DSU_NoSync_Parts(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j << 1);
                if (j % 2 == i) {
                    data[i][j].store(j << 1);
                } else {
                    data[i][j].store((j << 1) + 1);
                }
            }
        }
        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                //data[i][j].store(j << 1);
                if (j % 2 == i) {
                    data[i][j].store(j << 1);
                } else {
                    data[i][j].store((j << 1) + 1);
                }
            }
        }
        steps_count.store(0);
    }

    ~DSU_NoSync_Parts() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * size);
        }
    }

    void Union(int u, int v) override {
//std::cerr << " in union ";
        auto node = getNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 1) == (data[node][v].load(std::memory_order_relaxed) >> 1)) {
            return;
        }
        auto u_p = find(u, node, true);
        auto v_p = find(v, node, true);
        if (u_p == v_p) {return;}
//steps_count.fetch_add(1);
        //union_(u, v, node, true);

        if (data[node][u_p].load(std::memory_order_relaxed) & 1) {
            union_(u_p, v_p, node, true);
            return;
        }
        for (int i = 0; i < node_count; i++) {
            union_(u_p, v_p, i, (i == node));
        }
    }

    bool SameSet(int u, int v) override {
//std::cerr << "in sameset";
        auto node = getNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 1) == (data[node][v].load(std::memory_order_relaxed) >> 1)) {
            return true;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node, true);
            v_p = find(v_p, node, true);
            if (u_p == v_p) {
                return true;
            }
            if ((data[node][u_p].load(std::memory_order_acquire) >> 1) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return find(u, getNode(), true);
    }

    int find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                auto grand = data[node][(par >> 1)].load(std::memory_order_relaxed);
                if (par == grand) {
                    return (par >> 1);
                } else {
                    data[node][cur].compare_exchange_weak(par, grand);
                }
                cur = (par >> 1);
//steps_count.fetch_add(1);
            }
        } else {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_acquire);
                if ((par >> 1) == cur) {
                    return (par >> 1);
                }
                cur = (par >> 1);
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;

//        if (!is_local) {
//            u_p = find(u_p, node, is_local);
//            v_p = find(v_p, node, is_local);
//            if (((data[node][u_p].load(std::memory_order_acquire) & 1) == 0)
//            && ((data[node][v_p].load(std::memory_order_acquire) & 1) == 0)) {
//                return;
//            }
//            if (((u_p & 1) == 0) && ((v_p & 1) == 0)) {
//                return;
//            }
//        }

        while (true) {
            u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }
            if (u_p < v_p) {
                auto u_p_data1 = u_p*2 + 1;
                auto u_p_data0 = u_p*2;
                if (data[node][u_p].compare_exchange_weak(u_p_data1, ((v_p << 1) + 1))) {
                    return;
                } else {
                    data[node][u_p].compare_exchange_weak(u_p_data0, u_p_data1);
                }
            } else {
                auto v_p_data1 = v_p*2 + 1;
                auto v_p_data0 = v_p*2;
                if (data[node][v_p].compare_exchange_weak(v_p_data1, (u_p << 1) + 1)) {
//steps_count.fetch_add(1);
                    return;
                } else {
                    data[node][v_p].compare_exchange_weak(v_p_data0, v_p_data1);
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
    std::atomic<long long> steps_count;
};

#endif //TRY_DSU_NOSYNC_PARTS_H
