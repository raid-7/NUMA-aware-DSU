#include "../DSU.h"

class DSU_Helper : public DSU {
public:
    DSU_Helper(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        to_union.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        to_union.store(0);
    }

    ~DSU_Helper() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = getNode();
        if (node_count == 1) {
            node = 0;
        }

        __int64_t uv = (__int64_t(u) << 32) + v;
        __int64_t zero = 0;

        while (true) {
            if (to_union.compare_exchange_weak(zero, uv)) {
                break;
            } else {
                old_unions(node);
            }
        }

        old_unions(node);
        //union_(u, v, node);
    }

    bool SameSet(int u, int v) override {
        auto node = getNode();
        if (node_count == 1) {
            node = 0;
        }

        return SameSetOnNode(u, v, node);
    }

    int Find(int u) override {
        auto node = getNode();
        if (node_count > 1) {
            old_unions(node);
        } else {
            node = 0;
        }

        return find(u, node, true);
    }

    bool SameSetOnNode(int u, int v, int node) {
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }

        if (node_count > 1) {
            old_unions(node);
        }

//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }

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
    void old_unions(int node) {
        __int64_t uv;
        while (true) {
            uv = to_union.load(std::memory_order_acquire);
            if (uv == 0) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);

            for (int i = 0; i < node_count; i++) {
                union_(u, v, i, (i == node), node);
            }

            to_union.compare_exchange_strong(uv, 0);
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
//        auto node = 0;
//        auto msk = numa_get_run_node_mask();
//        for (int i = 1; i < node_count; i++) {
//            if (numa_bitmask_isbitset(msk, i)) {
//                node = i;
//            }
//        }
//        return node;
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::atomic<__int64_t> to_union;
};