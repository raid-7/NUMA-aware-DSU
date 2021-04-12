//
// Created by Mariia Chukhareva on 4/12/21.
//

#ifndef TRY_DSU_SYNCONNODE_H
#define TRY_DSU_SYNCONNODE_H

class DSU_SyncOnNode : public DSU {
public:
    DSU_SyncOnNode(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        to_union.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
            to_union[i] = (std::atomic<__int64_t> *) numa_alloc_onnode(sizeof(std::atomic<__int64_t>), i);
            to_union[i]->store(0);
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
            to_union[i]->store(0);
        }
    }

    ~DSU_SyncOnNode() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * size);
            numa_free(to_union[i], std::atomic<__int64_t>);
        }
    }

    void Union(int u, int v) override {
        auto node = getNode();

        __int64_t uv = (__int64_t(u) << 32) + v;
        __int64_t zero = 0;

        while (true) {
            if (to_union[node]->compare_exchange_weak(zero, uv)) {
                break;
            } else {
                old_unions(node);
            }
        }

        for (int i = 0; i < node_count; i++) {
            if (i != node) {
                union_(u, v, i, (i == node));
            }
        }
    }

    bool SameSet(int u, int v) override {
        auto node = getNode();
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }
        old_unions(node);
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
        return find(u, getNode(), true);
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

private:
    void old_unions(int node) {
        __int64_t uv;
        while (true) {
            uv = to_union[node]->load(std::memory_order_acquire);
            if (uv == 0) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);

            for (int i = 0; i < node_count; i++) {
                union_(u, v, i, node, (i == node));
            }

            to_union[node]->compare_exchange_strong(uv, 0);
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
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return;
//        }

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

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<std::atomic<__int64_t>*> to_union;
};


#endif //TRY_DSU_SYNCONNODE_H
