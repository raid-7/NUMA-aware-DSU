#include "../DSU.h"

class DSU_Helper : public DSU {
public:
    DSU_Helper(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        to_union.resize(size);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<__int64_t> *) numa_alloc_onnode(sizeof(std::atomic<__int64_t>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i] = (std::atomic<__int64_t> *) malloc(sizeof(std::atomic<__int64_t>));
            to_union[i]->store(1);
        }
        //to_union.store(1);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i]->store(1);
        }
        //to_union.store(1);
    }

    ~DSU_Helper() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = getNode();

        while (true) {
            __int64_t u_p = find(u, node, true);
            __int64_t v_p = find(v, node, true);
            if (u_p < v_p) {
                std::swap(u_p, v_p);
            }
            auto u_data = to_union[u_p]->load(std::memory_order_relaxed);
            if (u_data % 2 == 0) {
                continue;
            } else {
                if (to_union[u_p]->compare_exchange_strong(u_data, v_p * 2)) {
                    for (int i = 0; i < node_count; i++) {
                        union_(u_p, v_p, i, (node == i));
                    }
                    u_data = v_p * 2;
                    if (to_union[u_p]->compare_exchange_strong(u_data, v_p * 2 + 1)) {
                        for (int i = 0; i < node_count; i++) {
                            auto par = data[i][u_p].load(std::memory_order_acquire);
                            if (par % 2 == 0) {
                                data[i][u_p].compare_exchange_strong(par, par + 1);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    bool SameSet(int u, int v) override {
        return SameSetOnNode(u, v, getNode());
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
            if (getParent(node, u_p) == u_p) {
                return false;
            }
        }
    }

private:

    __int64_t find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
                auto grand = getParent(node, par);
                if (par == grand) {
                    return par;
                } else {
                    auto par_data = par * 2 + 1;
                    data[node][cur].compare_exchange_weak(par_data, grand * 2 + 1);
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

    void union_(int u, int v, int node, bool is_local, int cur_node) {
        __int64_t u_p = u;
        __int64_t v_p = v;
//        if (!is_local) {
//            u_p = find(u_p, cur_node, true);
//            v_p = find(v_p, cur_node, true);
//            if (data[node][u_p].compare_exchange_weak(u_p, v_p)) {
//                return;
//            }
//        }
        while (true) {
            u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }

            auto u_p_data = u_p*2 + 1;
            if (data[node][u_p].compare_exchange_weak(u_p_data, v_p * 2)) {
                return;
            }
        }
    }

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    __int64_t getParent(int node, int u) {
        auto par = data[node][u].load(std::memory_order_acquire);
        auto added = par % 2;
        par = par / 2;

        if (added == 1) {
            return par;
        } else {
            int status;
            auto data = to_union[u]->load(std::memory_order_acquire);
            if (par == data / 2) {
                if (status == 1) {
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
    std::vector<std::atomic<__int64_t>*> data;
    std::vector<std::atomic<__int64_t>*> to_union;
};