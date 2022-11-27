#ifndef TRY_DSU_HELPER_H
#define TRY_DSU_HELPER_H

#include "../DSU.h"

class DSU_Helper : public DSU {
public:
    std::string ClassName() {
        return "Helper";
    };

    DSU_Helper(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<__int64_t> *) numa_alloc_onnode(sizeof(std::atomic<__int64_t>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        to_union.store(1);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        to_union.store(1);
    }

    ~DSU_Helper() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();

        __int64_t u_p = find(u, node, true);
        __int64_t v_p = find(v, node, true);

        __int128_t uv = makeToUnion(u_p, v_p);//(__int64_t(u) << 32) + v;

        while (true) {
            auto was = to_union.load(std::memory_order_acquire);
            if (was % 2 == 1) {
                if (to_union.compare_exchange_weak(was, uv)) {
                    break;
                }
                old_unions(node);
            } else {
                old_unions(node);
            }
        }

        old_unions(node);
    }

    bool SameSet(int u, int v) override {
        return SameSetOnNode(u, v, NUMAContext::CurrentThreadNode());
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
            if (getParent(node, u_p) == u_p) {
                return false;
            }
        }
    }

private:
    void old_unions(int node) {
        __int128_t uv;
        int from, to, status;
        while (true) {
            uv = to_union.load(std::memory_order_acquire);
            std::tie(from, to, status) = getToUnion(uv);
            if (status == 1) {
                break;
            }

            for (int i = 0; i < node_count; i++) {
                union_(from, to, i, (i == node), node);
            }

            if (to_union.compare_exchange_strong(uv, uv + 1)) {
                for (int i = 0; i < node_count; i++) {
                    __int64_t par = data[i][from].load(std::memory_order_acquire);
                    if (par % 2 == 0) {
                        data[i][from].compare_exchange_strong(par, par + 1);
                    }
                }
            }
        }
    }

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

    __int64_t getParent(int node, int u) {
        auto par = data[node][u].load(std::memory_order_acquire);
        auto added = par % 2;
        par = par / 2;

        if (added == 1) {
            return par;
        } else {
            int from, to, status;
            std::tie(from, to, status) = loadToUnion();
            if (u == from && par == to) {
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

    std::tuple<int, int, int> loadToUnion() {
        auto uv = to_union.load(std::memory_order_acquire);
        return getToUnion(uv);
    }

    std::tuple<int, int, int> getToUnion(__int128_t uv) {
        int status = uv % 2;
        uv = uv / 2;
        __int64_t u = uv >> 32;
        __int64_t v = uv - (u << 32);
        return std::make_tuple(u, v, status);
    }

    __int128_t makeToUnion(int u, int v) {
        __int128_t uv = (__int64_t(u) << 32) + v;
        uv = uv * 2;
        return uv;
    }

    int size;
    int node_count;
    std::vector<std::atomic<__int64_t>*> data;
    std::atomic<__int128_t> to_union;
};

#endif