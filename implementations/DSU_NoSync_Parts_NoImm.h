#ifndef TRY_DSU_NOSYNC_PARTS_NO_IMM_H
#define TRY_DSU_NOSYNC_PARTS_NO_IMM_H

#include "../DSU.h"

class DSU_NoSync_Parts_NoImm : public DSU {
public:
    std::string ClassName() {
        return "NoSync_Parts_NoImm";
    };

    long long getStepsCount() {
        return steps_count.load();
    }

    void setStepsCount(int x) {
        steps_count.store(x);
    }
    // используется только в конструкторе
    int soleOwner(int mask) {
        int result = -1;
        bool has_owner = false;
        for (int i = 0; i < node_count; i++) {
            if (mask & (1 << i)) {
                if (has_owner) {
                    return -1;
                } else {
                    result = i;
                    has_owner = true;
                }
            }
        }
        return result;
    }


    DSU_NoSync_Parts_NoImm(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        owners_on_start.resize(size);
        owners.resize(size);
        for (int i = 0; i < size; i++) {
            this->owners[i] = (std::atomic_int*) malloc(sizeof(std::atomic_int));
        }
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                //data[i][j].store(j << 2);
                if (j % 2 == i) {
                    data[i][j].store(j << 2);
                } else {
                    data[i][j].store((j << 2) + 3);
                    owners[j]->store(1 << i);
                    owners_on_start[j] = (1 << i);
                }
            }
        }
        steps_count.store(0);
    }

    // owners -- for every v: node mask
    DSU_NoSync_Parts_NoImm(int size, int node_count, std::vector<int> owners) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j << 2);
            }
        }
        owners_on_start = owners;
        this->owners.resize(size);
        for (int i = 0; i < size; i++) {
            this->owners[i] = (std::atomic_int*) malloc(sizeof(std::atomic_int));
            this->owners[i]->store(owners[i]);
            int owner = soleOwner(owners[i]);
            if (owner != -1) {
                data[owner][i].store(i * 4 + 3);
            }
        }
        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j << 2);
            }
        }
        for (int i = 0; i < size; i++) {
            owners[i]->store(owners_on_start[i]);
            int owner = soleOwner(owners_on_start[i]);
            if (owner != -1) {
                data[owner][i].store(i * 4 + 3);
            }
        }
        steps_count.store(0);
    }

    ~DSU_NoSync_Parts_NoImm() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 2) == (data[node][v].load(std::memory_order_relaxed) >> 2)) {
            return;
        }
        auto u_p = u;//find(u, node, true);
        auto v_p = v;//find(v, node, true);
        while (true) {
            auto find_u = find(u_p, node, true);
            u_p = find_u >> 1;
            if (!(find_u & 1)) {
                u_p = loadNewV(u_p, node);
                continue;
            }
            auto find_v = find(v_p, node, true);
            v_p = find_v >> 1;
            if (!(find_v & 1)) {
                v_p = loadNewV(v_p, node);
                continue;
            }
            break;
        }
        if (u_p == v_p) { return; }
        if (u_p > v_p) { std::swap(u_p, v_p); }

        if (data[node][u_p].load(std::memory_order_acquire) & 1) {
            union_(u_p, v_p, node, true);
            return;
        }

        int mask = owners[u_p]->load(std::memory_order_acquire);
        int new_mask;
        while (true) {
            for (int i = 0; i < node_count; i++) {
                if (mask & (1 << i)) {
                    union_(u_p, v_p, i, (i == node));
                }
            }
            new_mask = owners[u_p]->load(std::memory_order_acquire);
            if (new_mask == mask) {
                break;
            } else {
                mask = new_mask;
            }
        }
    }

    bool SameSet(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 2) == (data[node][v].load(std::memory_order_relaxed) >> 2)) {
            return true;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            auto find_u = find(u_p, node, true);
            u_p = find_u >> 1;
            if (!(find_u & 1)) {
                u_p = loadNewV(u_p, node);
                continue;
            }
            auto find_v = find(v_p, node, true);
            v_p = find_v >> 1;
            if (!(find_v & 1)) {
                v_p = loadNewV(v_p, node);
                continue;
            }
            if (u_p == v_p) {
                return true;
            }
            if ((data[node][u_p].load(std::memory_order_acquire) >> 2) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return (find(u, NUMAContext::CurrentThreadNode(), true) >> 1);
    }

    int get_node_from_mask(int mask) {
        for (int i = 0; i < node_count; i++) {
            if ((mask >> i) & 1) {
                return i;
            }
        }
        return -1;
    }

    int loadNewV(int u, int to_node) {
        int from_node = get_node_from_mask(owners_on_start[u]);
        if (from_node == -1) {
            std::cerr << "on v=" + std::to_string(u) + " bad from_node\n";
        }
        auto u_p = u;
        while (true) {
            u_p = find_with_copy(u_p, from_node, to_node);
            while (true) {
                auto owners_was = owners[u_p]->load(std::memory_order_acquire);
                if (owners[u_p]->compare_exchange_weak(owners_was, owners_was | (1 << to_node))) {
                    break;
                }
            }
            auto from_node_u_data = data[from_node][u_p].load(std::memory_order_acquire);
            auto to_node_data = data[to_node][u_p].load(std::memory_order_acquire);
            if ((to_node_data >> 2) == (from_node_u_data >> 2)) {
                if (from_node_u_data & 1) {
                    if (!data[from_node][u_p].compare_exchange_weak(from_node_u_data, from_node_u_data - 1)) {
                        continue;
                    }
                }
                data[to_node][u_p].compare_exchange_weak(to_node_data, to_node_data | 2);
                return (to_node_data >> 2);
            }
        }
    }

    int find_with_copy(int u, int from_node, int to_node) {
        auto cur = u;
        while (true) {
            auto par = data[from_node][cur].load(std::memory_order_acquire) >> 2;
            if (par == cur) {
                break;
            }
            data[to_node][cur].store(par * 4 + 2);
            cur = par;
        }
        return cur;
    }

    // last bit -- do we have data on the node; 0 means no
    int find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                if (!(par & 2)) {
                    return (cur << 1);
                }
                auto grand = data[node][(par >> 2)].load(std::memory_order_relaxed);
                if (!(grand & 2)) {
                    return ((par >> 2) << 1);
                }
                if (par == grand) {
                    return ((par >> 2) << 1) + 1;
                } else {
                    data[node][cur].compare_exchange_weak(par, grand);
                }
                cur = (par >> 2);
            }
        } else {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_acquire);
                if (!(par & 2)) {
                    return (cur << 1);
                }
                if ((par >> 2) == cur) {
                    return ((par >> 2) << 1) + 1;
                }
                cur = (par >> 2);
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;

        while (true) {
            auto find_u = find(u_p, node, true);
            u_p = find_u >> 1;
            if (!(find_u & 1)) {
                u_p = loadNewV(u_p, node);
                continue;
            }
            auto find_v = find(v_p, node, true);
            v_p = find_v >> 1;
            if (!(find_v & 1)) {
                v_p = loadNewV(v_p, node);
                continue;
            }
            //u_p = find(u_p, node, is_local);
            //v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }
            //if (u_p < v_p) {
                auto u_p_data1 = u_p*4 + 3;
                auto u_p_data0 = u_p*4 + 2;
                if (data[node][u_p].compare_exchange_weak(u_p_data1, ((v_p << 2) + 3))) {
                    return;
                } else {
                    if (data[node][u_p].compare_exchange_weak(u_p_data0, ((v_p << 2) + 2))) {
                        return;
                    }
                }
            /*} else {
                auto v_p_data1 = v_p*4 + 3;
                auto v_p_data0 = v_p*4 + 2;
                if (data[node][v_p].compare_exchange_weak(v_p_data1, (u_p << 2) + 3)) {
                    return;
                } else {
                    if (data[node][v_p].compare_exchange_weak(v_p_data0, (u_p << 2) + 2)) {
                        return;
                    }
                }
            }*/
        }
    }

    int getParent(int u, int node) {
        int par_data = data[node][u].load(std::memory_order_acquire);

        if (par_data & 2) {
            return (par_data >> 2);
        } else {
            return -1;
        }
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data; // parent + is_onnode + is_sole_owner_bit
    std::vector<std::atomic<int>*> owners;
    std::atomic<long long> steps_count;

    std::vector<int> owners_on_start;
};

#endif //TRY_DSU_NOSYNC_PARTS_H
