#ifndef TRY_DSU_PARTS_H
#define TRY_DSU_PARTS_H

#include "../DSU.h"

class DSU_Parts : public DSU {
public:
    std::string ClassName() {
        return "Parts";
    };

    long long getStepsCount() {
        return steps_count.load();
    }

    void setStepsCount(int x) {
        steps_count.store(x);
    };

    DSU_Parts(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        to_union.resize(size);
        owners_on_start.resize(size);
        node_full_mask = 0;
        status_bit = 1 << node_count;
        for (int i = 0; i < node_count; i++) {
            node_full_mask = node_full_mask + (1 << i);
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store((j * 2 + 1) << 2);
                if (j%2 != i) {
                    data[i][j].store(((j * 2 + 1) << 2) + 3);
                }
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i] = (std::atomic<int> *) malloc(sizeof(std::atomic<int>));
            to_union[i]->store((i*2 + 1) << node_count);
            if (i % 2 == 0) {
                to_union[i]->store(((i*2 + 1) << node_count) | (2));
                owners_on_start[i] = 2;
            } else {
                to_union[i]->store(((i*2 + 1) << node_count) | (1));
                owners_on_start[i] = 1;
            }
        }
        steps_count.store(0);
    }

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

    DSU_Parts(int size, int node_count, std::vector<int> owners)
            :size(size), node_count(node_count), owners_on_start(owners) {
        data.resize(node_count);
        to_union.resize(size);
        node_full_mask = 0;
        status_bit = 1 << node_count;
        for (int i = 0; i < node_count; i++) {
            node_full_mask = node_full_mask + (1 << i);
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                if (soleOwner(owners[j]) == i) {
                    data[i][j].store(((j * 2 + 1) << 2) + 3);
                } else {
                    if (owners[j] & (1 << i)) {
                        data[i][j].store(((j * 2 + 1) << 2) + 2);
                    } else {
                        data[i][j].store((j * 2 + 1) << 2);
                    }
                }
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i] = (std::atomic<int> *) malloc(sizeof(std::atomic<int>));
            to_union[i]->store(((i*2 + 1) << node_count) | owners[i]);
        }
        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                if (soleOwner(owners_on_start[j]) == i) {
                    data[i][j].store(((j * 2 + 1) << 2) + 3);
                } else {
                    if (owners_on_start[j] & (1 << i)) {
                        data[i][j].store(((j * 2 + 1) << 2) + 2);
                    } else {
                        data[i][j].store((j * 2 + 1) << 2);
                    }
                }
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i]->store(((i*2 + 1) << node_count) | owners_on_start[i]);
        }
        steps_count.store(0);
    }

    ~DSU_Parts() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    bool SameSet(int u, int v) override {
        int node = getNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 3) == (data[node][v].load(std::memory_order_relaxed) >> 3)) {return true;}

        auto u_p = u;
        auto v_p = v;
        int res;
        while (true) {
            res = find(u_p, node, true);
            u_p = (res >> 1);
            if (!(res & 1)) {
                u_p = load_new_v(u_p, node);
                continue;
            }
            res = find(v_p, node, true);
            v_p = (res >> 1);
            if (!(res & 1)) {
                v_p = load_new_v(v_p, node);
                continue;
            }

            if (u_p == v_p) {
                return true;
            }
            if (getParent(node, u_p) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        int node = getNode();
        while (true) {
            auto res = find(u, getNode(), true);
            if (!(res & 1)) {
                load_new_v(u, node);
            } else {
                return (res >> 1);
            }
        }
    }

    void Union(int u, int v) {
        int node = getNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 3) == (data[node][v].load(std::memory_order_relaxed) >> 3)) {return;}

        auto u_p = u;
        auto v_p = v;
        int res;
        while (true) {
            res = find(u_p, node, true);
            u_p = (res >> 1);
            if (!(res & 1)) {
                u_p = load_new_v(u_p, node);
                //steps_count.fetch_add(1);
                continue;
            }
            res = find(v_p, node, true);
            v_p = (res >> 1);
            if (!(res & 1)) {
                v_p = load_new_v(v_p, node);
                //steps_count.fetch_add(1);
                continue;
            }
            if (u_p == v_p) { return; }

            //steps_count.fetch_add(1);
            if (u_p < v_p) {
                std::swap(u_p, v_p);
            }

            if (data[node][u_p].load(std::memory_order_acquire) & 1) {
                union_(u_p, v_p, node, true);
                //steps_count.fetch_add(1);
                return;
            }

            auto u_data = to_union[u_p]->load(std::memory_order_relaxed);
            if (!(u_data & status_bit)) {
                // union_(u_p, v_p, node, true);
                // __builtin_ia32_pause()
                continue;
            } else {
                auto u_mask = u_data & node_full_mask;
                auto new_u_data = ((v_p*2) << node_count) | u_mask;
                if (to_union[u_p]->compare_exchange_strong(u_data, new_u_data)) {
                    int mask = u_mask;
                    while (true) {
                        for (int i = 0; i < node_count; i++) {
                            if ((mask >> i) & 1) {
                                union_(u_p, v_p, i, (node == i));
                            }
                        }

                        if (to_union[u_p]->compare_exchange_strong(new_u_data, (new_u_data | status_bit))) {
                            for (int i = 0; i < node_count; i++) {
                                if (mask & (1 << i)) {
                                    auto par = data[i][u_p].load(std::memory_order_acquire);
                                    if (!(par & (4))) {
                                        data[i][u_p].compare_exchange_strong(par, (par | 4));
                                    }
                                }
                            }
                            break;
                        } else {
                            new_u_data = to_union[u_p]->load(std::memory_order_acquire);
                            mask = new_u_data & node_full_mask;
                        }
                    }
                    break;
                }
            }
        }
    }

    int get_mask(int u) {
        return (to_union[u]->load(std::memory_order_acquire) & node_full_mask);
    }

    int load_new_v(int u, int to_node) {
        return load_new_v(u, to_node, get_mask(u));
    }

    int load_new_v(int u, int to_node, int with_mask) {
        //steps_count.fetch_add(1);
        int from_node = get_node_from_mask(with_mask);
        auto u_p = u;

        while (true) {
            u_p = find_with_copy(u_p, from_node, to_node);
            //if (data[from_node][u_p].load(std::memory_order_acquire) == data[to_node][u_p].load(std::memory_order_acquire)) {
            //auto to_union_data = to_union[u_p]->load(std::memory_order_acquire);
            auto to_node_data = data[to_node][u_p].load(std::memory_order_acquire);
            auto from_node_data = data[from_node][u_p].load(std::memory_order_acquire);
            if ((to_node_data >> 3) != (from_node_data >> 3)) {continue;}
            if (from_node_data & 1) {
                if (!data[from_node][u_p].compare_exchange_weak(from_node_data, from_node_data - 1)) {
                    continue;
                }
            }
            auto to_union_data = to_union[u_p]->load(std::memory_order_acquire);
            if (to_union[u_p]->compare_exchange_weak(to_union_data, (to_union_data | (1 << to_node)))) {
                data[to_node][u_p].compare_exchange_weak(to_node_data, to_node_data | 2);
                break;
            } else {
                if (to_union_data & (1 << to_node)) {
                    data[to_node][u_p].compare_exchange_weak(to_node_data, to_node_data | 2);
                    break;
                }
            }
        }
        return u_p;
    }

    int get_node_from_mask(int mask) {
        for (int i = 0; i < node_count; i++) {
            if ((mask >> i) & 1) {
                return i;
            }
        }
    }

    int find_with_copy(int u, int from_node, int to_node) {
        auto cur = u;
        while (true) {
            auto par = getParent(from_node, cur);//data[from_node][cur].load(std::memory_order_acquire);
            //if (par == -1) {std::cerr << "ooooooooooooooooooops\n" + std::to_string(to_union[cur]->load()) + "\n";}
            if (par == cur) {
                return par;
            }
            auto par_data = (par << 3) + 4 + 2;
            data[to_node][cur].store(par_data);
            cur = par;
            //while (true) {
            //    auto q = to_union[cur]->load(std::memory_order_acquire); // or relaxed
            //    if (to_union[cur]->compare_exchange_weak(q, q | (1 << to_node)) || (q & (1 << to_node))) {break;}
            //    data[to_node][cur].compare_exchange_weak(par_data, (par_data | 2));
            //}
        }
    }

    // ends with 1 if ok and with 0 if need info
    int find(int u, int node, bool is_local) {
        if (is_local) {
            int par_data;
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
                if (par == -1) {
                    return (cur << 1);
                }
                auto grand = getParent(node, par);
                if (grand == -1) {
                    return (par << 1);
                }
                if (par == grand) {
                    return ((par << 1) + 1);
                } else {
                    par_data = ((par * 2 + 1) * 2 + 1) * 2 + 1; //здесь +1 валидна только для эксперимента
                    data[node][cur].compare_exchange_weak(par_data, (grand * 4 + 2 + 1) * 2 + 1); // здесь тоже
                }
                cur = par;
            }
        } else {
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
                if (par == -1) {
                    return (cur << 1);
                }
                if (par == cur) {
                    return ((par << 1) + 1);
                }
                cur = par;
            }
        }
    }

    void union_(__int64_t u, __int64_t v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;
        while (true) {
            auto find_u = find(u_p, node, is_local);
            u_p = find_u >> 1;
            if (!(find_u & 1)) {
                u_p = load_new_v(u_p, node);
                continue;
            }
            auto find_v = find(v_p, node, is_local);
            v_p = (find_v >> 1);
            if (!(find_v & 1)) {
                 v_p = load_new_v(v_p, node);
                 continue;
            }
            if (u_p == v_p) {
                return;
            }
//            if (v_p < u_p) {
//                auto v_p_data = ((v_p * 2 + 1)*2 + 1) * 2 + 1;
//                if (data[node][v_p].compare_exchange_weak(v_p_data, (u_p * 4 + 3) * 2 + 1)) {
//                    return;
//                } else {
//                    continue;
//                }
//            }

            auto u_p_data_was = data[node][u_p].load(std::memory_order_acquire);
            if (u_p_data_was & 1) {
                if (data[node][u_p].compare_exchange_weak(u_p_data_was, (v_p*8 + 4 + 2 + 1))) {
                    break;
                }
            } else {
                auto u_p_data = u_p * 8 + 4 + 2 + 0;//  ((u_p * 2 + 1)*2 + 1) * 2 + 1;
                if (data[node][u_p].compare_exchange_weak(u_p_data,  (v_p * 8 + 0 + 2 + 0))) { // и здесь
                    // auto u_p_data = ((u_p * 2 + 1)*2 + 1) * 2 + 1;
                    // steps_count.fetch_add(1);
                    return;
                }
            }
        }
    }

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int getParent(int node, int u) {
        auto par_data = data[node][u].load(std::memory_order_acquire);
        par_data = par_data >> 1;

        if ((par_data & 2) && (par_data & 1)) {
            return (par_data >> 2);
        } else {
            auto par = par_data >> 2;
            auto to_union_data = to_union[u]->load(std::memory_order_acquire);
            auto mask = to_union_data & node_full_mask;
            if (!((mask >> node) & 1)) {
                if (mask == 0) {
                    if (to_union[u]->compare_exchange_weak(to_union_data, to_union_data + (1 << node))) {
                        return u;
                    } else {
                        if (!(to_union[u]->load() & (1 << node))) {
                            return -1;
                        } else {
                            return u;
                        }
                    }
                } else {
                    return -1;
                }
            }
            to_union_data = to_union_data >> node_count;

            if (par == (to_union_data >> 1)) {
                if (to_union_data & 1) {
//                    data[node][u].compare_exchange_weak(par_data, (par_data | 2));
                    return par;
                } else {
                    return u;
                }
            } else {
//                data[node][u].compare_exchange_weak(par_data, (par_data | 2));
                return par;
            }
        }
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data; // parent + union_status_bit + is_onnode + is_sole_owner_bit
    std::vector<std::atomic<int>*> to_union; // v + status + mask
    int node_full_mask;
    int status_bit;
    std::atomic<long long> steps_count;

    std::vector<int> owners_on_start;
};

#endif //TRY_DSU_PARTS_H
