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
            } else {
                to_union[i]->store(((i*2 + 1) << node_count) | (1));
            }
        }
        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store((j * 2 + 1) << 2);
                if (j%2 != i) {
                    data[i][j].store(((j * 2 + 1) << 2) + 3);
                }
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i]->store((i*2 + 1) << node_count);
            if (i % 2 == 0) {
                to_union[i]->store(((i*2 + 1) << node_count) | (2));
            } else {
                to_union[i]->store(((i*2 + 1) << node_count) | (1));
            }
        }
        steps_count.store(0);
    }

    ~DSU_Parts() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
//std::cerr << "in union";
        auto node = getNode();
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {return;}
//if ((u%2 == node) || (v%2 ==node)) {
//std::cerr << "union " + std::to_string(u) + " and " + std::to_string(v) + " on node " + std::to_string(node) + "\n";
//}
        UnionOnNode(u, v, node);
    }

    bool SameSet(int u, int v) override {
//std::cerr << "in sameset";
        int node = getNode();
        if ((data[node][u].load(std::memory_order_relaxed) >> 3) == (data[node][v].load(std::memory_order_relaxed) >> 3)) {return true;}
        // return SameSetOnNode(u, v, node);
        auto u_p = u;
        auto v_p = v;
        int res;// std::pair<int, bool> res;
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
//steps_count.fetch_add(1);
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

    void UnionOnNode(int u, int v, int node) {
//std::cerr << "in uniononnode";
        if ((data[node][u].load(std::memory_order_relaxed) >> 3) == (data[node][v].load(std::memory_order_relaxed) >> 3)) {return;}
        auto u_p = u;
        auto v_p = v;
        int res;
        while (true) {
            res = find(u_p, node, true);
            u_p = (res >> 1);
//std::cerr << "*"+std::to_string(u_p)+"*";
            /*           if (!(res & 1)) {
           //                load_new_v(u_p, node);
           //std::cerr << "1: " + std::to_string(u_p) +  " in union " + std::to_string(u) + " and " + std::to_string(v) + " on node " + std::to_string(node) + "\n";//"*first " +std::to_string(u_p)+"*";
           u_p = load_new_v(u_p, node);
           steps_count.fetch_add(1);
                           continue;
                       }*/
            res = find(v_p, node, true);
            v_p = (res >> 1);
//std::cerr << "*"+std::to_string(v_p)+"*";
            /*if (!(res & 1)) {
                //load_new_v(v_p, node);
//std::cerr << "2: " + std::to_string(data[node][v_p].load()) + "\n";//"*second " +std::to_string(v_p)+"*";
//std::cerr << "2: " + std::to_string(v_p) +  " in union " + std::to_string(u) + " and " + std::to_string(v)+ " on node " + std::to_string(node) + "\n";
v_p = load_new_v(v_p, node);
steps_count.fetch_add(1);
                continue;
            }*/
//          if (u_p < v_p) {
//                std::swap(u_p, v_p);
//            }
            if (u_p == v_p) { return; }
//if (data[node][u_p].load(std::memory_order_acquire) & 1) {
            union_(u_p, v_p, node, true);
//steps_count.fetch_add(1);
            return;
//}
            steps_count.fetch_add(1);
            //if (u_p < v_p) {
            //    std::swap(u_p, v_p);
            //}
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
//steps_count.fetch_add(1);
                        }
                    }
                    break;
                } else {
//std::cerr << "-";
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
        steps_count.fetch_add(1);
/*if (u%2 == 1)*/  //{std::cerr << "load " + std::to_string(u) + " to " + std::to_string(to_node) + "\n";}
//std::cerr << "in load_new_v\n";
        int from_node = get_node_from_mask(with_mask);

        auto u_p = u;
        while (true) {
            u_p = find_with_copy(u_p, from_node, to_node);
            //if (data[from_node][u_p].load(std::memory_order_acquire) == data[to_node][u_p].load(std::memory_order_acquire)) {
            //auto to_union_data = to_union[u_p]->load(std::memory_order_acquire);
            auto to_union_data = to_union[u_p]->load(std::memory_order_acquire);
//std::cerr << " " + std::to_string(to_union_data) + "\n";
            auto res = data[to_node][u_p].load(std::memory_order_acquire);
            if ((res >> 3) != (data[from_node][u_p].load(std::memory_order_acquire) >> 3)) {continue;}
            if (to_union[u_p]->compare_exchange_weak(to_union_data, (to_union_data | (1 << to_node)))) {
                data[to_node][u_p].compare_exchange_weak(res, res | 2);
                break;
            }
            else {
                uint32_t source_val = to_union_data;
                uint32_t bitmask = (1 << to_node);
                uint32_t val = (source_val & bitmask);
                if (val)// (((source_val / (bitmask)) % 2)==1)//((unsigned int)(to_union_data) & ((unsigned int)(1) << (unsigned int)(to_node))) != 0)
                {
                    data[to_node][u_p].compare_exchange_weak(res, res | 2);
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
//std::cerr << "in find_with_copy\n";
        auto cur = u;
        while (true) {
            auto par = getParent(from_node, cur);//data[from_node][cur].load(std::memory_order_acquire);
            if (par == -1) {std::cerr << "ooooooooooooooooooops\n" + std::to_string(to_union[cur]->load()) + "\n";}
            auto par_data = par * 8;
            data[to_node][cur].store(par_data);
            //par = par >> 1;
            if (par == cur) {
                return par;
            }
            cur = par;
            while (true) {
                auto q = to_union[cur]->load(std::memory_order_acquire); // or relaxed
                if (to_union[cur]->compare_exchange_weak(q, q | (1 << to_node)) || (q & (1 << to_node))) {break;}
                data[to_node][cur].compare_exchange_weak(par_data, (par_data | 2));
            }
        }
    }

    // ends with 1 if ok and with 0 if need info
    int find(int u, int node, bool is_local) {
//std::cerr << "in find\n";
        if (is_local) {
            int par_data;
            auto cur = u;
            while (true) {
                auto par = getParent(node, cur);
/*                if (par == -1) {
//std::cerr << "find-q\n";
                    return (cur << 1);//std::make_pair(cur, false);
                }*/
                auto grand = getParent(node, par);
                /*   if (grand == -1) {
   //std::cerr << "find-w\n";
                       return (par << 1);//std::make_pair(par, false);
                   }*/
                if (par == grand) {
//std::cerr << "find-e\n";
                    return ((par << 1) + 1);//std::make_pair(par, true);
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
//std::cerr << "find-r\n";
                    return (cur << 1);//std::make_pair(cur, false);
                }
                if (par == cur) {
//std::cerr << "find-t\n";
                    return ((par << 1) + 1);//std::make_pair(par, true);
                }
                cur = par;
            }
        }
    }

    void union_(__int64_t u, __int64_t v, int node, bool is_local) {
//std::cerr << "in union_\n";
        int u_p = u;
        int v_p = v;
//        auto u_p_data = ((u_p * 2 + 1)*2 + 1) * 2 + 1; // здесь тоже
        while (true) {
            u_p = find(u_p, node, is_local);
            u_p = u_p >> 1; // здесь еще нужен чек про -1
            auto find_result = find(v_p, node, is_local);
            v_p = (find_result >> 1);
            /* if (!(find_result & 1)) {
                 v_p = load_new_v(v_p, node);
                 continue;
             }*/
            //v_p = find_result.first;
            if (u_p == v_p) {
                return;
            }
            if (v_p < u_p) {
                auto v_p_data = ((v_p * 2 + 1)*2 + 1) * 2 + 1;
                if (data[node][v_p].compare_exchange_weak(v_p_data, (u_p * 4 + 3) * 2 + 1)) {
                    return;
                } else {
                    continue;
                }
            }


            //auto u_p_data = u_p*2 + 1;
            auto u_p_data = ((u_p * 2 + 1)*2 + 1) * 2 + 1;
            if (data[node][u_p].compare_exchange_weak(u_p_data, (v_p * 4 + 3) * 2 + 1)) { // и здесь
//auto u_p_data = ((u_p * 2 + 1)*2 + 1) * 2 + 1;
//steps_count.fetch_add(1);
                return;
            }
        }
    }

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int getParent(int node, int u) {
//steps_count.fetch_add(1);
//std::cerr << "in getparent\n";
        auto par_data = data[node][u].load(std::memory_order_acquire);
        par_data = par_data >> 1; // здесь тоже

        if ((par_data & 2) && (par_data & 1)) {
            return (par_data >> 2);
        } else {
//steps_count.fetch_add(1);
            auto par = par_data >> 2;
            auto to_union_data = to_union[u]->load(std::memory_order_acquire);
            auto mask = to_union_data & node_full_mask;
            if (!((mask >> node) & 1)) {
                if (mask == 0) {
                    if (to_union[u]->compare_exchange_weak(to_union_data, to_union_data + (1 << node))) {
//data[node][u].compare_exchange_weak(par_data, (par_data | 3));
                        return u;
                    } else {
//std::cerr << "-1 zero mask\n";
                        if (!(to_union[u]->load() & (1 << node)))
                        {return -1;}
                        else {
//  data[node][u].compare_exchange_weak(par_data, (par_data | 3));
                            return u;
                        }
                        //return -1;
                    }
                } else {
//std::cerr << "-1\n";
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
    std::vector<std::atomic<int>*> data;
    std::vector<std::atomic<int>*> to_union;
    int node_full_mask;
    int status_bit;
    std::atomic<long long> steps_count;
};

#endif //TRY_DSU_PARTS_H
