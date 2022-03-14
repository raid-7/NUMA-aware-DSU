#ifndef TRY_DSU_FC_H
#define TRY_DSU_FC_H

#include "../DSU.h"

class DSU_FC: public DSU {
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

    DSU_FC(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }

        updates.resize(size);
        last.store(0);
        upd_in_progress.resize(node_count);
        last_updated.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            upd_in_progress[i].store(false);
            last_updated[i] = 0;
        }

        steps_count.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }

        last.store(0);
        for (int i = 0; i < node_count; i++) {
            upd_in_progress[i].store(false);
            last_updated.resize(node_count);
        }

        steps_count.store(0);
    }

    ~DSU_FC() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    bool SameSet(int u, int v) override {
        auto node = getNode();
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return true;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = do_find(node, u_p);
            v_p = do_find(node, v_p);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        int node = getNode();
        return do_find(node, u);
    }

    void Union(int u, int v) {
        int node = getNode();
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        int to_union = u << 32 + v;

        // add to queue
        while (true) {
            int the_last = last.load();
            if (last.compare_exchange_weak(the_last, the_last + 1)) {
                updates[the_last] = to_union;
                break;
            }
        }

        // try to start updating
        if (upd_in_progress[node].compare_exchange_weak(FALSE, true)) {
            do_all(node);
            while (true) {
                if (upd_in_progress[node].compare_exchange_weak(TRUE, false)) {
                    break;
                }
            }
        }

    }

    int do_find(int node, int v) {
        auto cur = v;
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
    }

    void do_all(int node) {
        while (true) {
            int the_last = last.load();
            while (last_updated[node] < the_last) {
                int to_union = updates[last_updated[node]++];
                int u = to_union >> 32;
                int v = to_union - u;
                do_union(node, u, v);
            }
            if (the_last == last.load()) {
                break;
            }
        }
    }

    void do_union(int node, int u, int v) {
        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = do_find(node, u_p);
            v_p = do_find(node, v_p);
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

    std::vector<int> updates; // u + v
    std::atomic<int> last; // in queue
    bool FALSE = false;
    bool TRUE = true;
    std::vector<std::atomic<bool>> upd_in_progress; //on node
    std::vector<int> last_updated; // on node

    std::atomic<long long> steps_count;
};

#endif //TRY_DSU_FC_H
