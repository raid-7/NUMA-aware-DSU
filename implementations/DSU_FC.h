#ifndef TRY_DSU_FC_H
#define TRY_DSU_FC_H

#include "../DSU.h"

class DSU_FC: public DSU {
public:
    std::string ClassName() {
        return "FC";
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

        updates.resize(size*10);
        for (int i = 0; i < size*10; i++) {
            updates[i] = (std::atomic<long long>*) malloc(sizeof(std::atomic<long long>));
            updates[i]->store(0);
        }
        last.store(0);
        last_updated.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            last_updated[i] = 0;
        }

        steps_count.store(0);
        std::cerr << "fc: finished\n";
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }

        for (int i = 0; i < size*10; i++) {
            updates[i]->store(0);
        }
        last.store(0);
        for (int i = 0; i < node_count; i++) {
            //upd_in_progress[i]->store(false);
            last_updated[i] = 0;
        }

        steps_count.store(0);
    }

    ~DSU_FC() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    bool SameSet(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
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
        int node = NUMAContext::CurrentThreadNode();
        return do_find(node, u);
    }

    void Union(int u, int v) {
        int node = NUMAContext::CurrentThreadNode();
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        long long to_union = ( ((long long) u) << 32) + v;

        // add to queue
        while (true) {
            int the_last = last.load();
            if (last.compare_exchange_weak(the_last, the_last + 1)) {
                updates[the_last]->store(to_union);
                break;
            }
            if (the_last >= size) {
                std::cerr << "ALARM\n";
            }
        }

        // try to start updating
        if (upd_in_progress[node].try_lock()) {
            do_all(node);
            upd_in_progress[node].unlock();
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
            long long to_union;
            long long u, v;
            while (last_updated[node] < the_last) {
                to_union = updates[last_updated[node]]->load();
                while (to_union == 0) {
                    to_union = updates[last_updated[node]]->load(std::memory_order_acquire);
                }
                last_updated[node]++;
                u = to_union >> 32;
                v = to_union - (u << 32);
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

    std::vector<std::atomic<long long>*> updates; // u + v
    std::atomic<int> last; // in queue
    bool FALSE = false;
    bool TRUE = true;
    int ZERO = 0;
    //std::vector<std::atomic<bool>*> upd_in_progress; //on node
    std::mutex upd_in_progress[4];
    std::vector<int> last_updated; // on node

    std::atomic<long long> steps_count;
};

#endif //TRY_DSU_FC_H
