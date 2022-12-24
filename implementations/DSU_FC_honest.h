#ifndef TRY_DSU_FC_honest_H
#define TRY_DSU_FC_honest_H

#include "../DSU.h"

class DSU_FC_honest: public DSU {
public:
    std::string ClassName() {
        return "FC_honest";
    };

    DSU_FC_honest(int size, int node_count) :size(size), node_count(node_count) {
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

        is_done.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            is_done[i] = (std::atomic<bool>*) numa_alloc_onnode(size * sizeof(std::atomic<bool>), i);
            for (int j = 0; j < size; j++) {
                is_done[i][j].store(false);
            }
        }

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
            last_updated[i] = 0;
            for (int j = 0; j < size; j++) {
                is_done[i][j].store(false);
            }
        }
    }

    ~DSU_FC_honest() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            numa_free(is_done[i], sizeof(std::atomic<bool>)*size);
        }
    }

    bool DoSameSet(int u, int v) override {
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

    void DoUnion(int u, int v) {
        int node = NUMAContext::CurrentThreadNode();
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        long long to_union = ( ((long long) u) << 32) + v;
        if(to_union == 0) {return;}

        int the_last = 0;
        // add to queue
        while (true) {
            the_last = last.load();
            if (the_last >= size-1) {
                std::cerr << "ALARM\n";
            }
            if (last.compare_exchange_weak(the_last, the_last + 1)) {
                //updates[the_last]->store(to_union);
                if (!updates[the_last]->compare_exchange_strong(ZERO, to_union)) {
                    std::cerr << "WHAT??\n";
                }
                break;
            }
        }

        // try to start updating
        if (upd_in_progress[node].try_lock()) {
        //if (upd_in_progress[node]->compare_exchange_weak(FALSE, true)) {
            do_all(node);
            upd_in_progress[node].unlock();
        }

        while (true) {
            if (is_done[node][the_last].load()) {
                break;
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
            long long to_union;
            long long u, v;
            while (last_updated[node] < the_last) {
                to_union = updates[last_updated[node]]->load();
                while (to_union == 0) {
                    to_union = updates[last_updated[node]]->load();
                }
                last_updated[node]++;
                u = to_union >> 32;
                v = to_union - (u << 32);
                do_union(node, u, v);
                is_done[node][last_updated[node] - 1].store(true);
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
    std::vector<std::atomic<bool>*> is_done; //for every node for every update
    // std::vector<std::atomic<bool>*> result; // for SameSet operations
    std::atomic<int> last; // in queue
    bool FALSE = false;
    bool TRUE = true;
    long long ZERO = 0;
   // std::vector<std::atomic<bool>*> upd_in_progress; //on node
    std::mutex upd_in_progress[4];
    std::vector<int> last_updated; // on node
};

#endif //TRY_DSU_FC_honest_H
