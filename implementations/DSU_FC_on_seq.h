#ifndef TRY_DSU_FC_on_seq_H
#define TRY_DSU_FC_on_seq_H

#include "../DSU.h"

class DSU_FC_on_seq: public DSU {
public:
    std::string ClassName() {
        return "FC_on_seq";
    };

    DSU_FC_on_seq(int size, int node_count) : size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (int*) malloc(sizeof(int) * size);
            for (int j = 0; j < size; j++) {
                data[i][j] = j;
            }
        }

        operations.resize(size*10);
        for (int i = 0; i < size*10; i++) {
            operations[i] = (std::atomic<long long>*) malloc(sizeof(std::atomic<long long>));
            operations[i]->store(0);
        }
        last.store(0);
        last_updated.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            last_updated[i] = 0;
        }

        is_done.resize(node_count);
        result.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            is_done[i] = (std::atomic<bool>*) numa_alloc_onnode(size * 10 * sizeof(std::atomic<bool>), i);
            result[i] = (std::atomic<bool>*) numa_alloc_onnode(size * 10 * sizeof(std::atomic<bool>), i);
            for (int j = 0; j < size*10; j++) {
                is_done[i][j].store(false);
                result[i][j].store(false);
            }
        }

        std::cerr << "fc: finished\n";
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j] = j;
            }
        }

        for (int i = 0; i < size*10; i++) {
            operations[i]->store(0);
        }
        last.store(0);
        for (int i = 0; i < node_count; i++) {
            last_updated[i] = 0;
            for (int j = 0; j < size*10; j++) {
                is_done[i][j].store(false);
            }
        }
    }

    ~DSU_FC_on_seq() {
        for (int i = 0; i < node_count; i++) {
            free(data[i]);
            numa_free(is_done[i], sizeof(std::atomic<bool>)*size);
        }
    }

    bool SameSet(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        if (data[node][u] == data[node][v]) {
            return true;
        }
        long long op_data = ( ((long long) u) << 32) + v;
        op_data = (op_data << 1) + 1;

        int the_last = 0;
        // add to queue
        while (true) {
            the_last = last.load();
            if (last.compare_exchange_weak(the_last, the_last + 1)) {
                operations[the_last]->store(op_data);
                break;
            }
            if (the_last >= size) {
                std::cerr << "ALARM\n";
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
        return result[node][the_last].load();
    }

    int Find(int u) override {
        int node = NUMAContext::CurrentThreadNode();
        return do_find(node, u);
    }

    void Union(int u, int v) {
        int node = NUMAContext::CurrentThreadNode();
        if (data[node][u] == data[node][v]) {
            return;
        }
        long long to_union = ( ((long long) u) << 32) + v;
        to_union = (to_union << 1);

        int the_last = 0;
        // add to queue
        while (true) {
            the_last = last.load();
            if (last.compare_exchange_weak(the_last, the_last + 1)) {
                operations[the_last]->store(to_union);
                break;
            }
            if (the_last >= size) {
                std::cerr << "ALARM\n";
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
        int cur = v;
        while (data[node][cur] != cur) {
            auto par = data[node][cur];
            data[node][cur] = data[node][par];
            cur = par;
        }
        return cur;
    }

    void do_union(int node, int u, int v) {
        int u_p = do_find(node, u);
        int v_p = do_find(node, v);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[node][u_p] = v_p;
        } else {
            data[node][v_p] = u_p;
        }
    }

    bool do_sameset(int node, int u, int v) {
        return (do_find(node, u) == do_find(node, v));
    }

    void do_all(int node) {
        while (true) {
            int the_last = last.load();
            long long op_data;
            long long u, v;
            while (last_updated[node] < the_last) {
                int id = last_updated[node];
                op_data = operations[id]->load();
                while (op_data == 0) {
                    op_data = operations[id]->load();
                }
                last_updated[node]++;
                if ((op_data & 1) == 0) {
                    op_data = op_data >> 1;
                    u = op_data >> 32;
                    v = op_data - (u << 32);
                    do_union(node, u, v);
                } else {
                    op_data = op_data >> 1;
                    u = op_data >> 32;
                    v = op_data - (u << 32);
                    bool res = do_sameset(node, u, v);
                    result[node][id].store(res);
                }
                is_done[node][id].store(true);
            }
            if (the_last == last.load()) {
                break;
            }
        }
    }

    int size;
    int node_count;
    std::vector<int*> data;

    std::vector<std::atomic<long long>*> operations; // u + v + 0/1   // 0 for update 1 for read
    std::vector<std::atomic<bool>*> is_done; //for every node for every update
    std::vector<std::atomic<bool>*> result; // for SameSet operations
    std::atomic<int> last; // in queue
    bool FALSE = false;
    bool TRUE = true;
    //std::vector<std::atomic<bool>*> upd_in_progress; //on node
    std::mutex upd_in_progress[4];
    std::vector<int> last_updated; // on node


};

#endif //TRY_DSU_FC_on_seq_H
