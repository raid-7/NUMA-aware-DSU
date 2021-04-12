//
// Created by Mariia Chukhareva on 4/10/21.
//

#ifndef TRY_DSU_CIRCULARBUFFER_H
#define TRY_DSU_CIRCULARBUFFER_H

class DSU_CircularBuffer : public DSU {
public:
    DSU_CircularBuffer(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        local_tail.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
            local_tail[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>), i);
            local_tail[i].store(0);
        }
        log_tail.store(0);
        min_tail.store(0);
        log = (std::atomic<__int64_t> *) numa_alloc_onnode(sizeof(std::atomic<__int64_t>) * LOG_SIZE, i);
        for (int i = 0; i < LOG_SIZE; i++) {
            log[i].store(0);
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
            local_tail[i].store(0);
        }
        log_tail.store(0);
        min_tail.store(0);
        for (int i = 0; i < LOG_SIZE; i++) {
            log[i].store(0);
        }
    }

    ~DSU_CircularBuffer() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(std::atomic<int>) * size);
            numa_free(local_tail[i], sizeof(std::atomic<__int64_t>) * LOG_SIZE);
        }
        numa_free(log, sizeof())
    }

    void Union(int u, int v) override {
        __int64_t uv = (__int64_t(u) << 32) + v;
        while (true) {
            int tail = log_tail.load(std::memory_order_acquire);
            int new_tail = tail + 1;
            if (new_tail == LOG_SIZE) {
                new_tail = 0;
            }
            while (true) {
                bool ok = true;
                for (int i = 0; i < node_count; i++) {
                    if (local_tail[i].load() == new_tail) {
                        ok = false;
                        continue;
                    }
                }
                if (ok) {
                    break;
                }
            }
            if (log_tail.compare_exchange_weak(tail, tail + 1)) {
                log[tail].store(uv);
                readLogW(tail);
                break;
            }
        }
    }

    bool SameSet(int u, int v) override {
        int node = getNode();
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = Find(u_p);
            v_p = Find(v_p);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        readLogR();
        return find(u, getNode());
    }

private:
    // читаем лог перед операцией чтения
    void readLogR() {
        auto node = getNode();
        while (true) {
            int local = local_tail[node].load();
            if (local == (log_tail.load() + 1) % LOG_SIZE) {
                break;
            }

            __int64_t uv = 0;
            if (uv == 0) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);
            _union(u, v, node);

            local_tail[node].compare_exchange_weak(local, (local + 1) % LOG_SIZE);
        }
    }

    // читаем лог перед операцией записи
    void readLogW(int last) {
        auto node = getNode();
        while (true) {
            int local = local_tail[node].load();
            if (local > last) {
                break;
            }
            if (local == (log_tail.load() + 1) % LOG_SIZE) {
                break;
            }

            __int64_t uv = 0;
            while (uv == 0) {
                uv = log[local].load();
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);
            _union(u, v, node);

            local_tail[node].compare_exchange_weak(local, (local + 1) % LOG_SIZE);
        }
    }

    void _union(int u, int v, int node) {
        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = _find(u_p, node);
            v_p = _find(v_p, node);
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

    int _find(int u, int node) {
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
    }

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;

    std::vector<std::atomic<int>*> local_tail;
    std::atomic<int> log_tail;
    std::atomic<int> min_tail; // TODO

    const int LOG_SIZE = 1e5;
    std::atomic<__int64_t>* log;
};

#endif //TRY_DSU_CIRCULARBUFFER_H
