#include <sched.h>
#include <thread>
#include <numa.h>
#include <atomic>
#include <vector>

#include "./utils/ConcurrencyFreaks/queues/MichaelScottQueue.hpp"
#include "./utils/ConcurrencyFreaks/queues/array/FAAArrayQueue.hpp"
#include "tbb/concurrent_queue.h"

class DSU {
public:
    virtual void ReInit() = 0;
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;
};

class DSU_Sequential : public DSU {
public:
    DSU_Sequential(int size) {
        this->size = size;
        data.resize(size);
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
    }

    void ReInit() override {
        data.resize(size);
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
    }

    void Union(int u, int v) override {
        int u_p = Find(u);
        int v_p = Find(v);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[u_p] = v_p;
        } else {
            data[v_p] = u_p;
        }
    }

    int Find(int u) override {
        int cur = u;
        while (data[cur] != cur) {
            auto par = data[cur];
            data[cur] = data[par];
            cur = par;
        }
        return cur;
    }

    bool SameSet(int u, int v) override {
        return Find(u) == Find(v);
    }

private:
    int size;
    std::vector<int> data;
};

class DSU_Helper : public DSU {
public:
    DSU_Helper(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        to_union.store(0);
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
        to_union.store(0);
    }

    ~DSU_Helper() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        __int64_t uv = (__int64_t(u) << 32) + v;
        __int64_t zero = 0;

        while (true) {
            if (to_union.compare_exchange_weak(zero, uv)) {
                break;
            } else {
                old_unions(node);
            }
        }

        old_unions(node);
        //union_(u, v, node);
    }

    bool SameSet(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        return SameSetOnNode(u, v, node);
    }

    int Find(int u) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count > 1) {
            old_unions(node);
        } else {
            node = 0;
        }

        return find(u, node, true);
    }

    bool SameSetOnNode(int u, int v, int node) {
        if (node_count > 1) {
            old_unions(node);
        }

        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node, true);
            v_p = find(v_p, node, true);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

private:
    void old_unions(int node) {
        __int64_t uv;
        while (true) {
            uv = to_union.load(std::memory_order_acquire);
            if (uv == 0) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);

            for (int i = 0; i < node_count; i++) {
                union_(u, v, i, (i == node));
            }

            to_union.compare_exchange_strong(uv, 0);
        }
    }

    int find(int u, int node, bool is_local) {
        if (is_local) {
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
        } else {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;
        if (!is_local) {
            u_p = find(u_p, numa_node_of_cpu(sched_getcpu()), true);
            v_p = find(v_p, numa_node_of_cpu(sched_getcpu()), true);
            if (u_p < v_p) {
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
        while (true) {
            u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
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
    std::atomic<__int64_t> to_union;
};

class DSU_Queue : public DSU {
public:
    DSU_Queue(int size, int node_count) : size(size), node_count(node_count) {
        data.resize(node_count);
        queues.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }

            auto addr = (tbb::concurrent_queue<__int64_t>*) numa_alloc_onnode(sizeof(tbb::concurrent_queue<__int64_t>), i);
            //queues[i]->Init(i);
            queues[i] = new (addr) tbb::concurrent_queue<__int64_t>();
        }
    }

    void ReInit() {
        for (int i = 0; i < node_count; i++) {
            //numa_free(data[i], sizeof(int) * size);
            //numa_free(queues[i], sizeof(FAAArrayQueue<std::pair<int, int>>));
            numa_free(queues[i], sizeof(tbb::concurrent_queue<std::pair<int, int>>));
        }
        for (int i = 0; i < node_count; i++) {
            //data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }

            auto addr = (tbb::concurrent_queue<__int64_t>*) numa_alloc_onnode(sizeof(tbb::concurrent_queue<__int64_t>), i);
            //queues[i]->Init(i);
            queues[i] = new (addr) tbb::concurrent_queue<__int64_t>();
        }
    }

    ~DSU_Queue() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            //numa_free(queues[i], sizeof(FAAArrayQueue<std::pair<int, int>>));
            //numa_free(queues[i], sizeof(tbb::concurrent_queue<std::pair<int, int>>));
            numa_free(queues[i], sizeof(tbb::concurrent_queue<__int64_t>));
        }
    }

    void Union(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        for (int i = 0; i < node_count; i++) {
            if (i == node) {
                continue;
            }
            //auto p = std::make_pair(u, v);
            //queues[i]->enqueue(&p, get_tid());
            //queues[i]->push(std::make_pair(u, v));
            __int64_t uv = (__int64_t(u) << 32) + v;
            queues[i]->push(uv);
        }

        union_(u, v, node);
    }

    bool SameSet(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }
        return SameSetOnNode(u, v, node);
    }

    int Find(int u) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count > 1) {
            old_unions(node);
        } else {
            node = 0;
        }

        return find(u, node);
    }

    bool SameSetOnNode(int u, int v, int node) {
        if (node_count > 1) {
            old_unions(node);
        }

        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node);
            v_p = find(v_p, node);
            if (u_p == v_p) {
                return true;
            }
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }
private:
    void old_unions(int node) {
        //std::pair<int, int> p;
        __int64_t uv;
        while (true) {
            //auto p = queues[node]->dequeue(get_tid());
//            if (p == nullptr) {
//                break;
//            }
            auto done = queues[node]->try_pop(uv);
            if (!done) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);
            //union_(p.first, p.second, node);
            union_(u, v, node);
        }
    }

    int find(int u, int node) {
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

    void union_(int u, int v, int node) {
        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = find(u_p, node);
            v_p = find(v_p, node);
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

private:
    int get_tid() {
        static std::atomic_int counter;
        counter = 0;
        thread_local static std::unique_ptr<int> tid = std::make_unique<int>(++counter);
        return *tid;
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;

    //std::vector<FAAArrayQueue<std::pair<int, int>>*> queues;
    std::vector<tbb::concurrent_queue<__int64_t>*> queues;
};


class DSU_NO_SYNC : public DSU {
public:
    DSU_NO_SYNC(int size, int node_count) :size(size), node_count(node_count) {
        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }
        }
    }

    ~DSU_NO_SYNC() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        for (int i = 0; i < node_count; i++) {
            union_(u, v, i, (i == node));
        }
    }

    bool SameSet(int u, int v) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        return SameSetOnNode(u, v, node);
    }

    int Find(int u) override {
        auto node = numa_node_of_cpu(sched_getcpu());
        if (node_count == 1) {
            node = 0;
        }

        return find(u, node, true);
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
            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

private:
    int find(int u, int node, bool is_local) {
        if (is_local) {
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
        } else {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_relaxed);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        int u_p = u;
        int v_p = v;
        if (!is_local) {
            u_p = find(u_p, numa_node_of_cpu(sched_getcpu()), true);
            v_p = find(v_p, numa_node_of_cpu(sched_getcpu()), true);
            if (u_p < v_p) {
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
        while (true) {
            u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
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
};

class DSU_USUAL : public DSU {
public:
    DSU_USUAL(int size) :size(size) {
        data = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, 0);
        for (int i = 0; i < size; i++) {
            data[i].store(i);
        }
    }

    void ReInit() override {
        for (int i = 0; i < size; i++) {
            data[i].store(i);
        }
    }

    ~DSU_USUAL() {
        numa_free(data, sizeof(int) * size);
    }

    void Union(int u, int v) override {
        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = Find(u_p);
            v_p = Find(v_p);
            if (u_p == v_p) {
                return;
            }
            if (u_p < v_p) {
                if (data[u_p].compare_exchange_weak(u_p, v_p)) {
                    return;
                }
            } else {
                if (data[v_p].compare_exchange_weak(v_p, u_p)) {
                    return;
                }
            }
        }
    }

    bool SameSet(int u, int v) override {
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = Find(u_p);
            v_p = Find(v_p);
            if (u_p == v_p) {
                return true;
            }
            if (data[u_p].load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        auto cur = u;
        while (true) {
            auto par = data[cur].load(std::memory_order_relaxed);
            auto grand = data[par].load(std::memory_order_relaxed);
            if (par == grand) {
                return par;
            } else {
                data[cur].compare_exchange_weak(par, grand);
            }
            cur = par;
        }
    }

private:
    int size;
    std::atomic<int>* data;
};