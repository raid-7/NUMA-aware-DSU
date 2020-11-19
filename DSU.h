#include <sched.h>
#include <thread>
#include <numa.h>
#include <atomic>
#include <vector>

#include "Queue.h"

class DSU {
public:
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;
};

class DSU_Sequential : public DSU {
public:
    DSU_Sequential(int size) {
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
        while (true) {
            auto uv = to_union.load(std::memory_order_acquire);
            if (uv == 0) {
                break;
            }
            __int64_t u = uv >> 32;
            __int64_t v = uv - (u << 32);

            for (int i = 0; i < node_count; i++) {
                union_(u, v, i, (i == node));
            }

            to_union.compare_exchange_weak(uv, 0);
        }
    }

    int find(int u, int node, bool is_local) {
        if (is_local) {
            auto cur = u;
            while (true) {
                auto par = data[node][cur].load(std::memory_order_acquire);
                auto grand = data[node][par].load(std::memory_order_acquire);
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
                auto par = data[node][cur].load(std::memory_order_acquire);
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

class DSU_MSQ : public DSU {
public:
    DSU_MSQ(int size, int node_count) : size(size), node_count(node_count) {
        data.resize(node_count);
        queues.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j);
            }

            queues[i] = (Queue*) numa_alloc_onnode(sizeof(Queue), i);
            queues[i]->Init(i);
        }
    }

    ~DSU_MSQ() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            numa_free(queues[i], sizeof(Queue));
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
            queues[i]->Push(std::make_pair(u, v));
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
            if (data[node][u_p].load() == u_p) {
                return false;
            }
        }
    }
private:
    void old_unions(int node) {
        while (true) {
            auto p = queues[node]->Pop();
            if (p == nullptr) {
                break;
            }
            union_(p->first, p->second, node);
            numa_free(p, sizeof(std::pair<int, int>));
        }
    }

    int find(int u, int node) {
        auto cur = u;
        while (true) {
            auto par = data[node][cur].load();
            auto grand = data[node][par].load();
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
            if (rand() % 2) {
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
    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<Queue*> queues;
};