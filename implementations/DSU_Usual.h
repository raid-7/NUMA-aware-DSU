#include "../DSU.h"

class DSU_USUAL : public DSU {
public:
    DSU_USUAL(int size) :size(size) {
        data = (std::atomic<int> *) malloc(sizeof(std::atomic<int>) * size);
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
        free(data);
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
        if (data[u].load(std::memory_order_relaxed) == data[v].load(std::memory_order_relaxed)) {
            return true;
        }

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