#include "../DSU.h"

class DSU_Usual : public DSU {
public:
    std::string ClassName() {
        return "Usual";
    };

    DSU_Usual(int size) :size(size) {
        data1 = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * (size / 2), 0);
        data2 = (std::atomic<int>*) numa_alloc_onnode(sizeof(std::atomic<int>) * (size - (size / 2)), 1);
        for (int i = 0; i < size / 2; i++) {
            data1[i].store(i);
        }
        for (int i = size / 2; i < size; i++) {
            data2[i - (szie / 2)] = i;
        }
    }

    void ReInit() override {
        for (int i = 0; i < size / 2; i++) {
            data1[i].store(i);
        }
        for (int i = size / 2; i < size; i++) {
            data2[i - (szie / 2)] = i;
        }
    }

    ~DSU_Usual() {
        numa_free(data1, sizeof(std::atomic<int>) * (size / 2));
        numa_free(data2, sizeof(std::atomic<int>) * (size - (size / 2)));
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
                if (get(u_p)->compare_exchange_weak(u_p, v_p)) {
                    return;
                }
            } else {
                if (get(v_p)->compare_exchange_weak(v_p, u_p)) {
                    return;
                }
            }
        }
    }

    bool SameSet(int u, int v) override {
        if (get(u)->load(std::memory_order_relaxed) == get(v)->load(std::memory_order_relaxed)) {
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
            if (get(u_p)->load(std::memory_order_acquire) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        auto cur = u;
        while (true) {
            auto par = get(cur)->load(std::memory_order_relaxed);
            auto grand = get(par)->load(std::memory_order_relaxed);
            if (par == grand) {
                return par;
            } else {
                get(cur)->compare_exchange_weak(par, grand);
            }
            cur = par;
        }
    }

private:
    std::atomic<int>* get(int i) {
        if (i <= (size / 2)) {
            return &(data1[i]);
        } else {
            return &(data2[i - (size / 2)]);
        }
    }

    int size;
    std::atomic<int>* data1;
    std::atomic<int>* data2;
};