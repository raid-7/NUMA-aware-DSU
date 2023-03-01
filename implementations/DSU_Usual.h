#include "../DSU.h"

class DSU_Usual : public DSU {
public:
    std::string ClassName() override {
        return "Usual";
    };

    DSU_Usual(int size) :size(size) {
        data1 = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * (size / 2), 0);
        data2 = (std::atomic<int>*) numa_alloc_onnode(sizeof(std::atomic<int>) * (size - (size / 2)), 1);
        for (int i = 0; i < size / 2; i++) {
            data1[i].store(i);
        }
        for (int i = size / 2; i < size; i++) {
            data2[i - (size / 2)] = i;
        }
    }

    void ReInit() override {
        for (int i = 0; i < size / 2; i++) {
            data1[i].store(i);
        }
        for (int i = size / 2; i < size; i++) {
            data2[i - (size / 2)] = i;
        }
    }

    ~DSU_Usual() {
        numa_free(data1, sizeof(std::atomic<int>) * (size / 2));
        numa_free(data2, sizeof(std::atomic<int>) * (size - (size / 2)));
    }

    void DoUnion(int u, int v) override {
        if (get(u)->load(std::memory_order_relaxed) == get(v)->load(std::memory_order_relaxed)) {
            return;
        }

        size_t uDepth = 0, vDepth = 0;

        int u_p = u;
        int v_p = v;
        while (true) {
            u_p = DoFind(u_p, uDepth);
            v_p = DoFind(v_p, vDepth);
            if (u_p == v_p) {
                break;
            }
            if (u_p < v_p) {
                if (get(u_p)->compare_exchange_weak(u_p, v_p)) {
                    break;
                }
            } else {
                if (get(v_p)->compare_exchange_weak(v_p, u_p)) {
                    break;
                }
            }
        }

        mHistFindDepth.inc(uDepth);
        mHistFindDepth.inc(vDepth);
    }

    bool DoSameSet(int u, int v) override {
        if (get(u)->load(std::memory_order_relaxed) == get(v)->load(std::memory_order_relaxed)) {
            mHistFindDepth.inc(1);
            mHistFindDepth.inc(1);
            return true;
        }

        size_t uDepth = 0, vDepth = 0;
        bool r;

        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = DoFind(u_p, uDepth);
            v_p = DoFind(v_p, vDepth);
            if (u_p == v_p) {
                r = true;
                break;
            }
            if (get(u_p)->load(std::memory_order_acquire) == u_p) {
                r = false;
                break;
            }
        }
        mHistFindDepth.inc(uDepth);
        mHistFindDepth.inc(vDepth);
        return r;
    }

    int Find(int u) override {
        size_t depth = 0;
        int r = DoFind(u, depth);
        mHistFindDepth.inc(r);
        return r;
    }

    int DoFind(int u, size_t& depth) {
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
            ++depth;
        }
    }

    std::atomic<int>* get(int i) {
        if (i < (size / 2)) {
            return &(data1[i]);
        } else {
            return &(data2[i - (size / 2)]);
        }
    }

    int size;
    std::atomic<int>* data1;
    std::atomic<int>* data2;
};