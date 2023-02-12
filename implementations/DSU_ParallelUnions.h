#ifndef TRY_DSU_PARALLELUNIONS_H
#define TRY_DSU_PARALLELUNIONS_H


#include "../DSU.h"


template <bool Halfing>
class DSU_ParallelUnions : public DSU {
public:
    std::string ClassName() override {
        using namespace std::string_literals;
        return "ParallelUnions/"s + (Halfing ? "halfing" : "squashing");
    };

    DSU_ParallelUnions(NUMAContext* ctx, int size)
        : DSU(ctx)
        , size(size), node_count(ctx->NodeCount()) {
        data.resize(node_count);
        to_union = std::vector<std::atomic<int>>(size);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) Ctx_->Allocate(i, sizeof(std::atomic<int>) * size);
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i].store(i * 2 + 1);
        }
    }

    void ReInit() override {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j * 2 + 1);
            }
        }
        for (int i = 0; i < size; i++) {
            to_union[i].store(i * 2 + 1);
        }
    }

    ~DSU_ParallelUnions() {
        for (int i = 0; i < node_count; i++) {
            Ctx_->Free(data[i], sizeof(int) * size);
        }
    }

    void DoUnion(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        // commented to match adaptive impls
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u, node, true);
            v_p = find(v, node, true);
            if (u_p == v_p) {
                return;
            }
            if (u_p < v_p) {
                std::swap(u_p, v_p);
            }
            auto u_data = u_p * 2 + 1;
            if (u_data % 2 == 0) { // FIXME
#if defined(__x86_64__)
                __builtin_ia32_pause();
#endif
                continue;
            } else {
                mGlobalDataAccess.inc(1);
                if (to_union[u_p].compare_exchange_strong(u_data, v_p * 2)) {
                    for (int i = 0; i < node_count; i++) {
                        union_(u_p, v_p, i, (node == i));
                    }
                    u_data = v_p * 2;
                    mGlobalDataAccess.inc(1);
                    if (to_union[u_p].compare_exchange_strong(u_data, v_p * 2 + 1)) {
                        mCrossNodeWrite.inc(node_count - 1);
                        mThisNodeWrite.inc(1);
                        for (int i = 0; i < node_count; i++) {
                            auto par = data[i][u_p].load(std::memory_order_acquire);
                            data[i][u_p].compare_exchange_strong(par, par | 1);
                        }
                    }
                    break;
                }
            }
        }
    }

    bool DoSameSet(int u, int v) override {
        int node = NUMAContext::CurrentThreadNode();
        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
            return true;
        }
        auto u_p = u;
        auto v_p = v;
        while (true) {
            u_p = find(u_p, node, true);
            v_p = find(v_p, node, true);
            if (u_p == v_p) {
                return true;
            }
            mThisNodeRead.inc(1);
            if (getParent(node, u_p) == u_p) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return find(u, NUMAContext::CurrentThreadNode(), true);
    }

private:

    int find(int u, int node, bool is_local) {
        if (is_local && DSU::EnableCompaction) {
            auto cur = u;
            while (true) {
                mThisNodeRead.inc(2);
                auto par = getParent(node, cur);
                auto grand = getParent(node, par);
                if (par == grand) {
                    return par;
                } else {
                    auto par_data = (par << 1) | 1;
                    mThisNodeWrite.inc(1);
                    data[node][cur].compare_exchange_weak(par_data, ((grand << 1) | 1));
                }
                if constexpr(Halfing) {
                    cur = grand;
                } else {
                    cur = par;
                }
            }
        } else {
            auto cur = u;
            while (true) {
                if (is_local) {
                    mThisNodeRead.inc(1);
                } else {
                    mCrossNodeRead.inc(1);
                }
                auto par = getParent(node, cur);
                if (par == cur) {
                    return par;
                }
                cur = par;
            }
        }
    }

    void union_(int u, int v, int node, bool is_local) {
        auto u_p = u;
        auto v_p = v;
        auto u_p_data = u_p * 2 + 1;
        while (true) {
            //u_p = find(u_p, node, is_local);
            v_p = find(v_p, node, is_local);
            if (u_p == v_p) {
                return;
            }

            //auto u_p_data = u_p*2 + 1;
            (is_local ? mThisNodeWrite : mCrossNodeWrite).inc(1);
            if (data[node][u_p].compare_exchange_weak(u_p_data, v_p * 2)) {
                return;
            }
        }
    }

    int getParent(int node, int u) {
        auto par = data[node][u].load(std::memory_order_acquire);

        if ((par & 1) == 1) {
            return (par >> 1);
        } else {
            par = par >> 1;
            mGlobalDataAccess.inc(1);
            auto data = to_union[u].load(std::memory_order_acquire);
            if (par == (data >> 1)) {
                if ((data & 1) == 1) {
                    return par;
                } else {
                    return u;
                }
            } else {
                return par;
            }
        }
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    std::vector<std::atomic<int>> to_union;

};


#endif //TRY_DSU_PARALLELUNIONS_H
