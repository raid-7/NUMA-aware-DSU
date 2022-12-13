#pragma once

#include "../DSU.h"
#include "../lib/util.hpp"

#include <array>


template <bool Halfing>
class DSU_LazyUnions : public DSU {
public:
    std::string ClassName() override {
        using namespace std::string_literals;
        return "LazyUnions/"s + (Halfing ? "halfing" : "squashing");
    };

    DSU_LazyUnions(NUMAContext* ctx, int size)
        : DSU(ctx)
        , size(size), node_count(ctx->NodeCount()) {
        using namespace std::string_literals;
        REQUIRE(size <= MAX_VERTICES, "Max supported size: "s + std::to_string(MAX_VERTICES)
                                             + "; given size "s + std::to_string(size));
        REQUIRE(ctx->NodeCount() > 1, "Single node is not supported by LazyUnions implementation");

        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) Ctx_->Allocate(i, sizeof(std::atomic<int>) * size);
        }
        doReInit();
    }

    void ReInit() override {
        doReInit();
    }

    ~DSU_LazyUnions() override {
        for (int i = 0; i < node_count; i++) {
            Ctx_->Free(data[i], sizeof(int) * size);
        }
    }

    void Union(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return;
//        }
        while (true) {
            int uDat = find(u, node, true);
            u = getDataParent(uDat);
            int vDat = find(v, node, true);
            v = getDataParent(vDat);
            if (u == v) {
                return;
            }
            if (u < v) { // TODO try implicit pseudorandom priorities
                std::swap(u, v);
                std::swap(uDat, vDat);
            }

            if ((uDat & M_OWNERS) != M_OWNERS) {
                int uOwner = getAnyDataOwnerId(uDat);
                while (getDataParent(data[uOwner][u].load()) == u)
                    __builtin_ia32_pause();
            }

            if (tryUpdateParent(u, v, node))
                break;
        }
    }

    bool SameSet(int u, int v) override {
        int node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }
        while (true) {
            int uDat = find(u, node, true);
            u = getDataParent(uDat);
            int vDat = find(v, node, true);
            v = getDataParent(vDat);
            if (u == v) {
                return true;
            }
            if (getDataParent(readDataChecked(node, u)) == u) {
                return false;
            }
        }
    }

    int Find(int u) override {
        return getDataParent(find(u, NUMAContext::CurrentThreadNode(), true));
    }

private:
    int find(int u, int node, bool compressPaths) {
        if (compressPaths) {
            while (true) {
                int localDat;
                int parDat = readDataChecked(node, u, localDat);
                int par = getDataParent(parDat);
                int grandDat = readDataChecked(node, par);
                int grand = getDataParent(grandDat);
                if (par == grand) {
                    if (par != u && !isDataOwner(parDat, node)) {
                        // copy non-root vertex to local memory
                        data[node][u].compare_exchange_strong(localDat, mixDataOwner(parDat, node));
                    }
                    return grandDat;
                } else {
                    if (isDataOwner(localDat, node)) {
                        // compress local
                        data[node][u].compare_exchange_weak(localDat, mixDataOwner(grandDat, node));
                    } else {
                        // copy non-root vertex to local memory
                        // TODO try without this
                        data[node][u].compare_exchange_weak(localDat, mixDataOwner(grandDat, node));
                    }
                }
                if constexpr(Halfing) {
                    u = grand;
                } else {
                    u = par;
                }
            }
        } else {
            while (true) {
                int dat = readDataChecked(node, u);
                int par = getDataParent(dat);
                if (par == u) {
                    return dat;
                }
                u = par;
            }
        }
    }

    inline bool tryUpdateParent(int u, int v, int node) {
        int expected = u | M_OWNERS | M_FINALIZED;
        if (!data[0][u].compare_exchange_strong(expected, makeData(u, 1 << node, true)))
            return false;
        for (int i = 1; i < node_count; ++i) {
            data[i][u].store(makeData(u, 1 << node, true));
        }
        data[node][u].store(makeData(v, 1 << node, true));
        return true;
    }

    inline int readDataChecked(int primaryNode, int u) const {
        int localData;
        return readDataChecked(primaryNode, u, localData);
    }

    inline int readDataChecked(int primaryNode, int u, int& localData) const {
        localData = data[primaryNode][u].load(std::memory_order_acquire);
        if (isDataOwner(localData, primaryNode)) {
            return localData;
        }

        int node = getAnyDataOwnerId(localData);
        return readDataUnsafe(node, u);
    }

    inline int readDataUnsafe(int node, int u) const {
        int dat = data[node][u].load(std::memory_order_acquire);
        // assert isDataOwner(par, node)
        return dat;
    }

    void doReInit() {
        for (int i = 0; i < node_count; i++) {
            for (int j = 0; j < size; j++) {
                data[i][j].store(j | M_OWNERS | M_FINALIZED);
            }
        }
    }

    static inline bool getDataFinalized(int d) {
        return d & M_FINALIZED;
    }

    static inline int getDataParent(int d) {
        return d & ~(M_OWNERS | M_FINALIZED);
    }

    static inline bool isDataOwner(int d, int numa_node) {
        return d & (1 << (numa_node + M_SHIFT_OWNERS));
    }

    static inline int getAnyDataOwnerId(int d) {
        return OWNER_LOOKUP[getDataOwners(d)];
    }

    static inline int getDataOwners(int d) {
        return (d & M_OWNERS) >> M_SHIFT_OWNERS;
    }

    static inline int makeData(int parent, int owners, bool finalized) {
        return parent | (owners << M_SHIFT_OWNERS) | (finalized ? M_FINALIZED : 0);
    }

    static inline int mixDataOwner(int data, int ownerId) {
        return data | ((1 << ownerId) << M_SHIFT_OWNERS);
    }

    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;

    static constexpr int MAX_NUMA_NODES = 4;
    static constexpr int MAX_VERTICES = (1 << (31 - MAX_NUMA_NODES)) - 1;
    static constexpr int M_FINALIZED = 1 << 31;
    static constexpr int M_SHIFT_OWNERS = 31 - MAX_NUMA_NODES;
    static constexpr int M_OWNERS = ((1 << MAX_NUMA_NODES) - 1) << M_SHIFT_OWNERS;
    static constexpr std::array<int, 1 << MAX_NUMA_NODES> OWNER_LOOKUP = makeOwnerLookupTable<MAX_NUMA_NODES>();
};
