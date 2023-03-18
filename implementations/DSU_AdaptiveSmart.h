#pragma once

#include "../DSU.h"
#include "../lib/util.hpp"

#include <array>
#include <sstream>


template <bool Halfing, bool AllowCrossNodeCompression=false>
class DSU_AdaptiveSmart : public DSU {
public:
    std::string ClassName() override {
        using namespace std::string_literals;
        return "AdaptiveSmart/"s + (Halfing ? "halfing" : "squashing");
    };

    DSU_AdaptiveSmart(NUMAContext* ctx, int size)
        : DSU(ctx)
        , size(size), node_count(ctx->NodeCount()) {
        using namespace std::string_literals;
        REQUIRE(size <= MAX_VERTICES, "Max supported size: "s + std::to_string(MAX_VERTICES)
                                             + "; given size "s + std::to_string(size));

        data.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) Ctx_->Allocate(i, sizeof(std::atomic<int>) * size);
        }
        doReInit();
    }

    void ReInit() override {
        doReInit();
    }

    void SetOwner(int v, int node) {
        for (int i = 0; i < node_count; i++) {
            int par = data[i][v].load(std::memory_order_relaxed);
            data[i][v].store(makeData(getDataParent(par), 1 << node, true), std::memory_order_relaxed);
        }
    }

    ~DSU_AdaptiveSmart() override {
        for (int i = 0; i < node_count; i++) {
            Ctx_->Free(data[i], sizeof(int) * size);
        }
    }

    void DoUnion(int u, int v) override {
        auto node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return;
//        }
        int uDat, vDat;
        u = findLocalOnly(u, node, uDat);
        v = findLocalOnly(v, node, vDat);
        if (u == v)
            return;
        bool uRoot = isDataOwner(uDat, node);
        bool vRoot = isDataOwner(vDat, node);
        size_t it = 0;
        while (true) {
            ++it;
            if (!((uRoot && u > v) || (vRoot && v > u))) { // find real roots
                uDat = find(u, node, true);
                vDat = find(v, node, true);
                u = getDataParent(uDat);
                v = getDataParent(vDat);
                if (u == v)
                    return;
                uRoot = vRoot = true;
            }
            if (u < v) { // TODO try implicit pseudorandom priorities
                std::swap(u, v);
                std::swap(uDat, vDat);
                std::swap(uRoot, vRoot);
            }

            // u is root now
            // make u child of v

            int owner = getAnyDataOwnerId(uDat);
            // every root must have exactly one owner
            // assert getDataOwners(uDat) == (1 << owner)
            if (owner == node) {
                mThisNodeWrite.inc(1);
            } else {
                mCrossNodeWrite.inc(1);
            }
            if (data[owner][u].compare_exchange_strong(uDat, makeData(v, 1 << owner, true)))
                break;

            uRoot = false;
        }
    }

    bool DoSameSet(int u, int v) override {
        int node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }
        int uDat, vDat;
        u = findLocalOnly(u, node, uDat);
        v = findLocalOnly(v, node, vDat);
        if (u == v)  // ancestors in current node match?
            return true;
        if (isDataOwner(uDat, node) && isDataOwner(vDat, node)) // found real roots?
            if (getDataParent(readDataChecked(node, u)) == u) // still root?
                return false;
        if (!isDataOwner(uDat, node)) {
            // make one step up outside of loop to save local read
            mCrossNodeRead.inc(1);
            uDat = readDataUnsafe(getAnyDataOwnerId(uDat), u);
        }
        if (!isDataOwner(vDat, node)) {
            // make one step up outside of loop to save local read
            mCrossNodeRead.inc(1);
            vDat = readDataUnsafe(getAnyDataOwnerId(vDat), v);
        }
        while (true) {
            u = getDataParent(uDat);
            v = getDataParent(vDat);
            if (u == v) {
                return true;
            }
            if (getDataParent(readDataChecked(node, u)) == u) {
                return false;
            }
            uDat = find(u, node, true);
            vDat = find(v, node, true);
        }
    }

    int Find(int u) override {
        return getDataParent(find(u, NUMAContext::CurrentThreadNode(), true));
    }

private:
    int findLocalOnly(int u, int node, int& localParDat) { // returns vertex
        while (true) {
            mThisNodeRead.inc(1);
            int parDat = readDataUnsafe(node, u);
            if (!isDataOwner(parDat, node)) {
                localParDat = parDat;
                return u;
            }
            int par = getDataParent(parDat);
            mThisNodeRead.inc(1);
            int grandDat = readDataUnsafe(node, par);
            if (!isDataOwner(grandDat, node)) {
                localParDat = grandDat;
                return par;
            }
            int grand = getDataParent(grandDat);
            if (par == grand) {
                localParDat = parDat;
                return par;
            } else {
                // compress local
                mThisNodeWrite.inc(1);
                data[node][u].compare_exchange_weak(parDat, grandDat);
            }
            if constexpr(Halfing) {
                u = grand;
            } else {
                u = par;
            }
        }
    }

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
                        mThisNodeWrite.inc(1);
                        data[node][u].compare_exchange_strong(localDat, mixDataOwner(parDat, node));
                    }
                    return grandDat;
                } else {
                    if (isDataOwner(localDat, node)) {
                        if (AllowCrossNodeCompression || isDataOwner(grandDat, node)) {
                            // compress local if we know about `par`
                            mThisNodeWrite.inc(1);
                            data[node][u].compare_exchange_weak(localDat, mixDataOwner(grandDat, node));
                        } else {
                            u = par; // else do not compress, but go to par even if Halfing enabled
                            continue;
                        }
                    } else {
                        // copy non-root vertex to local memory
                        // TODO try without this
                        mThisNodeWrite.inc(1);
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

    inline int readDataChecked(int primaryNode, int u) const {
        int localData;
        return readDataChecked(primaryNode, u, localData);
    }

    inline int readDataChecked(int primaryNode, int u, int& localData) const {
        localData = data[primaryNode][u].load(std::memory_order_acquire);
        mThisNodeRead.inc(1);
        if (isDataOwner(localData, primaryNode)) {
            mThisNodeReadSuccess.inc(1);
            return localData;
        }

        int node = getAnyDataOwnerId(localData);
        mCrossNodeRead.inc(1);
//        std::ostringstream s{};
//        s << "Current node: " << primaryNode << "; owners: " << getDataOwners(localData) << "; anyNode: " << node;
//        std::cout << s.str() << std::endl;
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
                data[i][j].store(makeData(j, 1, true));
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
