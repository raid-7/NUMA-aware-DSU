#pragma once

#include "../DSU.h"
#include "../lib/util.hpp"

#include <array>
#include <sstream>


template <bool Halfing, bool Stepping, bool AllowCrossNodeCompression=true>
class DSU_Adaptive : public DSU {
public:
    std::string ClassName() override {
        using namespace std::string_literals;
        int version = Stepping ? 3 : 2;
        return "Adaptive"s + std::to_string(version) +  "/"s +
            (Halfing ? "halfing" : "squashing");
    };

    DSU_Adaptive(NUMAContext* ctx, int size)
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

    ~DSU_Adaptive() override {
        for (int i = 0; i < node_count; i++) {
            Ctx_->Free(data[i], sizeof(int) * size);
        }
    }

private:
    struct DepthStats {
        size_t local = 0;
        size_t crossNode = 0;

        size_t total() const {
            return local + crossNode;
        }
    };

protected:
    void DoUnion(int u, int v) override {
        DepthStats uStats, vStats;

        DoUnionWithStats(u, v, uStats, vStats);

        mHistLocalFindDepth.inc(uStats.local);
        mHistLocalFindDepth.inc(vStats.local);
        mHistCrossNodeFindDepth.inc(uStats.crossNode);
        mHistCrossNodeFindDepth.inc(vStats.crossNode);
        mHistFindDepth.inc(uStats.total());
        mHistFindDepth.inc(vStats.total());
    }

    void DoUnionWithStats(int u, int v, DepthStats& uStats, DepthStats& vStats) {
        auto node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return;
//        }
        int u_, v_; // unused
        u = findLocalOnly(u, node, u_, uStats.local);
        v = findLocalOnly(v, node, v_, vStats.local);
        if (u == v)
            return;
        while (true) {
            int uDat = find(u, node, EnableCompaction, uStats.crossNode);
            u = getDataParent(uDat);
            int vDat = find(v, node, EnableCompaction, vStats.crossNode);
            v = getDataParent(vDat);
            if (u == v) {
                return;
            }
            if (u < v) { // TODO try implicit pseudorandom priorities
                std::swap(u, v);
                std::swap(uDat, vDat);
                if (DSU::EnableMetrics) {
                    std::swap(uStats, vStats);
                }
            }

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
        }
    }

    bool DoSameSet(int u, int v) override {
        DepthStats uStats, vStats;
        bool r;
        if constexpr(Stepping) {
            DepthStats stats;

            if constexpr (Halfing) {
                r = DoSteppingSameSet(u, v, stats);
            } else {
                r = DoSteppingSameSetSquashingOnly(u, v, stats);
            }

            uStats.local = stats.local / 2;
            uStats.crossNode = stats.crossNode / 2;
            vStats.local = stats.local - uStats.local;
            vStats.crossNode = stats.crossNode - uStats.crossNode;
        } else {
            r = DoSimpleSameSet(u, v, uStats, vStats);
        }

        mHistLocalFindDepth.inc(uStats.local);
        mHistLocalFindDepth.inc(vStats.local);
        mHistCrossNodeFindDepth.inc(uStats.crossNode);
        mHistCrossNodeFindDepth.inc(vStats.crossNode);
        mHistFindDepth.inc(uStats.total());
        mHistFindDepth.inc(vStats.total());

        return r;
    }

    bool DoSteppingSameSetSquashingOnly(int u, int v, DepthStats& stats) {
        int node = NUMAContext::CurrentThreadNode();
        int prevUDat = 0, prevVDat = 0;
        int prevU = u, prevV = v;
        while (true) {
            if (u == v)
                return true;
            if (u < v) {
                std::swap(u, v);
                std::swap(prevU, prevV);
                std::swap(prevUDat, prevVDat);
            }

            int localDat;
            int parDat = readDataChecked(node, u, localDat);
            int par = getDataParent(parDat);
            ++(isDataOwner(parDat, node) ? stats.local : stats.crossNode);

            if (EnableCompaction && prevU != u) {
                // ordinary compaction
                mThisNodeWrite.inc(1);
                data[node][prevU].store(mixDataOwner(parDat, node));
            }
            if (!EnableCompaction && par != u && !isDataOwner(localDat, node)) {
                // copy non-root vertex to local memory
                mThisNodeWrite.inc(1);
                data[node][u].store(mixDataOwner(parDat, node));
            }

            if (EnableCompaction && (par == u || par == v)) {
                // we are going to return
                // copy non-root vertices to local memory

                if (par != u && !isDataOwner(localDat, node)) {
                    mThisNodeWrite.inc(1);
                    data[node][u].store(mixDataOwner(parDat, node));
                }
                if (prevVDat && prevV != v && !isDataOwner(prevVDat, node)) {
                    mThisNodeWrite.inc(1);
                    data[node][prevV].compare_exchange_strong(
                            prevVDat,makeData(v, (1 << node) | getDataOwners(prevVDat), true));
                }
            }

            if (par == v)
                return true;
            if (par == u) // u is root
                // u cannot be the root of v, because v < u
                return false;

            prevU = u;
            prevUDat = localDat;
            u = par;
        }
    }

    bool DoSteppingSameSet(int u, int v, DepthStats& stats) { // TODO stats
        int node = NUMAContext::CurrentThreadNode();
        bool freeze = false;
        while (true) {
            if (u == v)
                return true;
            if (!freeze && u < v)
                std::swap(u, v);
            int localDat, localParDat;
            int parDat = readDataChecked(node, u, localDat);
            int par = getDataParent(parDat);
            ++(isDataOwner(parDat, node) ? stats.local : stats.crossNode);
            if (par == v)
                return true;
            int grandDat = readDataChecked(node, par, localParDat);
            ++(isDataOwner(grandDat, node) ? stats.local : stats.crossNode);
            int grand = getDataParent(grandDat);
            if (par == grand) {
                if (par != u && !isDataOwner(localDat, node)) {
                    // copy non-root vertex to local memory
                    mThisNodeWrite.inc(1);
                    data[node][u].store(mixDataOwner(parDat, node));
                }
                if (freeze) {
                    int vCur = readDataChecked(node, v);
                    if (getDataParent(vCur) == v)
                        return false; // we've already checked par != v
                    freeze = false;
                } else {
                    u = v;
                    v = par;
                    freeze = true;
                    continue;
                }
            } else {
                if (!EnableCompaction) {
                    if (!isDataOwner(localDat, node)) {
                        // copy non-root vertex to local memory
                        mThisNodeWrite.inc(1);
                        data[node][u].store(mixDataOwner(parDat, node));
                    }
                    if (!isDataOwner(localParDat, node)) {
                        mThisNodeWrite.inc(1);
                        data[node][par].store(mixDataOwner(grandDat, node));
                    }
                    u = grand;
                    continue;
                }

                if (isDataOwner(localDat, node)) {
                    if (AllowCrossNodeCompression || isDataOwner(localParDat, node)) {
                        // compress local if we know `par`
                        mThisNodeWrite.inc(1);
                        data[node][u].store(mixDataOwner(grandDat, node));
                    } else {
                        u = par;
                        continue;
                    }
                } else {
                    // copy non-root vertex to local memory
                    mThisNodeWrite.inc(1);
                    data[node][u].store(mixDataOwner(grandDat, node));
                }
            }
            if constexpr(Halfing) {
                u = grand;
            } else {
                u = par;
            }
        }
    }

    bool DoSimpleSameSet(int u, int v, DepthStats& uStats, DepthStats& vStats) {
        int node = NUMAContext::CurrentThreadNode();
        // TODO try this optimization with node owners
//        if (data[node][u].load(std::memory_order_relaxed) == data[node][v].load(std::memory_order_relaxed)) {
//            return true;
//        }

        int uDat, vDat;
        u = findLocalOnly(u, node, uDat, uStats.local);
        v = findLocalOnly(v, node, vDat, vStats.local);
        if (u == v)  // ancestors in current node match?
            return true;
        if (isDataOwner(uDat, node) && isDataOwner(vDat, node)) {// found real roots?
            mThisNodeRead.inc(1);
            if (getDataParent(readDataChecked(node, u)) == u) // still root?
                return false;
        }

        // make one step up outside of loop to save local read
        if (!isDataOwner(uDat, node)) {
            mCrossNodeRead.inc(1);
            uDat = readDataUnsafe(getAnyDataOwnerId(uDat), u);
            ++uStats.crossNode;
        }
        if (!isDataOwner(vDat, node)) {
            mCrossNodeRead.inc(1);
            vDat = readDataUnsafe(getAnyDataOwnerId(vDat), v);
            ++vStats.crossNode;
        }

        bool firstIter = true;
        while (true) {
            u = getDataParent(uDat);
            v = getDataParent(vDat);
            if (u == v) {
                return true;
            }
            if (!firstIter && getDataParent(readDataChecked(node, u)) == u) {
                return false;
            }
            uDat = find(u, node, EnableCompaction, uStats.crossNode);
            vDat = find(v, node, EnableCompaction, vStats.crossNode);
            firstIter = false;
        }
    }

    int Find(int u) override {
        size_t depth = 0;
        return getDataParent(find(u, NUMAContext::CurrentThreadNode(), EnableCompaction, depth));
    }

private:
    int findLocalOnly(int u, int node, int& localParDat, size_t& depth) { // returns vertex
        while (true) {
            ++depth;

            mThisNodeRead.inc(1);
            int parDat = readDataUnsafe(node, u);
            if (!isDataOwner(parDat, node)) {
                localParDat = parDat;
                return u;
            }
            int par = getDataParent(parDat);

            if (!DSU::EnableCompaction) {
                if (par == u) {
                    localParDat = parDat;
                    return u;
                }
                u = par;
                continue;
            }

            mThisNodeRead.inc(1);
            int grandDat = readDataUnsafe(node, par);
            if (!isDataOwner(grandDat, node)) {
                localParDat = grandDat;
                ++depth;
                return par;
            }
            int grand = getDataParent(grandDat);
            if (par == grand) {
                localParDat = grandDat;
                return par;
            } else {
                // compress local
                mThisNodeWrite.inc(1);
                data[node][u].store(grandDat);
            }
            if constexpr(Halfing) {
                u = grand;
                ++depth;
            } else {
                u = par;
            }
        }
    }

    int find(int u, int node, bool compressPaths, size_t& depth) {
        if (compressPaths) {
            while (true) {
                ++depth;

                int localDat;
                int parDat = readDataChecked(node, u, localDat);
                int par = getDataParent(parDat);
                int grandDat = readDataChecked(node, par);
                int grand = getDataParent(grandDat);
                if (par == grand) {
                    if (par != u && !isDataOwner(parDat, node)) {
                        // copy non-root vertex to local memory
                        mThisNodeWrite.inc(1);
                        data[node][u].store(mixDataOwner(parDat, node));
                    }
                    return grandDat;
                } else {
                    if (isDataOwner(localDat, node)) {
                        if (AllowCrossNodeCompression || isDataOwner(grandDat, node)) {
                            // compress local if we know about `par`
                            mThisNodeWrite.inc(1);
                            data[node][u].store( mixDataOwner(grandDat, node));
                        } else {
                            u = par; // else do not compress, but go to par even if Halfing enabled
                            continue;
                        }
                    } else {
                        // copy non-root vertex to local memory
                        mThisNodeWrite.inc(1);
                        data[node][u].store(mixDataOwner(grandDat, node));
                    }
                }
                if constexpr(Halfing) {
                    u = grand;
                    ++depth;
                } else {
                    u = par;
                }
            }
        } else {
            while (true) {
                ++depth;

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
