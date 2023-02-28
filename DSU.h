#ifndef TRY_DSU_H
#define TRY_DSU_H

#include "lib/numa.hpp"
#include "lib/metrics.hpp"

#include <sched.h>
#include <thread>
#include <atomic>
#include <vector>
#include <numa.h>
#include <mutex>

class DSU : public MetricsAwareBase {
public:
    static bool EnableMetrics;
    static bool EnableCompaction;

    explicit DSU(NUMAContext* ctx, [[maybe_unused]] size_t numThreads = 0)
            : MetricsAwareBase(EnableMetrics ? std::max(numThreads, (size_t)std::thread::hardware_concurrency()) : 0)
            , Ctx_(ctx) {}

    DSU()
        : DSU(nullptr) {}

    virtual std::string ClassName() = 0;
    virtual void ReInit() = 0;
    virtual void DoUnion(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool DoSameSet(int u, int v) = 0;
    virtual ~DSU() = default;

    void Union(int u, int v) {
        size_t beforeOpCNRead = mCrossNodeRead.get();
        size_t beforeOpCNWrite = mCrossNodeWrite.get();
        DoUnion(u, v);
        mHistCrossNodeRead.inc(mCrossNodeRead.get() - beforeOpCNRead);
        mHistCrossNodeWrite.inc(mCrossNodeWrite.get() - beforeOpCNWrite);
        mUnionRequests.inc(1);
    }

    bool SameSet(int u, int v) {
        size_t beforeOpCNRead = mCrossNodeRead.get();
        size_t beforeOpCNWrite = mCrossNodeWrite.get();
        bool r = DoSameSet(u, v);
        mHistCrossNodeRead.inc(mCrossNodeRead.get() - beforeOpCNRead);
        mHistCrossNodeWrite.inc(mCrossNodeWrite.get() - beforeOpCNWrite);
        if (r) {
            mSameSetRequestsTrue.inc(1);
        } else {
            mSameSetRequestsFalse.inc(1);
        }
        return r;
    }

protected:
    NUMAContext* Ctx_;
    MetricsCollector::Accessor mCrossNodeRead = accessor("cross_node_read");
    MetricsCollector::Accessor mCrossNodeWrite = accessor("cross_node_write");
    MetricsCollector::Accessor mThisNodeRead = accessor("this_node_read");
    MetricsCollector::Accessor mThisNodeReadSuccess = accessor("this_node_read_success");
    MetricsCollector::Accessor mThisNodeWrite = accessor("this_node_write");
    MetricsCollector::Accessor mGlobalDataAccess = accessor("global_data_read_write");

    MetricsCollector::HistAccessor mHistCrossNodeRead = histogram("hist_cross_node_read", 1000);
    MetricsCollector::HistAccessor mHistCrossNodeWrite = histogram("hist_cross_node_write", 1000);

private:
    MetricsCollector::Accessor mSameSetRequestsTrue = accessor("same_set_requests_true");
    MetricsCollector::Accessor mSameSetRequestsFalse = accessor("same_set_requests_false");
    MetricsCollector::Accessor mUnionRequests = accessor("union_requests");
};


// utility for adaptive implementations

template<int maxNumaNodes>
static constexpr std::array<int, 1 << maxNumaNodes> makeOwnerLookupTable() {
    std::array<int, 1 << maxNumaNodes> res; // NOLINT(cppcoreguidelines-pro-type-member-init)
    res[0] = -1;
    for (int i = 0; i < maxNumaNodes; ++i)
        res[1 << i] = i;
    for (int i = 1; i < (int) res.size(); ++i)
        res[i] = res[i & -i];
    return res;
}

#endif //TRY_DSU_H