#ifndef TRY_DSU_H
#define TRY_DSU_H

#include "lib/numa.hpp"

#include <sched.h>
#include <thread>
#include <atomic>
#include <vector>
#include <numa.h>
#include <mutex>

class DSU {
public:
    explicit DSU(NUMAContext* ctx)
            : Ctx_(ctx) {}

    DSU()
        : DSU(nullptr) {}

    virtual std::string ClassName() = 0;
    virtual void ReInit() = 0;
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;
    virtual ~DSU() = default;

protected:
    NUMAContext* Ctx_;
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