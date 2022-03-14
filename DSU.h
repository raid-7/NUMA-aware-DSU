#ifndef TRY_DSU_H
#define TRY_DSU_H

#include <sched.h>
#include <thread>
#include <atomic>
#include <vector>
#include <numa.h>

class DSU {
public:
    virtual std::string ClassName() = 0;
    virtual void ReInit() = 0;
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;

    // для дебаг-целей
    virtual long long getStepsCount() = 0;
    virtual void setStepsCount(int x) = 0;

    int getNode() {
        thread_local static int node = numa_node_of_cpu(sched_getcpu());
        return node;
    }
};

#endif //TRY_DSU_H