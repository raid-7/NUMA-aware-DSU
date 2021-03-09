#ifndef TRY_DSU_H
#define TRY_DSU_H

#include <sched.h>
#include <thread>
#include <numa.h>
#include <atomic>
#include <vector>

class DSU {
public:
    virtual void ReInit() = 0;
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;
};

#endif //TRY_DSU_H