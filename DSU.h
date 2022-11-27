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
    virtual std::string ClassName() = 0;
    virtual void ReInit() = 0;
    virtual void Union(int u, int v) = 0;
    virtual int Find(int u) = 0;
    virtual bool SameSet(int u, int v) = 0;
};

#endif //TRY_DSU_H