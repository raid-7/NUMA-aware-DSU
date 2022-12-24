#include "../DSU.h"
#include "../utils/ConcurrencyFreaks/queues/MichaelScottQueue.hpp"
#include "../utils/ConcurrencyFreaks/queues/array/FAAArrayQueue.hpp"
//#include "tbb/concurrent_queue.h"
//
//class DSU_Queue : public DSU {
//public:
//    DSU_Queue(int size, int node_count) : size(size), node_count(node_count) {
//        data.resize(node_count);
//        queues.resize(node_count);
//        for (int i = 0; i < node_count; i++) {
//            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
//            for (int j = 0; j < size; j++) {
//                data[i][j].store(j);
//            }
//
//            auto addr = (tbb::concurrent_queue<__int64_t>*) numa_alloc_onnode(sizeof(tbb::concurrent_queue<__int64_t>), i);
//            //queues[i]->Init(i);
//            queues[i] = new (addr) tbb::concurrent_queue<__int64_t>();
//        }
//    }
//
//    void ReInit() {
//        for (int i = 0; i < node_count; i++) {
//            //numa_free(data[i], sizeof(int) * size);
//            //numa_free(queues[i], sizeof(FAAArrayQueue<std::pair<int, int>>));
//            numa_free(queues[i], sizeof(tbb::concurrent_queue<std::pair<int, int>>));
//        }
//        for (int i = 0; i < node_count; i++) {
//            //data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
//            for (int j = 0; j < size; j++) {
//                data[i][j].store(j);
//            }
//
//            auto addr = (tbb::concurrent_queue<__int64_t>*) numa_alloc_onnode(sizeof(tbb::concurrent_queue<__int64_t>), i);
//            //queues[i]->Init(i);
//            queues[i] = new (addr) tbb::concurrent_queue<__int64_t>();
//        }
//    }
//
//    ~DSU_Queue() {
//        for (int i = 0; i < node_count; i++) {
//            numa_free(data[i], sizeof(int) * size);
//            //numa_free(queues[i], sizeof(FAAArrayQueue<std::pair<int, int>>));
//            //numa_free(queues[i], sizeof(tbb::concurrent_queue<std::pair<int, int>>));
//            numa_free(queues[i], sizeof(tbb::concurrent_queue<__int64_t>));
//        }
//    }
//
//    void DoUnion(int u, int v) override {
//        auto node = numa_node_of_cpu(sched_getcpu());
//        if (node_count == 1) {
//            node = 0;
//        }
//
//        for (int i = 0; i < node_count; i++) {
//            if (i == node) {
//                continue;
//            }
//            //auto p = std::make_pair(u, v);
//            //queues[i]->enqueue(&p, get_tid());
//            //queues[i]->push(std::make_pair(u, v));
//            __int64_t uv = (__int64_t(u) << 32) + v;
//            queues[i]->push(uv);
//        }
//
//        union_(u, v, node);
//    }
//
//    bool DoSameSet(int u, int v) override {
//        auto node = numa_node_of_cpu(sched_getcpu());
//        if (node_count == 1) {
//            node = 0;
//        }
//        return SameSetOnNode(u, v, node);
//    }
//
//    int Find(int u) override {
//        auto node = numa_node_of_cpu(sched_getcpu());
//        if (node_count > 1) {
//            old_unions(node);
//        } else {
//            node = 0;
//        }
//
//        return find(u, node);
//    }
//
//    bool SameSetOnNode(int u, int v, int node) {
//        if (node_count > 1) {
//            old_unions(node);
//        }
//
//        auto u_p = u;
//        auto v_p = v;
//        while (true) {
//            u_p = find(u_p, node);
//            v_p = find(v_p, node);
//            if (u_p == v_p) {
//                return true;
//            }
//            if (data[node][u_p].load(std::memory_order_acquire) == u_p) {
//                return false;
//            }
//        }
//    }
//private:
//    void old_unions(int node) {
//        //std::pair<int, int> p;
//        __int64_t uv;
//        while (true) {
//            //auto p = queues[node]->dequeue(get_tid());
////            if (p == nullptr) {
////                break;
////            }
//            auto done = queues[node]->try_pop(uv);
//            if (!done) {
//                break;
//            }
//            __int64_t u = uv >> 32;
//            __int64_t v = uv - (u << 32);
//            //union_(p.first, p.second, node);
//            union_(u, v, node);
//        }
//    }
//
//    int find(int u, int node) {
//        auto cur = u;
//        while (true) {
//            auto par = data[node][cur].load(std::memory_order_relaxed);
//            auto grand = data[node][par].load(std::memory_order_relaxed);
//            if (par == grand) {
//                return par;
//            } else {
//                data[node][cur].compare_exchange_weak(par, grand);
//            }
//            cur = par;
//        }
//
//    }
//
//    void union_(int u, int v, int node) {
//        int u_p = u;
//        int v_p = v;
//        while (true) {
//            u_p = find(u_p, node);
//            v_p = find(v_p, node);
//            if (u_p == v_p) {
//                return;
//            }
//            if (u_p < v_p) {
//                if (data[node][u_p].compare_exchange_weak(u_p, v_p)) {
//                    return;
//                }
//            } else {
//                if (data[node][v_p].compare_exchange_weak(v_p, u_p)) {
//                    return;
//                }
//            }
//
//        }
//    }
//
//private:
//    int get_tid() {
//        static std::atomic_int counter;
//        counter = 0;
//        thread_local static std::unique_ptr<int> tid = std::make_unique<int>(++counter);
//        return *tid;
//    }
//
//    int size;
//    int node_count;
//    std::vector<std::atomic<int>*> data;
//
//    //std::vector<FAAArrayQueue<std::pair<int, int>>*> queues;
//    std::vector<tbb::concurrent_queue<__int64_t>*> queues;
//};