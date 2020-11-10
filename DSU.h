#ifndef TRY_DSU_H
#define TRY_DSU_H

#include <sched.h>
#include <thread>
#include <numa.h>
#include <atomic>
#include <vector>

#include "Queue.h"

class DSU{
public:
    DSU(int size, int node_count) : size(size), node_count(node_count) {
        data.resize(node_count);
        queues.resize(node_count);
        for (int i = 0; i < node_count; i++) {
            data[i] = (std::atomic<int> *) numa_alloc_onnode(sizeof(std::atomic<int>) * size, i);
            //data[i] = (int*) numa_alloc_onnode(sizeof(int) * size, i);
            for (int j = 0; j < size; j++) {
                //data[i][j] = j;
                data[i][j].store(j);
            }

            queues[i] = (Queue*) numa_alloc_onnode(sizeof(Queue), i);
            queues[i]->Init(i);
        }
    }

    ~DSU() {
        for (int i = 0; i < node_count; i++) {
            numa_free(data[i], sizeof(int) * size);
            numa_free(queues[i], sizeof(Queue));
        }
    }

    void Union(int u, int v) {
        //std::string output = std::to_string(sched_getcpu()) + " in union \n";
        //std::cerr << output;
        m.lock();
        if (node_count > 1) {
            auto cpu = sched_getcpu();
            auto node = numa_node_of_cpu(cpu);
            //std::cerr << sched_getcpu() << " " << "got cpu and node \n";
            for (int i = 0; i < node_count; i++) {
                if (i == node)
                    continue;
                //std::cerr << sched_getcpu() << " " << "want to push in queue \n";
                queues[i]->Push(std::make_pair(u, v));
            }

            union_(u, v, node);
        } else {
            union_(u, v, 0);
        }


        m.unlock();
    }

    bool SameSet(int u, int v) {
        if (node_count > 1) {
            auto cpu = sched_getcpu();
            auto node = numa_node_of_cpu(cpu);
            m.lock();
            old_unions(node);
            m.unlock();
            return find(u, node) == find(v, node);
        } else {
            return find(u, 0) == find(v, 0);
        }
    }

    int Find(int u) {
        if (node_count > 1) {
            auto cpu = sched_getcpu();
            auto node = numa_node_of_cpu(cpu);
            m.lock();
            old_unions(node);
            m.unlock();
            return find(u, node);
        } else {
            return find(u, 0);
        }
    }

    bool __SameSetOnNode(int u, int v, int node) {
        m.lock();
        old_unions(node);
        auto res = (find(u, node) == find(v, node));
        m.unlock();
        return res;
    }

private:
    void old_unions(int node) {
        while (true) {
            auto p = queues[node]->Pop();
            if (p == nullptr) {
                break;
            }
            union_(p->first, p->second, node);
        }
    }

    int find(int u, int node) {
        //auto par = data[node][u];//.load();
        auto par = data[node][u].load();
        while (par != data[node][par].load()) {
            par = data[node][par].load();
        }
        //std::cerr << sched_getcpu() << " " << "parent found \n";
        return par;
    }

    void union_(int u, int v, int node) {
        int u_p = find(u, node);
        int v_p = find(v, node);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[node][u_p].store(v_p);
            //while (!data[node][u].compare_exchange_weak(u, v)) {}
        } else {
            //while (!data[node][v].compare_exchange_weak(v, u)) {}
            data[node][v_p].store(u_p);// = u_p;
        }
    }

private:
    int size;
    int node_count;
    std::vector<std::atomic<int>*> data;
    //std::vector<int*> data;
    std::vector<Queue*> queues;

    std::mutex m;
};


#endif //TRY_DSU_H
