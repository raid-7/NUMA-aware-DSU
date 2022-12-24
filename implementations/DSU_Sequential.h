#include "../DSU.h"

class DSU_Sequential : public DSU {
public:
    DSU_Sequential(int size) {
        this->size = size;
        data.resize(size);
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
    }

    void ReInit() override {
        data.resize(size);
        for (int i = 0; i < size; i++) {
            data[i] = i;
        }
    }

    void DoUnion(int u, int v) override {
        int u_p = Find(u);
        int v_p = Find(v);
        if (u_p == v_p) {
            return;
        }
        if (rand() % 2) {
            data[u_p] = v_p;
        } else {
            data[v_p] = u_p;
        }
    }

    int Find(int u) override {
        int cur = u;
        while (data[cur] != cur) {
            auto par = data[cur];
            data[cur] = data[par];
            cur = par;
        }
        return cur;
    }

    bool DoSameSet(int u, int v) override {
        return Find(u) == Find(v);
    }

private:
    int size;
    std::vector<int> data;
};