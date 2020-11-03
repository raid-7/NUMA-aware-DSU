#ifndef TRY_QUEUE_H
#define TRY_QUEUE_H

#include <queue>
#include <mutex>

class Element {
public:

    void Init(std::pair<int, int> p, int node) {
        first = (int*) numa_alloc_onnode(sizeof(int), node);
        *first = p.first;
        second = (int*) numa_alloc_onnode(sizeof(int), node);
        *second = p.second;
        next = this;
    }

    void SetNext(Element* e) {
        next.store(e);
    }

    int* GetFirst() {
        return first;
    }

    int* GetSecond() {
        return second;
    }

    Element* GetNext() {
        return next.load();
    }

    ~Element() {
        numa_free(first, sizeof(int));
        numa_free(second, sizeof(int));
    }

private:
    int* first;
    int* second;
    std::atomic<Element*> next;
};

// какая-то очень быстро написанная очередь
// ее надо бы проверить и пофиксить как минимум мьютекс
class Queue {
public:
    void Init(int node) {
        this->node = node;
        head = nullptr;
        tail = head;
    }

    void Push(std::pair<int, int> p) {
        m.lock();
        auto e = (Element*) numa_alloc_onnode(sizeof(Element), node);
        e->Init(p, node);
        tail->SetNext(e);
        tail = e;
        m.unlock();
    }

    std::vector<std::pair<int, int>> List() {
        m.lock();
        std::vector<std::pair<int, int>> result;
        while (!empty()) {
            auto p = pop();
            result.emplace_back(std::make_pair(*p->GetFirst(), *p->GetSecond()));
        }
        m.unlock();
        return result;
    }

private:
    Element* pop() {
        m.lock();
        auto e = head;
        head = head->GetNext();
        m.unlock();
        return e;
    }

    bool empty() {
        if (head == nullptr) {
            return true;
        }
        return false;
    }

    int node;
    Element* head;
    Element* tail;
    std::mutex m;
};

#endif //TRY_QUEUE_H
