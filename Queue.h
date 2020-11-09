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
        next = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node) ;
        next->store(nullptr);
    }

    void SetNext(Element* e) {
        next->store(e);
    }

    int* GetFirst() {
        return first;
    }

    int* GetSecond() {
        return second;
    }

    Element* GetNext() {
        return next->load();
    }

    ~Element() {
        numa_free(first, sizeof(int));
        numa_free(second, sizeof(int));
        numa_free(next, sizeof(std::atomic<Element*>));
    }

public:
    int* first;
    int* second;
    std::atomic<Element*>* next;
};

// какая-то очень быстро написанная очередь
// ее надо бы проверить и пофиксить как минимум мьютекс
class Queue {
public:
    void Init(int node) {
        this->node = (int*) numa_alloc_onnode(sizeof(int), node);
        *this->node = node;

        auto fake = (Element*) numa_alloc_onnode(sizeof(Element), node);
        fake->Init(std::make_pair(0, 0), node);

        head = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        head->store(fake);
        tail = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        tail->store(fake);
    }

    void Push(std::pair<int, int> p) {
        auto e = (Element*) numa_alloc_onnode(sizeof(Element), *node);
        e->Init(p, *node);

        Element* null = nullptr;
        while (true) {
            auto t = tail->load();
            auto next = t->GetNext();
            if (t->next->compare_exchange_weak(null, e)) {
                tail->compare_exchange_weak(t, e);
                break;
            } else {
                tail->compare_exchange_weak(t, t->GetNext());
            }
        }
    }

    std::pair<int, int>* Pop() {
        //std::cerr << "in pop \n";
        while (true) {
            auto h = head->load();
            auto t = tail->load();
            auto first = h->GetNext();

            if (h == t) {
                if (first == nullptr) {
                    return nullptr;
                } else {
                    tail->compare_exchange_weak(t, t->GetNext());
                }
            } else {
                auto e = (std::pair<int, int> *) numa_alloc_onnode(sizeof(std::pair<int, int>), *node);
                e->first = *first->GetFirst();
                e->second = *first->GetSecond();
                if (head->compare_exchange_weak(h, first)) {
                    return e;
                }
            }
        }
    }

private:
    int* node;
    std::atomic<Element*>* head;
    std::atomic<Element*>* tail;
    //std::mutex m;
};

#endif //TRY_QUEUE_H
