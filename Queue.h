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
        //next = (Element*) numa_alloc_onnode(sizeof(Element*), node);
        //next.store(nullptr);
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
    }

private:
    int* first;
    int* second;
    std::atomic<Element*>* next;
    //Element* next;
};

// какая-то очень быстро написанная очередь
// ее надо бы проверить и пофиксить как минимум мьютекс
class Queue {
public:
    void Init(int node) {
        this->node = (int*) numa_alloc_onnode(sizeof(int), node);
        *this->node = node;

        head = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        head->store(nullptr);
        tail = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        tail->store(nullptr);
    }

    void Push(std::pair<int, int> p) {
        m.lock();
        Element* e = (Element*) numa_alloc_onnode(sizeof(Element), *node);
        e->Init(p, *node);

        if (!tail) {
            head->store(e);
            tail->store(e);
        } else {
            tail->load()->SetNext(e);
            tail->store(e);
        }
        m.unlock();
    }

    std::vector<std::pair<int, int>> List() {
        m.lock();
        //std::cerr << "in Queue List \n";
        std::vector<std::pair<int, int>> result;
        while (!empty()) {
            //std::cerr << "in while \n";
            auto p = pop();
            if (!p) {
                break;
            }
            //std::cerr << p->first << " " << p->second << " poped \n";

            result.emplace_back(*p);
        }
        m.unlock();
        return result;
    }

private:
    std::pair<int, int>* pop() {
        //std::cerr << "in pop \n";
        if (!head) {
            return nullptr;
        }

        auto e = new std::pair<int, int>(*head->load()->GetFirst(), *head->load()->GetSecond());
        head->store(head->load()->GetNext());
        if (!head) {
            tail = nullptr;
        }
        return e;
    }

    bool empty() {
        if (!head ) {
            return true;
        }
        return false;
    }

    int* node;
    std::atomic<Element*>* head;
    std::atomic<Element*>* tail;
    std::mutex m;
};

#endif //TRY_QUEUE_H
