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

private:
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

        head = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        head->store(nullptr);
        tail = (std::atomic<Element*> *) numa_alloc_onnode(sizeof(std::atomic<Element*>), node);
        tail->store(nullptr);
    }

    void Push(std::pair<int, int> p) {
        m.lock();
        Element* e = (Element*) numa_alloc_onnode(sizeof(Element), *node);
        e->Init(p, *node);

        if (tail->load() == nullptr) {
            head->store(e);
            tail->store(e);
        } else {
            tail->load()->SetNext(e);
            tail->store(e);
        }
        m.unlock();
    }

//    std::vector<std::pair<int, int>> List() {
//        m.lock();
//        //std::cerr << "in Queue List \n";
//        std::vector<std::pair<int, int>> result;
//        while (!empty()) {
//            //std::cerr << "in while \n";
//            auto p = pop();
//            if (!p) {
//                break;
//            }
//            //std::cerr << p->first << " " << p->second << " poped \n";
//
//            result.emplace_back(*p);
//        }
//        m.unlock();
//        return result;
//    }

    std::pair<int, int>* Pop() {
        //std::cerr << "in pop \n";
        m.lock();
        if (head->load() == nullptr) {
            m.unlock();
            return nullptr;
        }

        auto e = (std::pair<int, int> *) numa_alloc_onnode(sizeof(std::pair<int, int>), *node);

        //auto e = new std::pair<int, int>(*head->load()->GetFirst(), *head->load()->GetSecond());
        e->first = *head->load()->GetFirst();
        e->second = *head->load()->GetSecond();

        head->store(head->load()->GetNext());
        if (head->load() == nullptr) {
            tail->store(nullptr);
        }
        m.unlock();

        return e;
    }

private:
    bool empty() {
        if (head->load() == nullptr) {
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
