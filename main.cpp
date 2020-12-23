#include <iostream>
#include <cassert>
#include <chrono>
#include "sched.h"

// найти больше функций или просто почитать можно тут: https://linux.die.net/man/3/numa
#include <numa.h>
#include <atomic>

void membind_VS_mems_allowed() {
    std::cout << "membind_VS_mems_allowed" << std::endl;
    auto mask1 = numa_get_membind();
    for (int i = 0; i < int(mask1->size); i++) {
        std::cout <<  numa_bitmask_isbitset(mask1, i);
    }
    std::cout << std::endl;

    auto mask2 = numa_get_mems_allowed();
    for (int i = 0; i < int(mask2->size); i++) {
        std::cout << numa_bitmask_isbitset(mask2, i);
    }
    std::cout << std::endl << std::endl;

    // насколько я понимаю, начальное состояние этих масок одинаковое
    // membind-маску можно менять ручками через numa_set_membind()
    // в доке можно почитать более развернутую вресию и без тени моей интерпретации
}

// проверяем, что выполняемся на ноде с номером id
void checkRunningNode(int id) {
    auto m = numa_get_run_node_mask();
    assert(numa_bitmask_isbitset(m, id));
    for (int i = 0; i < int(m->size); i++) {
        if (i != id) {
            assert(!numa_bitmask_isbitset(m, i));
        }
    }

    // проверим работоспособность определения ноды по cpu
    assert(numa_node_of_cpu(sched_getcpu()) == id);
}

// заполняем массив чем-нибудь
void fill(int N, std::atomic_int* data) {
    for (int i = 0; i < N; i++) {
        data[i].store(1);
    }
}

// просто тыкаемся в каждую ячейку, не надо смотреть на result
void process(int N, std::atomic_int* data, volatile int * result) {
    for (int i = 0; i < N; i++) {
        *result += data[i].load();
    }
}


// Тест: выделим на каждой ноде большой массив и проверим время доступа к локальной и не локальной памяти из ноды node
void test(int node) {
    std::cout << "Run test from node " << node << std::endl;
    const int N = 1e8;

    // будем выполняться на ноде с номером node
    numa_run_on_node(node);
    checkRunningNode(node);

    // проверим, что мы можем аллоцировать память на нодах
    // numa_get_membind возвращает маску по нодам
    auto getMemBindMask = numa_get_membind();
    for (int i = 0; i < int(getMemBindMask->size); i++) {
        if (numa_bitmask_isbitset(getMemBindMask, i)) {
            long freep;
            numa_node_size(i, &freep);
            std::cout << "Free mem on node " << i << " " << freep << " ";
            // Главная функция для аллокации памяти на ноде. Аллоцирует сколько надо памяти на данной ноде
            auto data = (std::atomic_int*) numa_alloc_onnode(sizeof(std::atomic_int) * N, i);
            numa_node_size(i, &freep);
            std::cout << "then after allocation " << i << " " << freep << " ";

            // Замеряем время работы fill+process
            // Несколько (runs) раз и берем среднее
            int runs = 5;
            float resultTimeSum = 0;

            // process считает сумму на массиве, в processresult будет записываться ответ
            volatile int * processResult = (volatile int*) numa_alloc_onnode(sizeof(volatile int), i);
            *processResult = 0;

            // bind привязывает поток к ноде
            // выполнение и аллокации теперь будут происходить на ноде node
            bitmask* bindMaskToRun = numa_bitmask_alloc(getMemBindMask->size);
            numa_bitmask_setbit(bindMaskToRun, node);
            numa_bind(bindMaskToRun);
            for (int j = 0; j < runs; j++) {
                auto start = std::chrono::high_resolution_clock::now();
                {
                    fill(N, data);
                    process(N, data, processResult);
                }
                auto stop = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
                resultTimeSum += duration.count();
            }
            numa_node_size(i, &freep);
            std::cout << "and after fill " << i << " " << freep << std::endl;

            std::cout << "Time for node " << i << ": " << resultTimeSum / runs << std::endl;

            // и надо освободить память!
            numa_free(data, sizeof(std::atomic_int) * N);
            numa_free((void*)processResult, sizeof(volatile int));

            //откатываемся к начальному состоянию
            numa_bind(getMemBindMask);
        }
    }
}


int main() {
    // проверяет все и в порядке с нумой в месте, где запускаем
    // -1 = не в порядке. и все остальные оперции будут работать как-то
    if (numa_available() != -1) {
        // numa_max_possible_node() returns the number of the highest possible node in a system.
        // In other words, the size of a kernel type nodemask_t (in bits) minus 1
        std::cout << "Number of nodes in the system: " << numa_num_possible_nodes() << std::endl;
        // вместо запроса числа нод можно спросить максимальный номер ядра
        // не знаю зачем это может быть нужно
        // +1, тк нумерация с нуля
        assert(numa_num_possible_nodes() == numa_max_possible_node() + 1);

        // то, что обычно нужно
        std::cout << "Number of available nodes: " << numa_num_configured_nodes() << std::endl;

        // сравним работу двух фоункций, проверяющих доступность памяти для аллокации
        // membind_VS_mems_allowed();


        for (int i = 0; i < numa_num_configured_nodes(); i++) {
            test(i);
        }
    }

    return 0;
}
