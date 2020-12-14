#include <iostream>
#include <cassert>
#include <chrono>

// найти больше функций или просто почитать можно тут: https://linux.die.net/man/3/numa
#include <numa.h>

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
}

void fill(int N, int* data) {
    for (int i = 0; i < N; i++) {
        data[i] = std::rand() % 10;
    }
}

int process(int N, int* data) {
    int result = 0;
    for (int i = 0; i < N; i++) {
        result += data[i];
    }
    return result;
}

// Тест: выделим на каждой ноде большой массив и проверим скорость доступа к локальной и не локальной памяти из ноды id
void test(int id) {
    std::cout << "Run test from node " << id << std::endl;
    const int N = 1e8;
    // будем выполняться на первой ноде
    numa_run_on_node(id);
    checkRunningNode(id);

    // проверим, что мы можем аллоцировать память на нодах
    // numa_get_membind возвращает маску по нодам
    auto mask = numa_get_membind();
    for (int i = 0; i < int(mask->size); i++) {
        if (numa_bitmask_isbitset(mask, i)) {
            auto data = (int*) numa_alloc_onnode(sizeof(int) * N, i);
            auto start = std::chrono::high_resolution_clock::now();
            {
                fill(N, data);
                process(N, data);
            }
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
            std::cout << "Time for node " << i << ": " << duration.count() << std::endl;
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
