#include <iostream>
#include <cassert>

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
}

// Тест: выделим память на каждой ноде и проверим скорость доступа к локальной и не локальной памяти процессора
void test() {
    // проверим, что мы можем аллоцировать память на нодах
    // numa_get_membind возвращает маску по нодам

    //auto mask = numa_get_membind();
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
        assert(numa_num_possible_nodes() == numa_max_possible_node() - 1);

        // то, что обычно нужно
        std::cout << "Number of available nodes: " << numa_num_configured_nodes() << std::endl;

        // сравним работу двух фоункций, проверяющих доступность памяти
        membind_VS_mems_allowed();



        test();
    }

    return 0;
}
