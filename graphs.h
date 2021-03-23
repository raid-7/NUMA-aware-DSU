#ifndef TRY_GRAPHS_H
#define TRY_GRAPHS_H

void shuffle(std::vector<std::pair<int, int>>* edges) {
    std::random_device rd;
    std::mt19937 q(rd());
    std::shuffle(edges->begin(), edges->end(), q);
}

std::vector<std::pair<int, int>>* graphRandom(int N, int E) {
    auto g = new std::vector<std::pair<int, int>>();

    for (int i = 0; i < E ; i++) {
        int x = rand() % N;
        int y = rand() % N;
        g->emplace_back(std::make_pair(x, y));
    }

    return g;
}

std::vector<std::pair<int, int>>* graphFromFile(std::string filename) {
    std::ifstream file;
    file.open(filename);

    int N, E;
    file >> N >> E;
    auto g = new std::vector<std::pair<int, int>>();

    int a, b;
    char c;

    if (filename[0] == 'W') {
        for (int i = 0; i < E; i++) {
            file >> c;
            file >> a >> b;
            N = std::max(N, std::max(a, b));
            g->emplace_back(std::make_pair(a, b));
            file >> a;
        }
    } else {
        for (int i = 0; i < E; i++) {
            file >> a >> b;
            N = std::max(N, std::max(a, b));
            g->emplace_back(std::make_pair(a, b));
        }
    }

    shuffle(g);
    return g;
}

#endif //TRY_GRAPHS_H
