#include <fstream>
#include <filesystem>
#include <sstream>


class MtxReader { // accepts `matrix coordinate pattern symmetric`
public:
    MtxReader(const std::filesystem::path& file)
        : input_(file)
    {}

    inline std::pair<size_t, size_t> readSize() {
        std::string line;
        do {
            std::getline(input_, line);
        } while ((line.starts_with('%')  || line.starts_with('#') || line.empty()) && !input_.eof());
        if (line.empty())
            return {0, 0};
        std::istringstream lineIn(line);
        size_t n = 0, m = 0;
        lineIn >> n >> m;
        return {n, m};
    }

    template <class Func>
    inline void readEdges(Func&& f) {
        std::string line;
        do {
            std::getline(input_, line);
            if (line.empty() || line.starts_with('%') || line.starts_with('#'))
                continue;
            std::istringstream lineIn(line);
            size_t n = 0, m = 0;
            lineIn >> n >> m;
            f(n, m);
        } while (!input_.eof());
    }

private:
    std::ifstream input_;
};
