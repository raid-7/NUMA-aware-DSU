#include "metrics.hpp"



std::ostream& operator <<(std::ostream& stream, const Metrics& metrics) {
    for (auto [key, value] : metrics.data()) {
        stream << key << ": " << value << '\n';
    }
    return stream;
}


Histogram operator +(const Histogram& a, const Histogram& b) {
    Histogram res;
    res += a;
    res += b;
    return res;
}

std::ostream& operator <<(std::ostream& stream, const Histogram& hist) {
    for (auto value : hist.data()) {
        stream << value << " ";
    }
    return stream;
}
