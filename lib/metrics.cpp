#include "metrics.hpp"


Metrics operator +(const Metrics& a, const Metrics& b) {
    Metrics res;
    res += a;
    res += b;
    return res;
}

std::ostream& operator <<(std::ostream& stream, const Metrics& metrics) {
    for (auto [key, value] : metrics.metrics) {
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
    for (auto value : hist.hist()) {
        stream << value << " ";
    }
    return stream;
}
