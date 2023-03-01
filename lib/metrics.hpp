#pragma once

#include "stats.hpp"
#include "numa.hpp"

#include <unordered_map>
#include <string>
#include <numeric>
#include <vector>
#include <ostream>
#include <mutex>


constexpr size_t METRIC_STRIDE = 4; // to reduce false sharing

template <class V>
class BaseMetrics {
private:
    std::unordered_map<std::string, V> metrics{};

public:
    using Value = V;

    V& operator [](const std::string& metric) {
        return metrics[metric];
    }

    const V& operator [](const std::string& metric) const {
        auto it = metrics.find(metric);
        if (it == metrics.end())
            return 0;
        return it->second;
    }

    const std::unordered_map<std::string, V>& data() const {
        return metrics;
    }

    void reset() {
        metrics.clear();
    }

    BaseMetrics<V>& operator +=(const BaseMetrics<V>& other) {
        for (auto [key, value] : other.metrics) {
            metrics[key] += value;
        }
        return *this;
    }
};

template <class V>
BaseMetrics<V> operator +(const BaseMetrics<V>& a, const BaseMetrics<V>& b) {
    BaseMetrics<V> res;
    res += a;
    res += b;
    return res;
}

using Metrics = BaseMetrics<size_t>;

std::ostream& operator <<(std::ostream& stream, const Metrics& metrics);

template <class It>
std::unordered_map<std::string, Stats<long double>> metricStats(const It begin, const It end) {
    std::unordered_map<std::string, std::vector<long double>> data;
    for (It it = begin; it != end; ++it) {
        for (auto [key, value] : it->data()) {
            data[key].push_back(static_cast<long double>(value));
        }
    }

    std::unordered_map<std::string, Stats<long double>> res;
    for (const auto& [key, values] : data) {
        res[key] = stats(values.begin(), values.end());
    }
    return res;
}

class Histogram {
public:
    explicit Histogram(std::vector<size_t> data = {})
        : hist(std::move(data)) {
        auto lastNonZero = std::find_if(hist.rbegin(), hist.rend(), [](size_t x) {
            return x > 0;
        });

        hist.erase(lastNonZero.base(), hist.end());

        if (hist.empty())
            hist.resize(1, 0);
    }

    size_t operator[](size_t value) const {
        return hist.empty() ? 0 : value >= hist.size() ? hist.back() : hist[value];
    }

    Histogram& operator +=(const Histogram& oth) {
        if (oth.hist.size() > hist.size()) {
            hist.resize(oth.hist.size(), 0);
        }
        for (size_t i = 0; i < oth.hist.size(); ++i) {
            hist[i] += oth.hist[i];
        }
        return *this;
    }

    const std::vector<size_t>& data() const {
        return hist;
    }

private:
    std::vector<size_t> hist;
};

Histogram operator +(const Histogram& a, const Histogram& b);

std::ostream& operator <<(std::ostream& stream, const Histogram& metrics);

using HistMetrics = BaseMetrics<Histogram>;

class MetricsCollector {
private:
    std::unordered_map<std::string, std::vector<size_t>> allMetrics;
    std::unordered_map<std::string, std::vector<std::vector<size_t>>> allHistMetrics;
    std::mutex mutex;
    size_t numThreads;

public:
    class Accessor {
    private:
        size_t* tlMetrics;

    public:
        explicit Accessor(size_t* tlMetrics)
                :tlMetrics(tlMetrics) {}

        void inc(const size_t value, int tid = NUMAContext::CurrentThreadId()) const {
            if (tlMetrics)
                tlMetrics[tid * METRIC_STRIDE] += value;
        }

        size_t get(int tid = NUMAContext::CurrentThreadId()) const {
            return tlMetrics[tid * METRIC_STRIDE];
        }
    };

    class HistAccessor {
    private:
        std::vector<size_t>* tlMetrics;
        size_t maxValue;

    public:
        explicit HistAccessor(std::vector<size_t>* tlMetrics, size_t max)
                : tlMetrics(tlMetrics), maxValue(max) {}

        void inc(const size_t value, int tid = NUMAContext::CurrentThreadId()) const {
            if (tlMetrics)
                tlMetrics[tid][value < maxValue ? value : maxValue] += 1;
        }
    };

    explicit MetricsCollector(size_t numThreads)
        :numThreads(numThreads) {}
    MetricsCollector(const MetricsCollector&) = delete;
    MetricsCollector(MetricsCollector&&) = delete;
    MetricsCollector& operator=(const MetricsCollector&) = delete;
    MetricsCollector& operator=(MetricsCollector&&) = delete;

    Accessor accessor(std::string metric) {
        if (!numThreads)
            return Accessor(nullptr);
        std::lock_guard lock(mutex);
        std::vector<size_t>& tlMetrics = allMetrics[std::move(metric)];
        if (tlMetrics.size() < numThreads * METRIC_STRIDE)
            tlMetrics.resize(numThreads * METRIC_STRIDE, 0);
        return Accessor(tlMetrics.data());
    }

    HistAccessor histAccessor(std::string metric, size_t max) {
        if (!numThreads)
            return HistAccessor(nullptr, 0);
        std::lock_guard lock(mutex);
        std::vector<std::vector<size_t>>& tlMetrics = allHistMetrics[std::move(metric)];
        if (tlMetrics.size() < numThreads)
            tlMetrics.resize(numThreads, {});
        for (size_t tid = 0; tid < numThreads; ++tid) {
            tlMetrics[tid].resize(max);
        }
        return HistAccessor(tlMetrics.data(), max);
    }

    Metrics combine() {
        Metrics res;
        std::lock_guard lock(mutex);
        for (const auto& [key, tlMetrics] : allMetrics) {
            res[key] = std::reduce(tlMetrics.begin(), tlMetrics.end());
        }
        return res;
    }

    HistMetrics combineHist() {
        HistMetrics res;
        std::lock_guard lock(mutex);
        for (const auto& [key, tlMetrics] : allHistMetrics) {
            for (const auto& vec : tlMetrics) {
                res[key] += Histogram(vec);
            }
        }
        return HistMetrics{res};
    }

    void reset(int tid) {
        std::lock_guard lock(mutex);
        for (auto& [key, tlMetrics] : allMetrics) {
            tlMetrics[tid * METRIC_STRIDE] = 0;
        }
        for (auto& [key, tlMetrics] : allHistMetrics) {
            std::fill(tlMetrics[tid].begin(), tlMetrics[tid].end(), 0);
        }
    }

    void reset() {
        std::lock_guard lock(mutex);
        for (auto& [key, tlMetrics] : allMetrics) {
            for (auto& metricValue : tlMetrics)
                metricValue = 0;
        }
        for (auto& [key, tlMetrics] : allHistMetrics) {
            for (auto& metricValues : tlMetrics)
                std::fill(metricValues.begin(), metricValues.end(), 0);
        }
    }
};

class MetricsAwareBase {
private:
    MetricsCollector collector;

protected:
    MetricsCollector::Accessor accessor(std::string metric) {
        return collector.accessor(std::move(metric));
    }

    MetricsCollector::HistAccessor histogram(std::string metric, size_t max) {
        return collector.histAccessor(std::move(metric), max);
    }

public:
    explicit MetricsAwareBase(size_t numThreads)
        : collector(numThreads) {}

    Metrics collectMetrics() {
        return collector.combine();
    }

    HistMetrics collectHistMetrics() {
        return collector.combineHist();
    }

    void resetMetrics(int tid) {
        collector.reset(tid);
    }

    void resetMetrics() {
        collector.reset();
    }
};
