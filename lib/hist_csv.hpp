#pragma once

#include "metrics.hpp"
#include "csv.hpp"


class HistCsvFile {
public:
    HistCsvFile(CsvFile file)
        : Csv_(std::move(file))
    {
        Csv_ << "index" << "metric" << "point" << "value";
    }

    HistCsvFile& Write(const std::string& name, const Histogram& hist) {
        size_t index = Index_++;
        for (size_t i = 0; i < hist.data().size(); ++i) {
            Csv_ << index << name << i << hist[i];
        }
        return *this;
    }

    HistCsvFile& Write(const HistMetrics& hists) {
        for (const auto& [name, hist] : hists.data()) {
            Write(name, hist);
        }
        return *this;
    }

    HistCsvFile& operator <<(const HistMetrics& hists) {
        return Write(hists);
    }

    size_t GetNextIndex() const {
        return Index_;
    }

private:
    CsvFile Csv_;
    size_t Index_ = 0;
};