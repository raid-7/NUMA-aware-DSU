#pragma once

#include "../DSU.h"

#include <vector>
#include <any>

struct Request {
    bool SameSetRequest;
    int u, v;

    void Apply(DSU* dsu) const {
        if (SameSetRequest) {
            Blackhole(dsu->SameSet(u, v));
        } else {
            dsu->Union(u, v);
        }
    }
};

struct StaticWorkload {
    std::vector<Request> PreHeatRequests;
    std::vector<std::vector<Request>> ThreadRequests;
    size_t N;
    std::vector<std::any> Metadata;

    template <class T>
    const T& GetMeta() const {
        auto it = std::find_if(Metadata.begin(), Metadata.end(), [](const auto& v) {
            return static_cast<bool>(std::any_cast<T>(&v));
        });
        if (it == Metadata.end())
            throw std::runtime_error("No such metadata");
        return *std::any_cast<T>(&*it);
    }
};

struct ComponentMappingMd {
    std::vector<int> Mapping;
};
