#pragma once

#include "workload.hpp"
#include "parameters.hpp"

#include <vector>

class WorkloadProvider {
public:
    virtual StaticWorkload MakeWorkload(NUMAContext* ctx,
                                        const ParameterSet& params) = 0;

    virtual std::string_view Name() const = 0;

    virtual std::vector<std::string> GetParameterNames() const = 0;

    virtual ParameterSet GetDefaultParameters() const = 0;

    virtual ~WorkloadProvider() {}
};
