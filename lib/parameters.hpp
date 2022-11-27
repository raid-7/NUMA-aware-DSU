#pragma once

#include <unordered_map>
#include <string>
#include <string_view>
#include <regex>


template <class T>
T FromString(const std::string&) {
    static_assert(!std::is_same_v<T, T>, "Unimplemented");
}

template <>
int FromString<int>(const std::string& v) {
    return std::stoi(v);
}

template <>
unsigned long FromString<unsigned long>(const std::string& v) {
    return std::stoul(v);
}

template <>
unsigned long long FromString<unsigned long long>(const std::string& v) {
    return std::stoull(v);
}

template <>
long FromString<long>(const std::string& v) {
    return std::stol(v);
}

template <>
long long FromString<long long>(const std::string& v) {
    return std::stoll(v);
}

template <>
float FromString<float>(const std::string& v) {
    return std::stof(v);
}

template <>
double FromString<double>(const std::string& v) {
    return std::stod(v);
}

template <>
bool FromString<bool>(const std::string& v) {
    return !v.empty() && v != "0" && v != "false" && v != "False" && v != "FALSE";
}

template <>
std::string FromString<std::string>(const std::string& v) {
    return v;
}



class ParameterSet {
public:
    explicit ParameterSet(std::unordered_map<std::string, std::string> params,
                          const ParameterSet* defaults = nullptr)
        : params_(std::move(params))
        , defaults_(defaults)
    {}

    template<class T>
    T Get(const std::string& key) const {
        auto it = params_.find(key);
        if (it == params_.end()) {
            if (!defaults_) {
                using namespace std::string_literals;
                throw std::runtime_error("Unknown parameter: "s + key);
            }
            return defaults_->template Get<T>(key);
        }
        return FromString<T>(it->second);
    }

private:
    std::unordered_map<std::string, std::string> params_;
    const ParameterSet* defaults_;
};

std::vector<ParameterSet> ParseParameters(const std::vector<std::string>& raw,
                                          const ParameterSet* defaults = nullptr) {
    struct KVPair {
        std::string key;
        std::vector<std::string> values;
        size_t pos = 0;
    };

    std::unordered_map<std::string, KVPair> parsed;
    std::regex splitter("[^,\\s]+");
    for (const auto& rawField : raw) {
        std::string::size_type eqPos = rawField.find("=");
        std::string key, value;
        if (eqPos != std::string::npos) {
            key = rawField.substr(0, eqPos);
            value = rawField.substr(eqPos + 1);
        } else {
            using namespace std::string_literals;
            throw std::runtime_error("Invalid parameter: "s + rawField);
        }

        auto begin = std::sregex_iterator(value.begin(), value.end(), splitter);
        auto end = std::sregex_iterator();
        parsed[key].key = key;
        while (begin != end) {
            std::smatch match = *begin;
            parsed[key].values.push_back(match.str());
            ++begin;
        }
    }

    std::vector<KVPair> parsedVec;
    for (auto& kv : parsed)
        parsedVec.push_back(std::move(kv.second));

    if (parsedVec.empty())
        return {};

    std::vector<ParameterSet> result;
    std::unordered_map<std::string, std::string> params;
    size_t r = 0;
    bool inc = false;
    while (true) {
        if (r == parsedVec.size()) {
            result.emplace_back(params, defaults);
            --r;
            inc = true;
            continue;
        }

        KVPair& pair = parsedVec[r];

        if (inc) {
            ++pair.pos;
            if (pair.pos >= pair.values.size()) {
                if (r == 0) {
                    // we are done
                    break;
                } else {
                    // digit overflow
                    pair.pos = 0;
                    --r;
                    continue;
                }
            } else {
                inc = false;
            }
        }

        params[pair.key] = pair.values[pair.pos];
        ++r;
    }

    return result;
}
