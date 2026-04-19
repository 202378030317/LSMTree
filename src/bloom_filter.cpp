#include "bloom_filter.h"
#include <cmath>
#include <functional>

namespace lsm {

BloomFilter::BloomFilter(size_t bits_per_key, size_t expected_keys) {
    size_t bits = bits_per_key * expected_keys;
    if (bits < 64) bits = 64;
    bits_.resize(bits, false);
    hash_count_ = static_cast<int>(std::log(2.0) * bits_per_key);
    if (hash_count_ < 1) hash_count_ = 1;
    if (hash_count_ > 30) hash_count_ = 30;
}

std::vector<uint64_t> BloomFilter::getHashes(const std::string& key) const {
    std::vector<uint64_t> hashes;
    hashes.reserve(hash_count_);
    
    uint64_t h1 = std::hash<std::string>{}(key);
    uint64_t h2 = std::hash<std::string>{}(key + "_salt");
    
    for (int i = 0; i < hash_count_; i++) {
        uint64_t hash = h1 + i * h2;
        hashes.push_back(hash);
    }
    
    return hashes;
}

void BloomFilter::add(const std::string& key) {
    auto hashes = getHashes(key);
    for (uint64_t hash : hashes) {
        bits_[hash % bits_.size()] = true;
    }
}

bool BloomFilter::mightContain(const std::string& key) const {
    auto hashes = getHashes(key);
    for (uint64_t hash : hashes) {
        if (!bits_[hash % bits_.size()]) {
            return false;
        }
    }
    return true;
}

void BloomFilter::clear() {
    for (size_t i = 0; i < bits_.size(); i++) {
        bits_[i] = false;
    }
}

std::string BloomFilter::serialize() const {
    std::string result;
    result.resize(bits_.size());
    for (size_t i = 0; i < bits_.size(); i++) {
        result[i] = bits_[i] ? 1 : 0;
    }
    return result;
}

bool BloomFilter::deserialize(const std::string& data) {
    if (data.size() != bits_.size()) {
        return false;
    }
    for (size_t i = 0; i < data.size(); i++) {
        bits_[i] = data[i] != 0;
    }
    return true;
}

} // namespace lsm