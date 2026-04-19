#ifndef BLOOM_FILTER_H
#define BLOOM_FILTER_H

#include <vector>
#include <string>
#include <cstdint>

namespace lsm {

class BloomFilter {
public:
    BloomFilter(size_t bits_per_key, size_t expected_keys);
    
    void add(const std::string& key);
    bool mightContain(const std::string& key) const;
    void clear();
    
    size_t bits() const { return bits_.size(); }
    std::string serialize() const;
    bool deserialize(const std::string& data);
    
private:
    std::vector<bool> bits_;
    int hash_count_;
    
    std::vector<uint64_t> getHashes(const std::string& key) const;
};

} // namespace lsm

#endif