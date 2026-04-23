// 布隆过滤器是一种空间效率极高的概率性数据结构，用于判断一个元素是否可能存在于集合中。
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
        std::vector<bool> bits_;// 位数组，每个位置可以是1或0
        int hash_count_;        // 哈希函数个数
        
        std::vector<uint64_t> getHashes(const std::string& key) const;
    };

} // namespace lsm

#endif