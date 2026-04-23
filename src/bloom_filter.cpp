#include "bloom_filter.h"
#include <cmath>
#include <functional>

namespace lsm {

    // 创建布隆过滤器，初始化位数组和哈希函数个数。
    // 参数：每个 key 占用多少位（通常 10）, 预期存储多少个 key
    BloomFilter::BloomFilter(size_t bits_per_key, size_t expected_keys) {
        // 计算总的位数组大小
        size_t bits = bits_per_key * expected_keys;
        if (bits < 64) bits = 64;
        // 创建 bits 大小的数组，全部初始化为 false(0)
        bits_.resize(bits, false);
        // 计算最优哈希函数个数
        hash_count_ = static_cast<int>(std::log(2.0) * bits_per_key);
        // 如果值异常，那么就调整到个数至少1，最多30
        if (hash_count_ < 1) hash_count_ = 1;
        if (hash_count_ > 30) hash_count_ = 30;
    }

    // 为给定的 key 生成多个哈希值（数量 = hash_count_）
    // 输入：一个 key（例如 "apple"）, 输出：hash_count_ 个不同的哈希值（例如 7 个）
    std::vector<uint64_t> BloomFilter::getHashes(const std::string& key) const {
        // 创建 vector 存储哈希值，并预分配空间，如果不 reserve，vector 会动态增长
        std::vector<uint64_t> hashes;
        hashes.reserve(hash_count_);
        // 双哈希技巧：只用2个哈希函数生成多个
        // std::hash<std::string> 是 C++ 标准库提供的哈希函数对象，它会对字符串内容计算一个哈希值
        uint64_t h1 = std::hash<std::string>{}(key);
        uint64_t h2 = std::hash<std::string>{}(key + "_salt");
        
        for (int i = 0; i < hash_count_; i++) {
            uint64_t hash = h1 + i * h2;// 线性组合
            hashes.push_back(hash);
        }
        
        return hashes;
    }

    // 将 key 添加到布隆过滤器中，设置对应的位为 1。
    void BloomFilter::add(const std::string& key) {
        // 获取哈希值
        auto hashes = getHashes(key);
        // 遍历每个哈希值
        for (uint64_t hash : hashes) {
            bits_[hash % bits_.size()] = true;
        }
    }

    // 判断 key 是否可能存在于集合中。
    bool BloomFilter::mightContain(const std::string& key) const {
        auto hashes = getHashes(key);
        for (uint64_t hash : hashes) {
            if (!bits_[hash % bits_.size()]) {
                // 一旦有任何位是0 → 一定不存在
                return false;
            }
        }
        // 只有当所有位都是1 → 可能存在
        return true;
        // 还有一种情况：不存在的key，但所有位都是1（假阳性），概率约为 1%（当 bits_per_key=10 时）
    }

    void BloomFilter::clear() {
        for (size_t i = 0; i < bits_.size(); i++) {
            bits_[i] = false;
        }
    }
    // 将布隆过滤器转换成字符串，便于存储或传输。
    std::string BloomFilter::serialize() const {
        // 创建空字符串，长度10
        std::string result;
        // result = "\0\0\0\0\0\0\0\0\0\0"
        result.resize(bits_.size());
        for (size_t i = 0; i < bits_.size(); i++) {
            result[i] = bits_[i] ? 1 : 0;
        }
        return result;
    }
    // 从字符串恢复布隆过滤器的状态。
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