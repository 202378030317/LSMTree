#ifndef SSTABLE_H
#define SSTABLE_H

#include "common.h"
#include "bloom_filter.h"
#include <fstream>
#include <memory>
#include <string>
#include <vector>

namespace lsm {

class SSTable {
public:
    SSTable(const std::string& dir, int level, int id);
    ~SSTable();
    
    static std::shared_ptr<SSTable> createFromMemTable(
        const std::string& dir, int level, int id,
        const std::vector<DataEntry>& entries);
    
    bool get(const std::string& key, std::string& value, uint64_t& timestamp);
    bool inRange(const std::string& key) const {
        return key >= min_key_ && key <= max_key_;
    }
    
    bool loadIndex();
    bool remove();
    
    size_t size() const { return size_; }
    size_t keyCount() const { return key_count_; }
    const std::string& getMinKey() const { return min_key_; }
    const std::string& getMaxKey() const { return max_key_; }
    const std::vector<IndexBlock>& getIndex() const { return index_; }
    
private:
    std::string dir_;
    std::string filename_;
    int level_;
    int id_;
    size_t size_;
    size_t key_count_;
    std::string min_key_;
    std::string max_key_;
    
    mutable std::fstream file_;
    std::vector<IndexBlock> index_;
    std::unique_ptr<BloomFilter> bloom_filter_;
    bool index_loaded_;
    
    bool writeFile(const std::vector<DataEntry>& entries);
    bool searchIndex(const std::string& key, IndexBlock& result);
    bool readBlock(uint64_t offset, DataEntry& entry);
};

} // namespace lsm

#endif