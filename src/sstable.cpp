#include "sstable.h"
#include "config.h"
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>

namespace lsm {

SSTable::SSTable(const std::string& dir, int level, int id)
    : dir_(dir), level_(level), id_(id), size_(0), key_count_(0), index_loaded_(false) {
    filename_ = dir_ + "/level_" + std::to_string(level) + 
                "_sstable_" + std::to_string(id) + ".sst";
    bloom_filter_.reset(new BloomFilter(
        Config::BLOOM_FILTER_BITS_PER_KEY, 
        Config::SSTABLE_BLOCK_SIZE));
}

SSTable::~SSTable() {
    if (file_.is_open()) {
        file_.close();
    }
}

bool SSTable::writeFile(const std::vector<DataEntry>& entries) {
    file_.open(filename_, std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        return false;
    }
    
    uint64_t offset = 0;
    key_count_ = entries.size();
    index_.clear();
    min_key_.clear();
    max_key_.clear();
    bloom_filter_->clear();
    
    for (const auto& entry : entries) {
        IndexBlock idx;
        idx.key = entry.key;
        idx.offset = offset;
        idx.size = entry.size();
        index_.push_back(idx);
        
        uint32_t key_size = entry.key.size();
        uint32_t value_size = entry.value.size();
        
        file_.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        file_.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        file_.write(entry.key.c_str(), key_size);
        file_.write(entry.value.c_str(), value_size);
        file_.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        
        uint8_t deleted = entry.deleted ? 1 : 0;
        file_.write(reinterpret_cast<const char*>(&deleted), sizeof(deleted));
        
        bloom_filter_->add(entry.key);
        
        offset += sizeof(key_size) + sizeof(value_size) + key_size + 
                  value_size + sizeof(entry.timestamp) + sizeof(deleted);
        
        if (min_key_.empty() || entry.key < min_key_) min_key_ = entry.key;
        if (max_key_.empty() || entry.key > max_key_) max_key_ = entry.key;
    }
    
    uint64_t index_offset = offset;
    uint32_t index_count = index_.size();
    
    for (const auto& idx : index_) {
        uint32_t key_size = idx.key.size();
        file_.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        file_.write(idx.key.c_str(), key_size);
        file_.write(reinterpret_cast<const char*>(&idx.offset), sizeof(idx.offset));
        file_.write(reinterpret_cast<const char*>(&idx.size), sizeof(idx.size));
    }
    
    std::string bloom_data = bloom_filter_->serialize();
    uint32_t bloom_size = bloom_data.size();
    file_.write(reinterpret_cast<const char*>(&bloom_size), sizeof(bloom_size));
    file_.write(bloom_data.c_str(), bloom_size);
    
    file_.write(reinterpret_cast<const char*>(&key_count_), sizeof(key_count_));
    file_.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    file_.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    
    size_ = offset + index_count * (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)) +
            sizeof(bloom_size) + bloom_size + sizeof(key_count_) + 
            sizeof(index_offset) + sizeof(index_count);
    
    file_.close();
    return true;
}

bool SSTable::loadIndex() {
    if (index_loaded_) return true;
    
    file_.open(filename_, std::ios::in | std::ios::binary);
    if (!file_.is_open()) return false;
    
    file_.seekg(0, std::ios::end);
    size_t file_size = file_.tellg();
    
    if (file_size < sizeof(key_count_) + sizeof(uint64_t) + sizeof(uint32_t)) {
        file_.close();
        return false;
    }
    
    file_.seekg(file_size - sizeof(key_count_) - sizeof(uint64_t) - sizeof(uint32_t));
    
    size_t key_count;
    uint64_t index_offset;
    uint32_t index_count;
    
    file_.read(reinterpret_cast<char*>(&key_count), sizeof(key_count));
    file_.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset));
    file_.read(reinterpret_cast<char*>(&index_count), sizeof(index_count));
    
    key_count_ = key_count;
    
    file_.seekg(index_offset);
    uint32_t bloom_size;
    file_.read(reinterpret_cast<char*>(&bloom_size), sizeof(bloom_size));
    
    if (bloom_size > 0) {
        std::string bloom_data(bloom_size, '\0');
        file_.read(&bloom_data[0], bloom_size);
        bloom_filter_->deserialize(bloom_data);
    }
    
    file_.seekg(index_offset + sizeof(bloom_size) + bloom_size);
    index_.resize(index_count);
    
    for (uint32_t i = 0; i < index_count; i++) {
        uint32_t key_size;
        file_.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        
        if (key_size > 0 && key_size < 1024) {
            index_[i].key.resize(key_size);
            file_.read(&index_[i].key[0], key_size);
            file_.read(reinterpret_cast<char*>(&index_[i].offset), sizeof(index_[i].offset));
            file_.read(reinterpret_cast<char*>(&index_[i].size), sizeof(index_[i].size));
        }
    }
    
    index_loaded_ = true;
    file_.close();
    return true;
}

bool SSTable::searchIndex(const std::string& key, IndexBlock& result) {
    if (!index_loaded_) return false;
    
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
        [](const IndexBlock& block, const std::string& k) {
            return block.key < k;
        });
    
    if (it != index_.end() && it->key == key) {
        result = *it;
        return true;
    }
    return false;
}

bool SSTable::readBlock(uint64_t offset, DataEntry& entry) {
    std::fstream file(filename_, std::ios::in | std::ios::binary);
    if (!file.is_open()) return false;
    
    file.seekg(offset);
    if (file.fail()) {
        file.close();
        return false;
    }
    
    uint32_t key_size, value_size;
    file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
    file.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    
    if (key_size > 1024 || value_size > 1024 * 1024) {
        file.close();
        return false;
    }
    
    entry.key.resize(key_size);
    entry.value.resize(value_size);
    file.read(&entry.key[0], key_size);
    file.read(&entry.value[0], value_size);
    file.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
    
    uint8_t deleted;
    file.read(reinterpret_cast<char*>(&deleted), sizeof(deleted));
    entry.deleted = deleted != 0;
    
    file.close();
    return true;
}

bool SSTable::get(const std::string& key, std::string& value, uint64_t& timestamp) {
    if (!index_loaded_ && !loadIndex()) {
        return false;
    }
    
    if (!bloom_filter_->mightContain(key)) {
        return false;
    }
    
    IndexBlock idx;
    if (!searchIndex(key, idx)) {
        return false;
    }
    
    DataEntry entry;
    if (!readBlock(idx.offset, entry)) {
        return false;
    }
    
    if (!entry.deleted) {
        value = entry.value;
        timestamp = entry.timestamp;
        return true;
    }
    
    return false;
}

bool SSTable::remove() {
    if (file_.is_open()) {
        file_.close();
    }
    return unlink(filename_.c_str()) == 0;
}

std::shared_ptr<SSTable> SSTable::createFromMemTable(
    const std::string& dir, int level, int id,
    const std::vector<DataEntry>& entries) {
    
    std::shared_ptr<SSTable> sstable(new SSTable(dir, level, id));
    if (sstable->writeFile(entries)) {
        sstable->loadIndex();
        return sstable;
    }
    return std::shared_ptr<SSTable>();
}

} // namespace lsm