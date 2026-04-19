#include "lsm_tree.h"
#include "config.h"
#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>

namespace lsm {

LSMTree::LSMTree(const std::string& data_dir) 
    : data_dir_(data_dir), running_(true), sequence_number_(0) {
    
    mkdir(data_dir.c_str(), 0755);
    levels_.resize(Config::MAX_LEVEL);
    active_memtable_.reset(new MemTable());
    wal_.reset(new WAL(data_dir));
    
    recover();
    
    flush_thread_ = std::thread(&LSMTree::flushWorker, this);
    compact_thread_ = std::thread(&LSMTree::compactWorker, this);
}

LSMTree::~LSMTree() {
    close();
}

void LSMTree::flush() {
    if (active_memtable_ && active_memtable_->size() > 0) {
        wal_->sync();
        switchMemTable();
        flushToL0();
    }
}

uint64_t LSMTree::getTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}

uint64_t LSMTree::nextSequence() {
    return ++sequence_number_;
}

void LSMTree::switchMemTable() {
    std::lock_guard<std::mutex> lock(mutex_);
    immutable_memtable_.reset(active_memtable_.release());
    active_memtable_.reset(new MemTable());
}

void LSMTree::flushToL0() {
    if (!immutable_memtable_ || immutable_memtable_->empty()) {
        return;
    }
    
    auto entries = immutable_memtable_->toVector();
    if (entries.empty()) {
        immutable_memtable_.reset();
        return;
    }
    
    std::sort(entries.begin(), entries.end());
    
    auto last = std::unique(entries.begin(), entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    entries.erase(last, entries.end());
    
    int file_id = static_cast<int>(levels_[0].size());
    std::shared_ptr<SSTable> sstable = SSTable::createFromMemTable(data_dir_, 0, file_id, entries);
    
    if (sstable) {
        std::lock_guard<std::mutex> lock(mutex_);
        levels_[0].push_back(sstable);
    }
    
    immutable_memtable_.reset();
    wal_->clear();
    
    if (levels_[0].size() >= Config::LEVEL0_FILE_NUM) {
        compact_cv_.notify_one();
    }
}

void LSMTree::flushWorker() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        flush_cv_.wait_for(lock, std::chrono::milliseconds(Config::FLUSH_INTERVAL_MS));
        
        if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
            lock.unlock();
            wal_->sync();
            switchMemTable();
            flushToL0();
        }
    }
}

Status LSMTree::put(const std::string& key, const std::string& value) {
    uint64_t seq = nextSequence();
    uint64_t ts = getTimestamp();
    
    if (!wal_->put(key, value, seq, ts)) {
        return Status::IO_ERROR;
    }
    
    active_memtable_->put(key, value, ts);
    
    // 强制 WAL 同步到磁盘
    wal_->sync();
    
    if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
        flush_cv_.notify_one();
    }
    
    return Status::OK;
}

bool LSMTree::searchInLevels(const std::string& key, std::string& value, uint64_t& timestamp) {
    if (active_memtable_->get(key, value, timestamp)) {
        return true;
    }
    
    if (immutable_memtable_ && immutable_memtable_->get(key, value, timestamp)) {
        return true;
    }
    
    for (int i = static_cast<int>(levels_[0].size()) - 1; i >= 0; i--) {
        if (levels_[0][i]->inRange(key)) {
            if (levels_[0][i]->get(key, value, timestamp)) {
                return true;
            }
        }
    }
    
    for (int level = 1; level < Config::MAX_LEVEL; level++) {
        for (size_t i = 0; i < levels_[level].size(); i++) {
            if (levels_[level][i]->inRange(key)) {
                if (levels_[level][i]->get(key, value, timestamp)) {
                    return true;
                }
                break;
            }
        }
    }
    
    return false;
}

Status LSMTree::get(const std::string& key, std::string& value) {
    uint64_t timestamp;
    if (searchInLevels(key, value, timestamp)) {
        return Status::OK;
    }
    return Status::NOT_FOUND;
}

Status LSMTree::remove(const std::string& key) {
    uint64_t seq = nextSequence();
    uint64_t ts = getTimestamp();
    
    if (!wal_->delete_(key, seq, ts)) {
        return Status::IO_ERROR;
    }
    
    active_memtable_->remove(key, ts);
    
    if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
        flush_cv_.notify_one();
    }
    
    return Status::OK;
}

size_t LSMTree::getLevelSize(int level) const {
    size_t total = 0;
    for (size_t i = 0; i < levels_[level].size(); i++) {
        total += levels_[level][i]->size();
    }
    return total;
}

void LSMTree::compactLevel0() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (levels_[0].empty()) return;
    
    std::vector<DataEntry> all_entries;
    
    for (size_t i = 0; i < levels_[0].size(); i++) {
        const std::vector<IndexBlock>& idxs = levels_[0][i]->getIndex();
        for (size_t j = 0; j < idxs.size(); j++) {
            DataEntry entry;
            entry.key = idxs[j].key;
            all_entries.push_back(entry);
        }
        levels_[0][i]->remove();
    }
    levels_[0].clear();
    
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    
    int file_id = static_cast<int>(levels_[1].size());
    std::shared_ptr<SSTable> new_sst = SSTable::createFromMemTable(data_dir_, 1, file_id, all_entries);
    if (new_sst) {
        levels_[1].push_back(new_sst);
    }
}

void LSMTree::compactLevel(int src_level, int dest_level) {
    if (levels_[src_level].size() < 2) return;
    
    std::vector<DataEntry> all_entries;
    
    for (size_t i = 0; i < levels_[src_level].size(); i++) {
        const std::vector<IndexBlock>& idxs = levels_[src_level][i]->getIndex();
        for (size_t j = 0; j < idxs.size(); j++) {
            DataEntry entry;
            entry.key = idxs[j].key;
            all_entries.push_back(entry);
        }
        levels_[src_level][i]->remove();
    }
    levels_[src_level].clear();
    
    if (!levels_[dest_level].empty()) {
        for (size_t i = 0; i < levels_[dest_level].size(); i++) {
            if (levels_[dest_level][i]->getMinKey() <= all_entries.back().key && 
                levels_[dest_level][i]->getMaxKey() >= all_entries.front().key) {
                const std::vector<IndexBlock>& idxs = levels_[dest_level][i]->getIndex();
                for (size_t j = 0; j < idxs.size(); j++) {
                    DataEntry entry;
                    entry.key = idxs[j].key;
                    all_entries.push_back(entry);
                }
                levels_[dest_level][i]->remove();
            }
        }
        levels_[dest_level].clear();
    }
    
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    
    size_t max_size = Config::MEMTABLE_SIZE_MAX * static_cast<size_t>(Config::LEVEL_SIZE_MULTIPLIER);
    size_t current_size = 0;
    std::vector<DataEntry> batch;
    
    for (size_t i = 0; i < all_entries.size(); i++) {
        batch.push_back(all_entries[i]);
        current_size += all_entries[i].size();
        
        if (current_size >= max_size) {
            int file_id = static_cast<int>(levels_[dest_level].size());
            std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(data_dir_, dest_level, file_id, batch);
            if (sst) {
                levels_[dest_level].push_back(sst);
            }
            batch.clear();
            current_size = 0;
        }
    }
    
    if (!batch.empty()) {
        int file_id = static_cast<int>(levels_[dest_level].size());
        std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(data_dir_, dest_level, file_id, batch);
        if (sst) {
            levels_[dest_level].push_back(sst);
        }
    }
}

void LSMTree::doCompaction() {
    if (levels_[0].size() >= Config::LEVEL0_FILE_NUM) {
        compactLevel0();
    }
    
    for (int level = 1; level < Config::MAX_LEVEL - 1; level++) {
        size_t max_size = static_cast<size_t>(Config::MEMTABLE_SIZE_MAX * 
                          std::pow(Config::LEVEL_SIZE_MULTIPLIER, level));
        if (getLevelSize(level) >= max_size) {
            compactLevel(level, level + 1);
        }
    }
}

void LSMTree::compactWorker() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        compact_cv_.wait_for(lock, std::chrono::seconds(Config::COMPACTION_INTERVAL_SECONDS));
        lock.unlock();
        doCompaction();
    }
}

void LSMTree::recover() {
    DIR* dir = opendir(data_dir_.c_str());
    if (dir) {
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name = entry->d_name;
            if (name.find("level_") == 0 && name.find(".sst") != std::string::npos) {
                int level = std::stoi(name.substr(6, 1));
                size_t pos = name.find("sstable_");
                if (pos != std::string::npos) {
                    int id = std::stoi(name.substr(pos + 8, name.find(".sst") - pos - 8));
                    std::shared_ptr<SSTable> sstable(new SSTable(data_dir_, level, id));
                    if (sstable->loadIndex()) {
                        levels_[level].push_back(sstable);
                    }
                }
            }
        }
        closedir(dir);
        
        for (int level = 0; level < Config::MAX_LEVEL; level++) {
            std::sort(levels_[level].begin(), levels_[level].end(),
                [](const std::shared_ptr<SSTable>& a, const std::shared_ptr<SSTable>& b) {
                    return a->getMinKey() < b->getMinKey();
                });
        }
    }
    
    wal_->recover([this](const WalRecord& record) {
        if (record.type == WalRecordType::PUT) {
            active_memtable_->put(record.key, record.value, record.timestamp);
        } else if (record.type == WalRecordType::DELETE) {
            active_memtable_->remove(record.key, record.timestamp);
        }
        if (record.sequence > sequence_number_.load()) {
            sequence_number_ = record.sequence;
        }
    });
}

void LSMTree::manualCompaction() {
    doCompaction();
}

void LSMTree::printStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "=== LSM Tree Statistics ===" << std::endl;
    std::cout << "Active MemTable: " << active_memtable_->size() << " bytes, "
              << active_memtable_->count() << " keys" << std::endl;
    std::cout << "Sequence Number: " << sequence_number_.load() << std::endl;
    
    if (immutable_memtable_) {
        std::cout << "Immutable MemTable: " << immutable_memtable_->size() << " bytes, "
                  << immutable_memtable_->count() << " keys" << std::endl;
    }
    
    for (int i = 0; i < Config::MAX_LEVEL; i++) {
        std::cout << "Level " << i << ": " << levels_[i].size() << " files, ";
        size_t keys = 0, bytes = 0;
        for (size_t j = 0; j < levels_[i].size(); j++) {
            keys += levels_[i][j]->keyCount();
            bytes += levels_[i][j]->size();
        }
        std::cout << keys << " keys, " << bytes << " bytes" << std::endl;
    }
}

void LSMTree::close() {
    running_ = false;
    flush_cv_.notify_all();
    compact_cv_.notify_all();
    
    if (flush_thread_.joinable()) flush_thread_.join();
    if (compact_thread_.joinable()) compact_thread_.join();
    
    if (active_memtable_ && active_memtable_->size() > 0) {
        wal_->sync();
        switchMemTable();
        flushToL0();
    }
    
    wal_->sync();
}

} // namespace lsm