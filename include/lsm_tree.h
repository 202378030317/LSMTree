#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "common.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>

namespace lsm {

class LSMTree {
public:
    LSMTree(const std::string& data_dir);
    ~LSMTree();
    
    Status put(const std::string& key, const std::string& value);
    Status get(const std::string& key, std::string& value);
    Status remove(const std::string& key);
    
    void printStats() const;
    void manualCompaction();
    void flush();  // 强制刷盘
    void close();
    
private:
    std::string data_dir_;
    std::unique_ptr<MemTable> active_memtable_;
    std::unique_ptr<MemTable> immutable_memtable_;
    std::vector<std::vector<std::shared_ptr<SSTable>>> levels_;
    std::unique_ptr<WAL> wal_;
    
    std::atomic<bool> running_;
    std::atomic<uint64_t> sequence_number_;
    std::thread flush_thread_;
    std::thread compact_thread_;
    mutable std::mutex mutex_;
    std::condition_variable flush_cv_;
    std::condition_variable compact_cv_;
    
    uint64_t getTimestamp();
    uint64_t nextSequence();
    void switchMemTable();
    void flushToL0();
    void flushWorker();
    void doCompaction();
    void compactLevel0();
    void compactLevel(int src_level, int dest_level);
    void compactWorker();
    bool searchInLevels(const std::string& key, std::string& value, uint64_t& timestamp);
    void recover();
    size_t getLevelSize(int level) const;
};

} // namespace lsm

#endif