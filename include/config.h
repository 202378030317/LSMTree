#ifndef CONFIG_H
#define CONFIG_H

#include <cstddef>

namespace lsm {

struct Config {
    // MemTable 配置 (4MB)
    static constexpr size_t MEMTABLE_SIZE_MAX = 4 * 1024 * 1024;
    static constexpr int MEMTABLE_MAX_LEVEL = 12;
    
    // SSTable 配置
    static constexpr size_t SSTABLE_BLOCK_SIZE = 4096;
    static constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 10;
    
    // Compaction 配置
    static constexpr int MAX_LEVEL = 4;
    static constexpr int LEVEL0_FILE_NUM = 4;
    static constexpr double LEVEL_SIZE_MULTIPLIER = 10.0;
    
    // 后台任务配置
    static constexpr int FLUSH_INTERVAL_MS = 100;
    static constexpr int COMPACTION_INTERVAL_SECONDS = 1;
};

} // namespace lsm

#endif