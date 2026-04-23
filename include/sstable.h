// 负责将内存中的数据持久化到磁盘
// SSTable = Sorted String Table（有序字符串表）
/* 
特点：
1. 数据按 key 排序
2. 一旦写入，不可修改（只读）
3. 包含索引和布隆过滤器
4. 支持快速查找
*/
// 文件结构：
// ┌─────────────────┐
// │ Data Blocks     │ ← 实际数据
// ├─────────────────┤
// │ Index Blocks    │ ← 索引（key → 位置）
// ├─────────────────┤
// │ Bloom Filter    │ ← 快速判断 key 是否存在
// ├─────────────────┤
// │ Metadata        │ ← 元数据（key数量、范围等）
// └─────────────────┘
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
        // 构造函数：创建对象，生成文件名，初始化布隆过滤器
        SSTable(const std::string& dir, int level, int id);
        ~SSTable();
        // 工厂方法 : 从 MemTable 的数据创建 SSTable 文件
        static std::shared_ptr<SSTable> createFromMemTable(
            const std::string& dir, int level, int id,
            const std::vector<DataEntry>& entries);
        // 根据 key 查找对应的 value
        bool get(const std::string& key, std::string& value, uint64_t& timestamp);
        // 快速判断 key 是否可能在此文件中，若没有直接跳过这一页
        bool inRange(const std::string& key) const {
            return key >= min_key_ && key <= max_key_;
        }
        // 从磁盘加载索引到内存
        bool loadIndex();
        // 删除磁盘上的 SSTable 文件
        bool remove();
        // repair1 读取所有完整数据（用于 Compaction）
        std::vector<DataEntry> readAllEntries();
        
        size_t size() const { return size_; }
        size_t keyCount() const { return key_count_; }
        const std::string& getMinKey() const { return min_key_; }
        const std::string& getMaxKey() const { return max_key_; }
        const std::vector<IndexBlock>& getIndex() const { return index_; }
        
    private:
        std::string dir_;      // 目录路径，如 "./data"
        std::string filename_; // 完整文件名，如 "./data/level_0_sstable_0.sst"
        int level_;            // 层级（0,1,2,3...）
        int id_;               // 文件编号（同层内唯一）
        // 文件名格式：level_{level_}_sstable_{id_}.sst
        // eg：level_0_sstable_0.sst  // L0 层第0个文件
        size_t size_;          // 文件大小（字节）,用于 Compaction 时计算层级大小
        size_t key_count_;     // 包含的 key 数量
        std::string min_key_;  // 最小 key（用于范围查询）
        std::string max_key_;  // 最大 key（用于范围查询）
        
        mutable std::fstream file_;                     // 文件流（可读可写）,定义一个 mutable 允许在 const 函数中修改
        std::vector<IndexBlock> index_;                 // 索引（key → 位置）
        std::unique_ptr<BloomFilter> bloom_filter_;     // 布隆过滤器
        bool index_loaded_;                             // 索引是否已加载 
        // 将数据写入磁盘文件
        bool writeFile(const std::vector<DataEntry>& entries);
        // 在索引中二分查找 key
        bool searchIndex(const std::string& key, IndexBlock& result);
        // 根据偏移量读取数据块
        bool readBlock(uint64_t offset, DataEntry& entry);
    };
    
} // namespace lsm

#endif