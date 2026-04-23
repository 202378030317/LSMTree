// 整个 LSM Tree 存储引擎的基础数据类型定义
#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <cstdint>

namespace lsm {

    enum class Status {
        OK,         // 操作成功
        NOT_FOUND,  // key不存在
        IO_ERROR,   // io错误
        CORRUPTION  // 数据损坏
    };
    // 代表一个键值对条目
    struct DataEntry {
        std::string key;    // 键
        std::string value;  // 值
        uint64_t timestamp; // 时间戳：使用 uint64_t 保证跨平台大小一致
        bool deleted;       // 墓碑标记
        
        DataEntry();
        DataEntry(const std::string& k, const std::string& v, uint64_t ts, bool del = false);
        // 计算一个条目的内存/磁盘占用
        size_t size() const;
        // 逻辑：key不同时，先按 key 升序排列。key 相同时，按 timestamp 降序，保证新版本在前
        bool operator<(const DataEntry& other) const;
    };
    // SSTable 文件的索引，用于快速定位数据
    struct IndexBlock {
        std::string key;    // 键
        uint64_t offset;    // 在文件中的偏移起始位置
        uint32_t size;      // 数据块大小
    };

} // namespace lsm

#endif