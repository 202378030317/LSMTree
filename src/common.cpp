#include "common.h"

namespace lsm {
    DataEntry::DataEntry() : timestamp(0), deleted(false) {}
    DataEntry::DataEntry(const std::string& k, const std::string& v, uint64_t ts, bool del)
        : key(k), value(v), timestamp(ts), deleted(del) {}
    // 计算每个DataEntry所包含的字节数
    size_t DataEntry::size() const {
        return key.size() + value.size() + sizeof(timestamp) + sizeof(deleted);
    }
    // 重载比较运算符
    bool DataEntry::operator<(const DataEntry& other) const {
        if (key != other.key) return key < other.key;
        return timestamp > other.timestamp;
    }
} // namespace lsm