#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <cstdint>

namespace lsm {

enum class Status {
    OK,
    NOT_FOUND,
    IO_ERROR,
    CORRUPTION
};

struct DataEntry {
    std::string key;
    std::string value;
    uint64_t timestamp;
    bool deleted;
    
    DataEntry();
    DataEntry(const std::string& k, const std::string& v, uint64_t ts, bool del = false);
    
    size_t size() const;
    bool operator<(const DataEntry& other) const;
};

struct IndexBlock {
    std::string key;
    uint64_t offset;
    uint32_t size;
};

} // namespace lsm

#endif