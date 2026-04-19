#include "common.h"

namespace lsm {

DataEntry::DataEntry() : timestamp(0), deleted(false) {}

DataEntry::DataEntry(const std::string& k, const std::string& v, uint64_t ts, bool del)
    : key(k), value(v), timestamp(ts), deleted(del) {}

size_t DataEntry::size() const {
    return key.size() + value.size() + sizeof(timestamp) + sizeof(deleted);
}

bool DataEntry::operator<(const DataEntry& other) const {
    if (key != other.key) return key < other.key;
    return timestamp > other.timestamp;
}

} // namespace lsm