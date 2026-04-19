#ifndef WAL_H
#define WAL_H

#include "common.h"
#include <fstream>
#include <mutex>
#include <functional>

namespace lsm {

enum class WalRecordType : uint8_t {
    PUT = 0x01,
    DELETE = 0x02
};

struct WalRecord {
    WalRecordType type;
    uint64_t sequence;
    uint64_t timestamp;
    std::string key;
    std::string value;
    
    std::string encode() const;
    bool decode(const std::string& data);
};

class WAL {
public:
    WAL(const std::string& dir);
    ~WAL();
    
    bool put(const std::string& key, const std::string& value, uint64_t seq, uint64_t ts);
    bool delete_(const std::string& key, uint64_t seq, uint64_t ts);
    bool sync();
    void clear();
    bool recover(std::function<void(const WalRecord&)> callback);
    
private:
    std::string dir_;
    std::string current_file_;
    std::fstream file_;
    std::mutex mutex_;
    uint64_t current_sequence_;
    
    std::string getWalFileName();
    bool openFile();
    bool writeRecord(const WalRecord& record);
    uint32_t calculateCRC(const std::string& data);
};

} // namespace lsm

#endif