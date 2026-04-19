#include "wal.h"
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <iostream>

namespace lsm {

static uint32_t crc32_table[256];
static bool crc_table_init = false;

static void initCRC32Table() {
    if (crc_table_init) return;
    
    uint32_t polynomial = 0xEDB88320;
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ ((crc & 1) ? polynomial : 0);
        }
        crc32_table[i] = crc;
    }
    crc_table_init = true;
}

static uint32_t computeCRC32(const uint8_t* data, size_t len) {
    initCRC32Table();
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc ^ data[i]) & 0xFF];
    }
    return crc ^ 0xFFFFFFFF;
}

uint32_t WAL::calculateCRC(const std::string& data) {
    return computeCRC32(reinterpret_cast<const uint8_t*>(data.data()), data.size());
}

std::string WalRecord::encode() const {
    std::string result;
    result.push_back(static_cast<uint8_t>(type));
    result.append(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
    result.append(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
    
    uint32_t key_size = key.size();
    result.append(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
    result.append(key);
    
    uint32_t value_size = value.size();
    result.append(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
    result.append(value);
    
    return result;
}

bool WalRecord::decode(const std::string& data) {
    if (data.size() < 1 + 8 + 8 + 4 + 4) {
        return false;
    }
    
    size_t pos = 0;
    type = static_cast<WalRecordType>(data[pos]);
    pos++;
    
    memcpy(&sequence, data.data() + pos, 8);
    pos += 8;
    memcpy(&timestamp, data.data() + pos, 8);
    pos += 8;
    
    uint32_t key_size;
    memcpy(&key_size, data.data() + pos, 4);
    pos += 4;
    key.assign(data.data() + pos, key_size);
    pos += key_size;
    
    uint32_t value_size;
    memcpy(&value_size, data.data() + pos, 4);
    pos += 4;
    value.assign(data.data() + pos, value_size);
    
    return true;
}

std::string WAL::getWalFileName() {
    return dir_ + "/wal.log";
}

WAL::WAL(const std::string& dir) : dir_(dir), current_sequence_(0) {
    mkdir(dir.c_str(), 0755);
    current_file_ = getWalFileName();
    openFile();
}

WAL::~WAL() {
    if (file_.is_open()) {
        sync();
        file_.close();
    }
}

bool WAL::openFile() {
    if (file_.is_open()) {
        file_.close();
    }
    
    file_.open(current_file_, std::ios::out | std::ios::in | std::ios::binary | std::ios::app);
    if (!file_.is_open()) {
        file_.open(current_file_, std::ios::out | std::ios::binary);
        if (!file_.is_open()) return false;
    }
    
    return true;
}

bool WAL::writeRecord(const WalRecord& record) {
    std::string encoded = record.encode();
    uint32_t crc = calculateCRC(encoded);
    encoded.append(reinterpret_cast<const char*>(&crc), sizeof(crc));
    
    uint32_t size = encoded.size();
    file_.write(reinterpret_cast<const char*>(&size), sizeof(size));
    file_.write(encoded.data(), size);
    
    // 立即刷新到磁盘
    file_.flush();
    
    return !file_.fail();
}

bool WAL::put(const std::string& key, const std::string& value, uint64_t seq, uint64_t ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WalRecord record;
    record.type = WalRecordType::PUT;
    record.sequence = seq;
    record.timestamp = ts;
    record.key = key;
    record.value = value;
    
    if (!writeRecord(record)) {
        return false;
    }
    
    current_sequence_ = std::max(current_sequence_, seq);
    return true;
}

bool WAL::delete_(const std::string& key, uint64_t seq, uint64_t ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    WalRecord record;
    record.type = WalRecordType::DELETE;
    record.sequence = seq;
    record.timestamp = ts;
    record.key = key;
    record.value = "";
    
    if (!writeRecord(record)) {
        return false;
    }
    
    current_sequence_ = std::max(current_sequence_, seq);
    return true;
}

bool WAL::sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (file_.is_open()) {
        file_.flush();
    }
    return true;
}

void WAL::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    // 只重置序列号，不删除文件内容
    // 让 WAL 文件保留，直到下次重启时覆盖
    current_sequence_ = 0;
    std::cout << "WAL sequence reset" << std::endl;
}

bool WAL::recover(std::function<void(const WalRecord&)> callback) {
    std::fstream file(current_file_, std::ios::in | std::ios::binary);
    if (!file.is_open()) {
        return true;
    }
    
    while (true) {
        uint32_t size;
        file.read(reinterpret_cast<char*>(&size), sizeof(size));
        if (file.eof()) break;
        
        if (size == 0 || size > 10 * 1024 * 1024) {
            break;
        }
        
        std::string data(size, '\0');
        file.read(&data[0], size);
        if (file.gcount() != static_cast<int>(size)) break;
        
        if (data.size() < 4) continue;
        
        uint32_t stored_crc;
        memcpy(&stored_crc, data.data() + data.size() - 4, 4);
        std::string record_data = data.substr(0, data.size() - 4);
        uint32_t calc_crc = computeCRC32(reinterpret_cast<const uint8_t*>(record_data.data()), record_data.size());
        
        if (stored_crc != calc_crc) {
            continue;
        }
        
        WalRecord record;
        if (record.decode(record_data)) {
            callback(record);
            current_sequence_ = std::max(current_sequence_, record.sequence);
        }
    }
    
    file.close();
    return true;
}

} // namespace lsm