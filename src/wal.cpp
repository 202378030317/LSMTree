#include "wal.h"
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <iostream>

namespace lsm {
    static uint32_t crc32_table[256];
    
    static bool crc_table_init = false;
    // 预计算 CRC32 查找表，加速后续的 CRC 计算。
    static void initCRC32Table() {
        if (crc_table_init) return;// 性能优化：避免重复计算 256 个字节的 CRC 值
        
        uint32_t polynomial = 0xEDB88320;// CRC32 标准多项式
        // 一个字节有 256 种可能（0x00 - 0xFF），预先计算每个字节的 CRC 值，存入表中
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;                                   // 当前字节的值
            for (int j = 0; j < 8; j++) {                       // 每个字节 8 位
                crc = (crc >> 1) ^ ((crc & 1) ? polynomial : 0);
            }
            crc32_table[i] = crc;
        }
        crc_table_init = true;// 初始化完成后修改标志
    } 
    /*
    输入：任意长度的数据
    输出：32 位的 CRC 校验和
    作用：检测数据是否被损坏
    */
    static uint32_t computeCRC32(const uint8_t* data, size_t len) {
        initCRC32Table();
        uint32_t crc = 0xFFFFFFFF;// 初始值：32 位全 1
        for (size_t i = 0; i < len; i++) {
            crc = (crc >> 8) ^ crc32_table[(crc ^ data[i]) & 0xFF];
        }
        return crc ^ 0xFFFFFFFF;// 将最终结果的每一位取反。
    }
    // 利用预计算的查找表，快速计算任意数据的 CRC32 值。
    uint32_t WAL::calculateCRC(const std::string& data) {
        return computeCRC32(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    }
    // 输入：WalRecord 对象 ; 输出：字节字符串
    std::string WalRecord::encode() const {
        std::string result;
        // 将枚举类型转换为 1 字节整数并追加。
        result.push_back(static_cast<uint8_t>(type));
        // 将 uint64_t 类型的 8 字节数据序列号和时间戳追加到字符串。
        result.append(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
        result.append(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp));
        // 写入key，先写长度，这样解码时先读长度，再读对应长度的内容
        uint32_t key_size = key.size();
        result.append(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        result.append(key);
        // 同理写入value
        uint32_t value_size = value.size();
        result.append(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        result.append(value);
        
        return result;
    }

    bool WalRecord::decode(const std::string& data) {
        // 最小长度检查
        if (data.size() < 1 + 8 + 8 + 4 + 4) {
            return false;
        }
        
        size_t pos = 0;// 位置指针
        type = static_cast<WalRecordType>(data[pos]);
        pos++;
        // 读取序列号和时间戳
        memcpy(&sequence, data.data() + pos, 8);
        pos += 8;
        memcpy(&timestamp, data.data() + pos, 8);
        pos += 8;
        // 读取key
        uint32_t key_size;
        memcpy(&key_size, data.data() + pos, 4);
        pos += 4;
        key.assign(data.data() + pos, key_size);
        pos += key_size;
        // 读取value
        uint32_t value_size;
        memcpy(&value_size, data.data() + pos, 4);
        pos += 4;
        value.assign(data.data() + pos, value_size);
        
        return true;
    }
    // 生成 WAL 文件的完整路径
    std::string WAL::getWalFileName() {
        return dir_ + "/wal.log";
    }

    WAL::WAL(const std::string& dir) : dir_(dir), current_sequence_(0) {
        // 创建目录（如果不存在）
        mkdir(dir.c_str(), 0755);// 第一个参数：目录路径 ; 第二个参数：权限（八进制）
        current_file_ = getWalFileName();
        openFile();// 打开文件，准备写入
    }

    WAL::~WAL() {
        if (file_.is_open()) {
            sync();// 刷新缓冲区，确保数据写入磁盘
            file_.close();
        }
    }

    bool WAL::openFile() {
        // 若文件已经打开，需要先关闭再进行下一步处理
        if (file_.is_open()) {
            file_.close();
        }
        // 第一次打开：读写追加模式(可写入 | 可读取 | 二进制模式 | 追加模式)
        file_.open(current_file_, std::ios::out | std::ios::in | std::ios::binary | std::ios::app);
        // 第二次打开：只写创建模式
        if (!file_.is_open()) {
            file_.open(current_file_, std::ios::out | std::ios::binary);
            if (!file_.is_open()) return false;
        }
        
        return true;
    }
    // 输入：一条操作记录（PUT 或 DELETE）
    // 作用：将记录持久化到磁盘
    bool WAL::writeRecord(const WalRecord& record) {
        // 先将结构体转化为字符串
        std::string encoded = record.encode();
        uint32_t crc = calculateCRC(encoded);
        // 追加 CRC 到数据末尾
        encoded.append(reinterpret_cast<const char*>(&crc), sizeof(crc));
        // 获取完整数据的字节数
        uint32_t size = encoded.size();
        // 先写入编码文件的长度信息
        file_.write(reinterpret_cast<const char*>(&size), sizeof(size));
        // 再将完整的编码数据写入文件
        file_.write(encoded.data(), size);
        
        // 立即刷新到磁盘
        file_.flush();
        
        return !file_.fail();
    }

    bool WAL::put(const std::string& key, const std::string& value, uint64_t seq, uint64_t ts) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 定义一个对象并初始化
        WalRecord record;
        record.type = WalRecordType::PUT;
        record.sequence = seq;
        record.timestamp = ts;
        record.key = key;
        record.value = value;
        // 写入磁盘
        if (!writeRecord(record)) {
            return false;
        }
        // 记录最大序列号，保证更新时知道从哪个序列号开始
        current_sequence_ = std::max(current_sequence_, seq);
        return true;
    }

    bool WAL::delete_(const std::string& key, uint64_t seq, uint64_t ts) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 定义一个对象并初始化
        WalRecord record;
        record.type = WalRecordType::DELETE;
        record.sequence = seq;
        record.timestamp = ts;
        record.key = key;
        record.value = "";// 删除操作此处为空值
        
        if (!writeRecord(record)) {
            return false;
        }
        
        current_sequence_ = std::max(current_sequence_, seq);
        return true;
    }
    // 作用：强制将缓冲区中的所有数据写入磁盘
    // 确保之前写入的数据真正持久化
    bool WAL::sync() {
        std::lock_guard<std::mutex> lock(mutex_);
        // 只有文件打开时才执行刷新
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
    // 负责从磁盘日志中恢复数据，是崩溃恢复的关键
    // 输入：回调函数，用于处理每条恢复的记录
    // 作用：读取 WAL 文件，重放所有操作
    bool WAL::recover(std::function<void(const WalRecord&)> callback) {
        // 以二进制只读模式打开 WAL 文件
        std::fstream file(current_file_, std::ios::in | std::ios::binary);
        if (!file.is_open()) {
            return true;// 没有 WAL 文件，正常情况
            // 场景1：首次运行，没有 WAL 文件
            // 不是错误，只是没有需要恢复的数据
            // 场景2：Flush 后删除了 WAL 文件
            // 正常情况，不需要恢复
            // 这两种情况也是正常的情况
        }
        
        while (true) {
            // 读取 4 字节的长度字段，这样知道了后续还需要多少数据去读
            uint32_t size;
            file.read(reinterpret_cast<char*>(&size), sizeof(size));
            if (file.eof()) break;// 读到文件末尾（file.eof())停止
            
            if (size == 0 || size > 10 * 1024 * 1024) {// 防止读取异常数据导致内存溢出，文件出现严重损坏，停止整个恢复过程
                break;
            }
            
            std::string data(size, '\0');// 创建 size 大小的字符串
            file.read(&data[0], size);   // 读取数据
            // 检查读取是否完整，gcount()返回实际读取的字节数
            if (file.gcount() != static_cast<int>(size)) break;
            
            // data 的格式：
            // [record_data][crc32(4)]
            // 至少需要 4 字节才能存储 CRC
            // 如果 data.size() < 4，数据肯定损坏
            if (data.size() < 4) continue;
            
            uint32_t stored_crc;
            // 从数据末尾取出存储的 CRC 值
            memcpy(&stored_crc, data.data() + data.size() - 4, 4);
            // 去除末尾 4 字节 CRC，只保留原始数据
            std::string record_data = data.substr(0, data.size() - 4);
            // 对 record_data 重新计算 CRC
            uint32_t calc_crc = computeCRC32(reinterpret_cast<const uint8_t*>(record_data.data()), record_data.size());
            // 如果和原来存储的不相等，说明数据被篡改或损坏，那么就跳过损坏的记录，继续读下一条
            if (stored_crc != calc_crc) {
                continue;
            }
            // 将字节数据转换回结构体，并通知调用者
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