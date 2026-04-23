// 预写日志: LSM Tree 中保证数据持久性和崩溃恢复的核心组件。
#ifndef WAL_H
#define WAL_H

#include "common.h"
#include <fstream>
#include <mutex>
#include <functional>

namespace lsm {
    // 标识 WAL 记录的类型，区分是写入还是删除。
    enum class WalRecordType : uint8_t {
        PUT = 0x01,
        DELETE = 0x02
    };

    struct WalRecord {
        WalRecordType type;     // 操作类型：写入操作，value 有内容 ；删除操作，value为空
        uint64_t sequence;      // 序列号（递增）
        uint64_t timestamp;     // 时间戳
        std::string key;        // 键
        std::string value;      // 值
        
        std::string encode() const;             // 序列化
        bool decode(const std::string& data);   // 反序列化
    };

    class WAL {
    public:
        WAL(const std::string& dir);
        ~WAL();
        
        bool put(const std::string& key, const std::string& value, uint64_t seq, uint64_t ts);
        bool delete_(const std::string& key, uint64_t seq, uint64_t ts);
        // 强制将缓冲区数据写入磁盘
        bool sync();
        // 清空 WAL（Flush 后调用）
        void clear();
        // 启动时恢复数据
        bool recover(std::function<void(const WalRecord&)> callback);
        
    private:
        std::string dir_;              // WAL 文件存放目录的路径
        std::string current_file_;     // 当前 WAL 文件名(完整的文件路径)
        std::fstream file_;            // 文件流
        std::mutex mutex_;             // 互斥锁（线程安全）
        uint64_t current_sequence_;    // 当前最大序列号

        std::string getWalFileName();                   // 获取文件名
        bool openFile();                                // 打开文件
        bool writeRecord(const WalRecord& record);      // 写入记录
        uint32_t calculateCRC(const std::string& data); // 计算校验和
    };

} // namespace lsm

#endif