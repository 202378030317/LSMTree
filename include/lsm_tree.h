// LSM Tree 的主控制类，协调所有组件的工作
/* 核心职责：
1. 管理内存表（active + immutable）
2. 管理磁盘 SSTable（多层结构）
3. 管理 WAL（持久化）
4. 管理后台线程（刷盘 + 合并）
5. 提供对外接口（put/get/remove）
*/
#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "common.h"
#include "memtable.h"
#include "sstable.h"
#include "wal.h"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <condition_variable>

namespace lsm {
    class LSMTree {
    public:
        // 创建，自动恢复数据
        LSMTree(const std::string& data_dir);
        ~LSMTree();
        // 增查删
        Status put(const std::string& key, const std::string& value);
        Status get(const std::string& key, std::string& value);
        Status remove(const std::string& key);
        // 打印统计信息
        void printStats() const;
        // 手动触发合并
        void manualCompaction();
        // 强制刷盘
        void flush();  
        // 安全关闭
        void close();
    private:
        
        std::string data_dir_;                                          // 数据目录
        std::unique_ptr<MemTable> active_memtable_;                     // 活跃内存表（可读写）
        std::unique_ptr<MemTable> immutable_memtable_;                  // 只读内存表（等待刷盘）
        std::vector<std::vector<std::shared_ptr<SSTable>>> levels_;     // 多层 SSTable
        std::unique_ptr<WAL> wal_;                                      // 预写日志

        std::atomic<bool> running_;                    // 后台线程运行标志
        std::atomic<uint64_t> sequence_number_;        // 全局序列号（原子操作）
        std::thread flush_thread_;                     // 刷盘线程
        std::thread compact_thread_;                   // 合并线程
        mutable std::mutex mutex_;                     // 互斥锁
        std::condition_variable flush_cv_;             // 刷盘条件变量
        std::condition_variable compact_cv_;           // 合并条件变量
        // 获取当前时间戳
        uint64_t getTimestamp();
        // 获取下一个序列号
        uint64_t nextSequence();
        // 切换 active → immutable
        void switchMemTable();
        // 将 immutable 刷到 L0
        void flushToL0();
        // 后台刷盘线程主函数
        void flushWorker();
        // 检查并执行合并
        void doCompaction();
        // 合并 L0 → L1
        void compactLevel0();
        // 合并任意两层
        void compactLevel(int src_level, int dest_level);
        // 后台合并线程主函数
        void compactWorker();
        // 查询
        bool searchInLevels(const std::string& key, std::string& value, uint64_t& timestamp);
        // 恢复
        void recover();
        // 获取某层的总大小
        size_t getLevelSize(int level) const;
    };
} // namespace lsm
#endif