// 负责在内存中存储最新的数据
#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "common.h"
#include <vector>
#include <mutex>
#include <atomic>

namespace lsm {
    // MemTable = Memory Table 即内存中的有序表，所有写入先到这里
    // 使用跳表（Skip List）实现，O(log n) 读写
    class MemTable {
    public:
        // 下面定义跳表的节点
        struct Node {
            std::string key;
            std::string value;
            uint64_t timestamp;
            bool deleted;
            std::vector<Node*> next;    // 指向下一个节点的指针数组，next[i] 表示这个节点在第 i 层的"下一个节点"是谁。
            
            Node(const std::string& k, int level);
            ~Node();
        };
        
        MemTable(int max_level = 12);
        ~MemTable();
        // 插入
        void put(const std::string& key, const std::string& value, uint64_t timestamp);
        // 查找
        void remove(const std::string& key, uint64_t timestamp);
        // 删除
        bool get(const std::string& key, std::string& value, uint64_t& timestamp);
        // Flush时使用，用于导出数据
        std::vector<DataEntry> toVector() const;
        
        size_t size() const { return size_; }
        size_t count() const { return count_; }
        bool empty() const { return count_ == 0; }
        // 清空
        void clear();
        
    private:
        Node* head_;                        // 哨兵头结点
        int max_level_;                     // 最大层数
        int current_level_;                 // 当前实际层数
        mutable std::atomic<size_t> size_;  // 总字节数
        mutable std::atomic<size_t> count_; // 元素个数
        mutable std::mutex mutex_;
        // 随机生成层数
        int randomLevel();
        // 查找算法
        Node* findGreaterOrEqual(const std::string& key, std::vector<Node*>& prevs);
        Node* findNode(const std::string& key);
    };
} // namespace lsm
#endif