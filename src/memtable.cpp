// 跳表（Skip List）的完整实现，是 LSM Tree 的内存核心
#include "memtable.h"
#include <cstdlib>
#include <algorithm>

namespace lsm {

    MemTable::Node::Node(const std::string& k, int level) 
        : key(k), 
        timestamp(0), 
        deleted(false), 
        next(level, nullptr) // 创建 level 大小的 vector，全部初始化为 nullptr
        {}

    MemTable::Node::~Node() {}

    MemTable::MemTable(int max_level) 
        : max_level_(max_level), current_level_(1), size_(0), count_(0) {
        // 创建头节点
        /*
        跳表初始状态：
        Level 11-1:  head → nullptr
        Level 0:     head → nullptr
        */
        head_ = new Node("", max_level_);
    }

    MemTable::~MemTable() {
        Node* current = head_;
        while (current) {
            Node* next = current->next[0];
            delete current;
            current = next;
        }
    }
    /*
    概率分析：
    rand() % 2 产生 0 或 1，概率各 50%
    当结果为 1 时继续增加层数
    level=1: 概率 = 1/2 = 50%
    level=2: 概率 = 1/2 * 1/2 = 1/4 = 25%
    level=3: 概率 = 1/2 * 1/2 * 1/2 = 1/8 = 12.5%
    level=4: 概率 = 1/16 = 6.25%
    ...
    level=12: 概率 = 1/4096 ≈ 0.024%
    期望层数 = 1/(1-p) = 1/(0.5) = 2
    平均每个节点有 2 层
    */
    int MemTable::randomLevel() {
        int level = 1;
        // 直到生成随机数为0且层数保证不能大于最大层数
        while ((rand() % 2) && level < max_level_) {
            level++;
        }
        return level;
    }

    MemTable::Node* MemTable::findGreaterOrEqual(const std::string& key, std::vector<Node*>& prevs) {
        Node* x = head_;
        int level = current_level_ - 1;
        while (level >= 0) {
            Node* next = x->next[level];
            while (next && next->key < key) {
                x = next;
                next = x->next[level];
            }
            if (!prevs.empty()) {
                // prevs[level] 存储每层的前驱节点，用于插入操作
                prevs[level] = x;
            }
            if (level == 0) {
                return next;
            }
            level--;
        }
        return nullptr;
    }

    MemTable::Node* MemTable::findNode(const std::string& key) {
        std::vector<Node*> prevs(max_level_, nullptr);
        Node* node = findGreaterOrEqual(key, prevs);
        if (node && node->key == key) {
            return node;
        }
        return nullptr;
    }

    void MemTable::put(const std::string& key, const std::string& value, uint64_t timestamp) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 先检查是否存在，如果存在的话只做修改即可
        Node* existing = findNode(key);
        if (existing) {
            existing->value = value;
            existing->timestamp = timestamp;
            existing->deleted = false;
            return;
        }
        // 生成随机层数
        int new_level = randomLevel();
        if (new_level > current_level_) {
            current_level_ = new_level;
        }
        // 创建一个节点并进行初始化
        Node* new_node = new Node(key, new_level);
        new_node->value = value;
        new_node->timestamp = timestamp;
        new_node->deleted = false;
        // 查找该节点在每层的前驱并记录在prevs中
        std::vector<Node*> prevs(max_level_, nullptr);
        findGreaterOrEqual(key, prevs);
        // 每层依次插入
        for (int i = 0; i < new_level; i++) {
            new_node->next[i] = prevs[i]->next[i];
            prevs[i]->next[i] = new_node;
        }
        // 更新总体的字节数和元素个数
        size_ += key.size() + value.size();
        count_++;
    }
    // 删除元素(激活墓碑标记，清空其值即可)
    void MemTable::remove(const std::string& key, uint64_t timestamp) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        Node* node = findNode(key);
        if (node) {
            node->deleted = true;
            node->timestamp = timestamp;// 记录删除时间
            node->value.clear();
        } else {
            // 没找到，也要插入墓碑标记，为了处理 Compaction 时的数据一致性！
            put(key, "", timestamp);// 先插入一个空值
            node = findNode(key);   // 再找到该节点
            if (node) node->deleted = true;// 标记为删除
        }
    }
    // 获取元素的值和时间戳
    bool MemTable::get(const std::string& key, std::string& value, uint64_t& timestamp) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        Node* node = findNode(key);
        if (node && !node->deleted) {
            value = node->value;
            timestamp = node->timestamp;
            return true;
        }
        return false;
    }
    // 将跳表中的所有数据导出为有序的 DataEntry 向量，主要用于 Flush 刷盘时生成 SSTable，返回的 vector 是按 key 有序的
    std::vector<DataEntry> MemTable::toVector() const {
        std::lock_guard<std::mutex> lock(mutex_);// 加锁保证：遍历期间不允许修改

        std::vector<DataEntry> entries;
        // 没有 reserve：每添加一个元素，可能触发重新分配
        // 有 reserve：一次性分配 count_ 个元素的空间，避免多次重新分配，性能更好
        entries.reserve(count_);
        // 要获取所有数据，必须遍历第0层，从第0层第一个元素出发
        Node* node = head_->next[0];
        while (node) {
            if (!node->key.empty()) {// 防御性编程，该检查可以省略
                entries.emplace_back(node->key, node->value, node->timestamp, node->deleted);// emplace_back：直接在 vector 中构造，避免拷贝
            }
            node = node->next[0];
        }
        return entries;
    }
    // 清空整个 MemTable，释放所有节点内存，通常用于 Flush 完成后重置内存表。
    void MemTable::clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        // 通过第0层遍历可以删除每个节点
        Node* current = head_->next[0];
        while (current) {
            Node* next = current->next[0];
            delete current;
            current = next;
        }
        // 重置所有头结点
        for (int i = 0; i < max_level_; i++) {
            head_->next[i] = nullptr;
        }
        
        current_level_ = 1; // 重置为只有第0层
        size_ = 0;          // 字节数为0
        count_ = 0;         // 节点数为0
    }
} // namespace lsm