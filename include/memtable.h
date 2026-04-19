#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "common.h"
#include <vector>
#include <mutex>
#include <atomic>

namespace lsm {

class MemTable {
public:
    struct Node {
        std::string key;
        std::string value;
        uint64_t timestamp;
        bool deleted;
        std::vector<Node*> next;
        
        Node(const std::string& k, int level);
        ~Node();
    };
    
    MemTable(int max_level = 12);
    ~MemTable();
    
    void put(const std::string& key, const std::string& value, uint64_t timestamp);
    void remove(const std::string& key, uint64_t timestamp);
    bool get(const std::string& key, std::string& value, uint64_t& timestamp);
    std::vector<DataEntry> toVector() const;
    
    size_t size() const { return size_; }
    size_t count() const { return count_; }
    bool empty() const { return count_ == 0; }
    void clear();
    
private:
    Node* head_;
    int max_level_;
    int current_level_;
    mutable std::atomic<size_t> size_;
    mutable std::atomic<size_t> count_;
    mutable std::mutex mutex_;
    
    int randomLevel();
    Node* findGreaterOrEqual(const std::string& key, std::vector<Node*>& prevs);
    Node* findNode(const std::string& key);
};

} // namespace lsm

#endif