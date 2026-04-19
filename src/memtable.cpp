#include "memtable.h"
#include <cstdlib>
#include <algorithm>

namespace lsm {

MemTable::Node::Node(const std::string& k, int level) 
    : key(k), timestamp(0), deleted(false), next(level, nullptr) {}

MemTable::Node::~Node() {}

MemTable::MemTable(int max_level) 
    : max_level_(max_level), current_level_(1), size_(0), count_(0) {
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

int MemTable::randomLevel() {
    int level = 1;
    while ((rand() % 2) && level < max_level_) {
        level++;
    }
    return level;
}

MemTable::Node* MemTable::findGreaterOrEqual(const std::string& key, 
                                              std::vector<Node*>& prevs) {
    Node* x = head_;
    int level = current_level_ - 1;
    while (level >= 0) {
        Node* next = x->next[level];
        while (next && next->key < key) {
            x = next;
            next = x->next[level];
        }
        if (!prevs.empty()) {
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
    
    Node* existing = findNode(key);
    if (existing) {
        existing->value = value;
        existing->timestamp = timestamp;
        existing->deleted = false;
        return;
    }
    
    int new_level = randomLevel();
    if (new_level > current_level_) {
        current_level_ = new_level;
    }
    
    Node* new_node = new Node(key, new_level);
    new_node->value = value;
    new_node->timestamp = timestamp;
    new_node->deleted = false;
    
    std::vector<Node*> prevs(max_level_, nullptr);
    findGreaterOrEqual(key, prevs);
    
    for (int i = 0; i < new_level; i++) {
        new_node->next[i] = prevs[i]->next[i];
        prevs[i]->next[i] = new_node;
    }
    
    size_ += key.size() + value.size();
    count_++;
}

void MemTable::remove(const std::string& key, uint64_t timestamp) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    Node* node = findNode(key);
    if (node) {
        node->deleted = true;
        node->timestamp = timestamp;
        node->value.clear();
    } else {
        put(key, "", timestamp);
        node = findNode(key);
        if (node) node->deleted = true;
    }
}

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

std::vector<DataEntry> MemTable::toVector() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DataEntry> entries;
    entries.reserve(count_);
    
    Node* node = head_->next[0];
    while (node) {
        if (!node->key.empty()) {
            entries.emplace_back(node->key, node->value, node->timestamp, node->deleted);
        }
        node = node->next[0];
    }
    return entries;
}

void MemTable::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    Node* current = head_->next[0];
    while (current) {
        Node* next = current->next[0];
        delete current;
        current = next;
    }
    
    for (int i = 0; i < max_level_; i++) {
        head_->next[i] = nullptr;
    }
    
    current_level_ = 1;
    size_ = 0;
    count_ = 0;
}

} // namespace lsm