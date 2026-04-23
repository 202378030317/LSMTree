#include "lsm_tree.h"
#include "sstable.h"
#include "config.h"
#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>

namespace lsm {

LSMTree::LSMTree(const std::string& data_dir) 
    : data_dir_(data_dir), running_(true), sequence_number_(0) {
    // 创建数据存储目录
    mkdir(data_dir.c_str(), 0755);
    // 为层级数组分配空间
    levels_.resize(Config::MAX_LEVEL);
    // 创建活跃的内存表
    active_memtable_.reset(new MemTable());
    // 创建预写日志
    wal_.reset(new WAL(data_dir));
    // 从磁盘恢复之前未刷盘的数据
    /* 
    场景1：正常启动（上次正常关闭）
        SSTable 文件存在
        WAL 已被清空
        直接加载 SSTable
    场景2：崩溃后启动
        SSTable 文件存在（已刷盘的数据）
        WAL 有未刷盘的记录
        加载 SSTable + 重放 WAL
    场景3：首次启动
        没有 SSTable 文件
        WAL 为空
        直接开始
    */
    recover();
    // 启动两个后台并行守护线程:监控 MemTable 大小，满了就刷盘 ; 监控文件数量和大小，触发合并
    flush_thread_ = std::thread(&LSMTree::flushWorker, this);
    compact_thread_ = std::thread(&LSMTree::compactWorker, this);
}

LSMTree::~LSMTree() {
    close();
}

void LSMTree::flush() {
    // 只有当 MemTable 存在且有数据时才执行
    if (active_memtable_ && active_memtable_->size() > 0) {
        // 确保 WAL 中的所有数据已写入磁盘
        wal_->sync();
        // 将活跃 MemTable 变为不可变 MemTable
        switchMemTable();
        // 将 immutable_memtable_ 写入 SSTable 文件
        flushToL0();
    }
}
// 返回当前时间的微秒级时间戳，用于版本控制
uint64_t LSMTree::getTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
}
// 返回递增的序列号，保证每个操作有唯一标识
uint64_t LSMTree::nextSequence() {
    return ++sequence_number_;
}
// 将活跃 MemTable 变为不可变 MemTable，并创建新的活跃 MemTable
void LSMTree::switchMemTable() {
    std::lock_guard<std::mutex> lock(mutex_);
    /* 步骤：
    1. active_memtable_.release() 
        释放 unique_ptr 的所有权并返回原始指针
    active_memtable_ 变为 nullptr
    2. immutable_memtable_.reset(ptr)
        接管原始指针
    immutable_memtable_ 现在指向原来的 active
    */
    immutable_memtable_.reset(active_memtable_.release());
    // 创建新的空 MemTable 作为活跃表
    active_memtable_.reset(new MemTable());
}
// 核心刷盘函数，负责将内存中的 immutable MemTable 写入磁盘成为 L0 层的 SSTable 文件
void LSMTree::flushToL0() {
    // 没有待刷盘的数据，直接返回
    if (!immutable_memtable_ || immutable_memtable_->empty()) {
        return;
    }
    // 将跳表中的所有数据转换为 vector
    auto entries = immutable_memtable_->toVector();
    if (entries.empty()) {
        immutable_memtable_.reset();
        return;
    }
    // 先按 key 升序，key 相同时，按 timestamp 降序，为去重和 SSTable 写入做准备
    std::sort(entries.begin(), entries.end());
    // 删除重复的 key，只保留最新版本
    auto last = std::unique(entries.begin(), entries.end(),
    // std::unique 将相邻的重复 key 的老版本移到末尾，返回指向第一个重复元素的位置   
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    // erase 删除从 last 到末尾的元素
    entries.erase(last, entries.end());
    // 创建 SSTable 对象并写入磁盘（新文件的 ID = 当前层的文件总数）
    int file_id = static_cast<int>(levels_[0].size());
    std::shared_ptr<SSTable> sstable = SSTable::createFromMemTable(data_dir_, 0, file_id, entries);
    // 将新创建的 SSTable 添加到 L0 层
    if (sstable) {
        std::lock_guard<std::mutex> lock(mutex_);
        levels_[0].push_back(sstable);
    }
    // 释放内存表，清空 WAL(因为数据已持久化到 SSTable，回溯也不需要这部分了，所以WAL 不再记录)
    immutable_memtable_.reset();
    wal_->clear();
    // 如果 L0 文件数达到阈值，通知合并线程
    if (levels_[0].size() >= Config::LEVEL0_FILE_NUM) {
        compact_cv_.notify_one();
    }
}
// 后台刷盘线程函数，负责自动监控 MemTable 大小并在达到阈值时触发刷盘
void LSMTree::flushWorker() {
    // 持续运行，直到程序关闭
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        // 等待指定时间（100ms）或被 notify_one() 唤醒
        flush_cv_.wait_for(lock, std::chrono::milliseconds(Config::FLUSH_INTERVAL_MS));
        // 判断 MemTable 是否已满
        if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
            // 在刷盘前释放锁,其他线程可以继续写入新数据到新的 active_memtable_
            lock.unlock();
            // 执行实际的刷盘操作
            wal_->sync();
            switchMemTable();
            flushToL0();
        }
    }
}
// 写入入口函数，负责将用户数据持久化存储
// 参数是一对key-value
Status LSMTree::put(const std::string& key, const std::string& value) {
    // 该键值对的分配序列号和时间戳
    uint64_t seq = nextSequence();
    uint64_t ts = getTimestamp();
    // 先将操作写入磁盘日志（WAL），此时数据在 C++ 流缓冲区（内存），还没有写入操作系统缓冲区，更没有写入磁盘，断电即销毁
    if (!wal_->put(key, value, seq, ts)) {
        return Status::IO_ERROR;
    }
    // 将数据写入内存表（跳表）
    // 因为 MemTable 只关心版本新旧，所以只用时间戳即可
    active_memtable_->put(key, value, ts);
    
    // 强制 WAL 的日志数据同步到磁盘，此时数据已经写入磁盘，即使立即断电，数据也在
    wal_->sync();
    // 如果 MemTable 满了，通知后台刷盘线程
    if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
        flush_cv_.notify_one();
    }
    
    return Status::OK;
}
// 核心查找函数，负责在所有层级中搜索指定的 key
// 查找顺序：active MemTable → immutable MemTable → L0 → L1 → L2 → L3（新数据覆盖旧数据，找到最新的就返回）
bool LSMTree::searchInLevels(const std::string& key, std::string& value, uint64_t& timestamp) {
    // active_memtable_ 是最新写入的地方，包含所有尚未刷盘的最新数据
    if (active_memtable_->get(key, value, timestamp)) {
        return true;
    }
    // 包含的数据比 active 旧，但比磁盘新
    if (immutable_memtable_ && immutable_memtable_->get(key, value, timestamp)) {
        return true;
    }
    // 从后往前遍历：levels_[0] 中，后面的文件更新(最近才加入的)
    // L0 的特点：文件间 key 可能重叠，所以必须检查所有文件，直到找到
    for (int i = static_cast<int>(levels_[0].size()) - 1; i >= 0; i--) {
        // 先检查 inRange 实现快速过滤：如果 key 不在文件范围内，直接跳过
        if (levels_[0][i]->inRange(key)) {
            if (levels_[0][i]->get(key, value, timestamp)) {
                return true;
            }
        }
    }
    // 在L1+ 文件里面O(n*n)找，L1+ 层的文件 key 范围不重叠，找到就退出(break)
    for (int level = 1; level < Config::MAX_LEVEL; level++) {
        for (size_t i = 0; i < levels_[level].size(); i++) {
            if (levels_[level][i]->inRange(key)) {
                if (levels_[level][i]->get(key, value, timestamp)) {
                    return true;
                }
                break;
            }
        }
    }
    
    return false;
}
// 根据 key 查询对应的 value，找到返回 OK，否则返回 NOT_FOUND(对searchInLevels的封装)
Status LSMTree::get(const std::string& key, std::string& value) {
    uint64_t timestamp;
    if (searchInLevels(key, value, timestamp)) {
        return Status::OK;
    }
    return Status::NOT_FOUND;
}
// 标记删除指定的 key（不是物理删除，而是插入墓碑标记）,和put逻辑相同
Status LSMTree::remove(const std::string& key) {
    uint64_t seq = nextSequence();
    uint64_t ts = getTimestamp();
    
    if (!wal_->delete_(key, seq, ts)) {
        return Status::IO_ERROR;
    }
    
    active_memtable_->remove(key, ts);
    
    if (active_memtable_->size() >= Config::MEMTABLE_SIZE_MAX) {
        flush_cv_.notify_one();
    }
    
    return Status::OK;
}
// 用于计算指定层级所有 SSTable 文件的总大小
size_t LSMTree::getLevelSize(int level) const {
    size_t total = 0;
    for (size_t i = 0; i < levels_[level].size(); i++) {
        total += levels_[level][i]->size();
    }
    return total;
}
/*
// 负责将 L0 层的多个 SSTable 文件合并到 L1 层，触发条件：L0 文件数 >= LEVEL0_FILE_NUM (4)
void LSMTree::compactLevel0() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (levels_[0].empty()) return;
    // 从所有 L0 文件中提取 key
    std::vector<DataEntry> all_entries;
    
    for (size_t i = 0; i < levels_[0].size(); i++) {
        // 获取 SSTable 文件的索引列表
        // IndexBlock 记录了每个 key 在 SSTable 文件中的位置。
        const std::vector<IndexBlock>& idxs = levels_[0][i]->getIndex();
        for (size_t j = 0; j < idxs.size(); j++) {
            DataEntry entry;
            entry.key = idxs[j].key;        // 这里先暂时只取key
            all_entries.push_back(entry);
        }
        levels_[0][i]->remove();
    }
    levels_[0].clear();
    // 排序后去重(去重逻辑还是一样的)
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    // 创建L1文件
    int file_id = static_cast<int>(levels_[1].size());
    std::shared_ptr<SSTable> new_sst = SSTable::createFromMemTable(data_dir_, 1, file_id, all_entries);
    // 将去重后的 key 列表写入新的 L1 SSTable
    if (new_sst) {
        levels_[1].push_back(new_sst);
    }
}
// 通用层级合并函数，负责将任意两层（src_level → dest_level）的 SSTable 文件合并
// 将 src_level 层的文件与 dest_level 层重叠的文件合并，生成新的 dest_level 文件，旧的被删除
void LSMTree::compactLevel(int src_level, int dest_level) {
    // 源层至少需要 2 个文件才合并
    if (levels_[src_level].size() < 2) return;
    // 收集源层所有 key
    std::vector<DataEntry> all_entries;
    
    for (size_t i = 0; i < levels_[src_level].size(); i++) {
        const std::vector<IndexBlock>& idxs = levels_[src_level][i]->getIndex();
        for (size_t j = 0; j < idxs.size(); j++) {
            DataEntry entry;
            entry.key = idxs[j].key;
            all_entries.push_back(entry);
        }
        levels_[src_level][i]->remove();
    }
    levels_[src_level].clear();
    // 找出目标层中与源层 key 范围重叠的文件,将这些文件的 key 也收集起来加入合并列表，然后删除这些文件
    // 处理好后合并列表里面的这些 key 需要与源层的 key 一起排序去重，最后重新生成新的 SSTable 文件
    // 不重叠的文件保留
    if (!levels_[dest_level].empty()) {
        // 遍历目标层的所有文件，检查是否与源层 key 范围重叠
        for (size_t i = 0; i < levels_[dest_level].size(); i++) {
            if (levels_[dest_level][i]->getMinKey() <= all_entries.back().key && 
                levels_[dest_level][i]->getMaxKey() >= all_entries.front().key) {
                // 将重叠文件的所有 key 加入合并列表
                const std::vector<IndexBlock>& idxs = levels_[dest_level][i]->getIndex();
                for (size_t j = 0; j < idxs.size(); j++) {
                    DataEntry entry;
                    entry.key = idxs[j].key;
                    all_entries.push_back(entry);
                }
                // 删除磁盘上的 SSTable 文件
                levels_[dest_level][i]->remove();
            }
        }
        levels_[dest_level].clear();
    }
    // 排序后去重
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    // 将去重后的数据分批(目标层每个文件有大小限制，不能无限大)写入新的 SSTable 文件
    // 计算目标层单个文件的最大字节数
    size_t max_size = Config::MEMTABLE_SIZE_MAX * static_cast<size_t>(Config::LEVEL_SIZE_MULTIPLIER);
    // 当前批次累计大小
    size_t current_size = 0;
    // 当前批次的数据
    std::vector<DataEntry> batch;
    
    for (size_t i = 0; i < all_entries.size(); i++) {
        // 将每条数据加入批次，更新累计大小
        batch.push_back(all_entries[i]);
        current_size += all_entries[i].size();
        // 当批次大小达到上限时，写入文件
        if (current_size >= max_size) {
            int file_id = static_cast<int>(levels_[dest_level].size());
            std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(data_dir_, dest_level, file_id, batch);
            // 将新文件加入目标层列表
            if (sst) {
                levels_[dest_level].push_back(sst);
            }
            // 清空当前批次，准备下一个文件
            batch.clear();
            current_size = 0;
        }
    }
    // 循环结束后，如果还有剩余数据，写入最后一个文件(for循环未进入if的最后情况)
    if (!batch.empty()) {
        int file_id = static_cast<int>(levels_[dest_level].size());
        std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(data_dir_, dest_level, file_id, batch);
        if (sst) {
            levels_[dest_level].push_back(sst);
        }
    }
}
*/
// repair1
void LSMTree::compactLevel0() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (levels_[0].empty()) return;
    
    std::vector<DataEntry> all_entries;
    
    // 修复：读取完整数据
    for (size_t i = 0; i < levels_[0].size(); i++) {
        std::vector<DataEntry> file_entries = levels_[0][i]->readAllEntries();
        all_entries.insert(all_entries.end(), 
                          file_entries.begin(), 
                          file_entries.end());
        levels_[0][i]->remove();
    }
    levels_[0].clear();
    
    // 排序去重
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    
    // 创建新文件
    int file_id = static_cast<int>(levels_[1].size());
    std::shared_ptr<SSTable> new_sst = SSTable::createFromMemTable(
        data_dir_, 1, file_id, all_entries);
    if (new_sst) {
        levels_[1].push_back(new_sst);
    }
}
// repair1
void LSMTree::compactLevel(int src_level, int dest_level) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (levels_[src_level].size() < 2) return;
    
    std::vector<DataEntry> all_entries;
    
    // 修复1：读取源层完整数据
    for (size_t i = 0; i < levels_[src_level].size(); i++) {
        std::vector<DataEntry> file_entries = levels_[src_level][i]->readAllEntries();
        all_entries.insert(all_entries.end(), 
                          file_entries.begin(), 
                          file_entries.end());
        levels_[src_level][i]->remove();
    }
    levels_[src_level].clear();
    
    // 修复2：处理目标层，保留不重叠的文件
    if (!levels_[dest_level].empty()) {
        std::vector<std::shared_ptr<SSTable>> keep_files;
        
        for (size_t i = 0; i < levels_[dest_level].size(); i++) {
            // 检查是否重叠
            if (levels_[dest_level][i]->getMinKey() <= all_entries.back().key && 
                levels_[dest_level][i]->getMaxKey() >= all_entries.front().key) {
                // 重叠：读取完整数据
                std::vector<DataEntry> file_entries = levels_[dest_level][i]->readAllEntries();
                all_entries.insert(all_entries.end(), 
                                  file_entries.begin(), 
                                  file_entries.end());
                levels_[dest_level][i]->remove();
            } else {
                // 不重叠：保留
                keep_files.push_back(levels_[dest_level][i]);
            }
        }
        
        levels_[dest_level].clear();
        levels_[dest_level] = keep_files;  // 恢复保留的文件
    }
    
    // 排序去重
    std::sort(all_entries.begin(), all_entries.end());
    auto last = std::unique(all_entries.begin(), all_entries.end(),
        [](const DataEntry& a, const DataEntry& b) {
            return a.key == b.key;
        });
    all_entries.erase(last, all_entries.end());
    
    // 分批创建新文件
    size_t max_size = Config::MEMTABLE_SIZE_MAX * 
                      static_cast<size_t>(Config::LEVEL_SIZE_MULTIPLIER);
    size_t current_size = 0;
    std::vector<DataEntry> batch;
    
    for (size_t i = 0; i < all_entries.size(); i++) {
        batch.push_back(all_entries[i]);
        current_size += all_entries[i].size();
        
        if (current_size >= max_size) {
            int file_id = static_cast<int>(levels_[dest_level].size());
            std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(
                data_dir_, dest_level, file_id, batch);
            if (sst) {
                levels_[dest_level].push_back(sst);
            }
            batch.clear();
            current_size = 0;
        }
    }
    
    if (!batch.empty()) {
        int file_id = static_cast<int>(levels_[dest_level].size());
        std::shared_ptr<SSTable> sst = SSTable::createFromMemTable(
            data_dir_, dest_level, file_id, batch);
        if (sst) {
            levels_[dest_level].push_back(sst);
        }
    }
}
// 检查各层是否需要合并，如果需要则执行，这里的L0和L0+层级的阈值判断条件不同
void LSMTree::doCompaction() {
    if (levels_[0].size() >= Config::LEVEL0_FILE_NUM) {
        compactLevel0();
    }
    // 注意这里是MAX_LEVEL - 1，即只检查到MAX_LEVEL - 1层，合并到MAX_LEVEL层
    for (int level = 1; level < Config::MAX_LEVEL - 1; level++) {
        size_t max_size = static_cast<size_t>(Config::MEMTABLE_SIZE_MAX * 
                          std::pow(Config::LEVEL_SIZE_MULTIPLIER, level));
        if (getLevelSize(level) >= max_size) {
            compactLevel(level, level + 1);// 合并到下一层
        }
    }
}
// 后台线程主函数，定期检查并执行合并
void LSMTree::compactWorker() {
    while (running_) {
        std::unique_lock<std::mutex> lock(mutex_);
        compact_cv_.wait_for(lock, std::chrono::seconds(Config::COMPACTION_INTERVAL_SECONDS));
        lock.unlock();
        doCompaction();
    }
}
// 从磁盘恢复之前的状态
// 1. 加载所有 SSTable 文件
// 2. 从 WAL 重放未刷盘的操作
void LSMTree::recover() {
    // 打开数据目录，准备读取文件列表
    DIR* dir = opendir(data_dir_.c_str());
    if (dir) {
        struct dirent* entry;
        // 读取目录中的每一个文件/子目录
        while ((entry = readdir(dir)) != nullptr) {
            std::string name = entry->d_name;
            // 筛选 SSTable 文件
            if (name.find("level_") == 0 && name.find(".sst") != std::string::npos) {
                // 从文件名提取层级编号
                int level = std::stoi(name.substr(6, 1));
                // 从文件名提取文件编号
                size_t pos = name.find("sstable_");
                if (pos != std::string::npos) {
                    int id = std::stoi(name.substr(pos + 8, name.find(".sst") - pos - 8));
                    // 创建 SSTable 对象，加载索引，加入对应层级
                    std::shared_ptr<SSTable> sstable(new SSTable(data_dir_, level, id));
                    if (sstable->loadIndex()) {// 读取文件末尾的元数据，加载索引到内存
                        levels_[level].push_back(sstable);
                    }
                }
            }
        }
        closedir(dir);
        // 排序每层文件,按最小key排序，便于后续的二分
        for (int level = 0; level < Config::MAX_LEVEL; level++) {
            std::sort(levels_[level].begin(), levels_[level].end(),
                [](const std::shared_ptr<SSTable>& a, const std::shared_ptr<SSTable>& b) {
                    return a->getMinKey() < b->getMinKey();
                });
        }
    }
    // 重放 WAL 中的所有操作，恢复未刷盘的数据（传入一个回调函数）
    wal_->recover([this](const WalRecord& record) {
        if (record.type == WalRecordType::PUT) {
            active_memtable_->put(record.key, record.value, record.timestamp);
        } else if (record.type == WalRecordType::DELETE) {
            active_memtable_->remove(record.key, record.timestamp);
        }
        if (record.sequence > sequence_number_.load()) {
            sequence_number_ = record.sequence;
        }
    });
}
// 手动触发合并函数
// 自动合并：后台线程定期执行（每秒检查一次）
// 手动合并：用户主动触发，立即执行
void LSMTree::manualCompaction() {
    doCompaction();
}
// 打印 LSM Tree 的当前状态，用于监控和调试
void LSMTree::printStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::cout << "=== LSM Tree Statistics ===" << std::endl;
    std::cout << "Active MemTable: " << active_memtable_->size() << " bytes, "
              << active_memtable_->count() << " keys" << std::endl;
    std::cout << "Sequence Number: " << sequence_number_.load() << std::endl;
    
    if (immutable_memtable_) {
        std::cout << "Immutable MemTable: " << immutable_memtable_->size() << " bytes, "
                  << immutable_memtable_->count() << " keys" << std::endl;
    }
    
    for (int i = 0; i < Config::MAX_LEVEL; i++) {
        std::cout << "Level " << i << ": " << levels_[i].size() << " files, ";
        size_t keys = 0, bytes = 0;
        for (size_t j = 0; j < levels_[i].size(); j++) {
            keys += levels_[i][j]->keyCount();
            bytes += levels_[i][j]->size();
        }
        std::cout << keys << " keys, " << bytes << " bytes" << std::endl;
    }
}
// 安全关闭 LSM Tree，确保所有数据持久化
void LSMTree::close() {
    // 线程循环终止，通知所有等待的线程赶紧合并和flush
    running_ = false;
    flush_cv_.notify_all();
    compact_cv_.notify_all();
    // 等待后台线程完全退出
    if (flush_thread_.joinable()) flush_thread_.join();
    if (compact_thread_.joinable()) compact_thread_.join();
    // 将最后的数据刷到磁盘,wal落盘
    if (active_memtable_ && active_memtable_->size() > 0) {
        wal_->sync();
        switchMemTable();
        flushToL0();
    }
    // 确保所有日志数据写入磁盘
    wal_->sync();
}

} // namespace lsm