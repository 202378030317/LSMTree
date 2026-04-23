// SSTable 的完整实现，负责将内存中的数据持久化到磁盘文件
#include "sstable.h"
#include "config.h"
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <cstring>

namespace lsm {
// 创建 SSTable 对象，初始化成员变量
SSTable::SSTable(const std::string& dir, int level, int id)
    : dir_(dir), level_(level), id_(id), size_(0), key_count_(0), index_loaded_(false) {
    // 生成文件名
    filename_ = dir_ + "/level_" + std::to_string(level) + 
                "_sstable_" + std::to_string(id) + ".sst";
    // 创建布隆过滤器
    bloom_filter_.reset(
        new BloomFilter(
        Config::BLOOM_FILTER_BITS_PER_KEY,  // 10 bits/key
        Config::SSTABLE_BLOCK_SIZE)         // 4096 预期key数
    );
}

SSTable::~SSTable() {
    if (file_.is_open()) {
        file_.close();
    }
}
// 将一批有序的 DataEntry 写入磁盘文件，同时构建索引、布隆过滤器和元数据。
bool SSTable::writeFile(const std::vector<DataEntry>& entries) {// 参数：entries  已排序的 DataEntry 向量（由 MemTable 提供）
    // 以二进制写入模式打开文件
    file_.open(filename_, std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        return false;
    }
    
    uint64_t offset = 0;           // 当前写入位置
    key_count_ = entries.size();   // 记录key数量
    index_.clear();                // 清空索引
    min_key_.clear();              // 清空最小key
    max_key_.clear();              // 清空最大key
    bloom_filter_->clear();        // 清空布隆过滤器
    
    // 将一条 DataEntry 的所有字段按照固定格式写入磁盘文件
    for (const auto& entry : entries) {
        // 创建索引键对象
        IndexBlock idx;
        // 初始化
        idx.key = entry.key;
        idx.offset = offset;
        idx.size = entry.size();
        index_.push_back(idx);
        // 写入数据字段
        uint32_t key_size = entry.key.size();
        uint32_t value_size = entry.value.size();
        // 将整数类型的内存地址转换为 char* 指针，因为 write 函数接受 const char* 参数
        // 写入 key 的长度，用于后续读取时知道 key 占多少字节
        file_.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        // 写入 value 的长度
        file_.write(reinterpret_cast<const char*>(&value_size), sizeof(value_size));
        // 写入 key 的实际字符串内容：entry.key.c_str() 返回 const char* 指针，指向字符串的第一个字符
        file_.write(entry.key.c_str(), key_size);
        // 写入 value 的实际字符串内容
        file_.write(entry.value.c_str(), value_size);
        // 写入时间戳
        // 为什么需要时间戳？
        // 同一个 key 可能有多个版本，时间戳大的表示新版本，保证去重时保留最新版本
        file_.write(reinterpret_cast<const char*>(&entry.timestamp), sizeof(entry.timestamp));
        
        uint8_t deleted = entry.deleted ? 1 : 0;
        // 写入删除标记，实现逻辑删除
        file_.write(reinterpret_cast<const char*>(&deleted), sizeof(deleted));
        // 将 key 添加到布隆过滤器
        bloom_filter_->add(entry.key);
        // offset 是下一条数据的起始写入位置, 需要记录每条数据的起始位置用于索引
        offset += sizeof(key_size) + sizeof(value_size) + key_size + 
                  value_size + sizeof(entry.timestamp) + sizeof(deleted);
        // 更新文件中 key 的最小值和最大值
        if (min_key_.empty() || entry.key < min_key_) min_key_ = entry.key;
        if (max_key_.empty() || entry.key > max_key_) max_key_ = entry.key;
    }
    // offset 是数据区结束的位置，也是索引区开始的位置
    uint64_t index_offset = offset;
    // 记录有多少条索引项
    uint32_t index_count = index_.size();
    
    // 写入索引区
    for (const auto& idx : index_) {
        uint32_t key_size = idx.key.size();
        // 写入 key 的长度
        file_.write(reinterpret_cast<const char*>(&key_size), sizeof(key_size));
        // 写入 key 的实际字符串内容
        file_.write(idx.key.c_str(), key_size);
        // 写入数据在文件中的位置
        file_.write(reinterpret_cast<const char*>(&idx.offset), sizeof(idx.offset));
        // 写入数据块的大小
        file_.write(reinterpret_cast<const char*>(&idx.size), sizeof(idx.size));
    }
    
    // 写入布隆过滤器
    // 因为位数组不能直接写入文件（包含内部结构）所以需要转换成连续的字节序列(即转换为字符串格式)
    std::string bloom_data = bloom_filter_->serialize();
    // 记录布隆过滤器占用的字节数
    uint32_t bloom_size = bloom_data.size();
    file_.write(reinterpret_cast<const char*>(&bloom_size), sizeof(bloom_size));
    file_.write(bloom_data.c_str(), bloom_size);
    
    // 写入元数据
    // 记录文件中包含多少个 key
    file_.write(reinterpret_cast<const char*>(&key_count_), sizeof(key_count_));
    file_.write(reinterpret_cast<const char*>(&index_offset), sizeof(index_offset));
    file_.write(reinterpret_cast<const char*>(&index_count), sizeof(index_count));
    // 计算文件大小并关闭
    size_ = offset + index_count * (sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t)) +
            sizeof(bloom_size) + bloom_size + sizeof(key_count_) + 
            sizeof(index_offset) + sizeof(index_count);
    
    file_.close();
    return true;
}
// 从磁盘文件加载索引和布隆过滤器到内存
bool SSTable::loadIndex() {
    if (index_loaded_) return true;
    
    file_.open(filename_, std::ios::in | std::ios::binary);
    if (!file_.is_open()) return false;
    // 移动到文件末尾
    file_.seekg(0, std::ios::end);
    // 此时获取当前位置即文件大小
    size_t file_size = file_.tellg();
    // 确保文件至少包含元数据
    if (file_size < sizeof(key_count_) + sizeof(uint64_t) + sizeof(uint32_t)) {
        file_.close();
        return false;
    }
    // 跳转到元数据开始的位置
    file_.seekg(file_size - sizeof(key_count_) - sizeof(uint64_t) - sizeof(uint32_t));
    // 读取元数据
    size_t key_count;
    uint64_t index_offset;
    uint32_t index_count;
    
    file_.read(reinterpret_cast<char*>(&key_count), sizeof(key_count));
    file_.read(reinterpret_cast<char*>(&index_offset), sizeof(index_offset));
    file_.read(reinterpret_cast<char*>(&index_count), sizeof(index_count));
    
    key_count_ = key_count;
    // 跳转到布隆过滤器开始的位置
    file_.seekg(index_offset);
    // 读取布隆过滤器
    uint32_t bloom_size;
    file_.read(reinterpret_cast<char*>(&bloom_size), sizeof(bloom_size));
    
    if (bloom_size > 0) {
        std::string bloom_data(bloom_size, '\0');
        file_.read(&bloom_data[0], bloom_size);
        bloom_filter_->deserialize(bloom_data);
    }
    // 跳转到索引区开始的位置
    file_.seekg(index_offset + sizeof(bloom_size) + bloom_size);
    // 预分配给索引数组空间
    index_.resize(index_count);
    // 读取索引
    for (uint32_t i = 0; i < index_count; i++) {
        uint32_t key_size;
        file_.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        // 安全检查 ：防止恶意数据导致内存溢出
        if (key_size > 0 && key_size < 1024) {
            index_[i].key.resize(key_size);
            file_.read(&index_[i].key[0], key_size);
            file_.read(reinterpret_cast<char*>(&index_[i].offset), sizeof(index_[i].offset));
            file_.read(reinterpret_cast<char*>(&index_[i].size), sizeof(index_[i].size));
        }
    }
    // 完成加载
    index_loaded_ = true;
    file_.close();
    return true;
}
// 使用二分查找在索引中快速定位 key（索引区是有序的，使用二分查找更合适）
bool SSTable::searchIndex(const std::string& key, IndexBlock& result) {
    // 确保索引已经在内存中了，如果索引没加载，无法查找
    if (!index_loaded_) return false;
    // 在有序序列中查找第一个 >= key 的元素，返回指向该元素的迭代器
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
        [](const IndexBlock& block, const std::string& k) {
            return block.key < k;
        });
    // 这里检查值相等是必要的
    if (it != index_.end() && it->key == key) {
        result = *it;
        return true;
    }
    return false;
}
// repair1
std::vector<DataEntry> SSTable::readAllEntries() {
    std::vector<DataEntry> entries;
    entries.reserve(key_count_);
    
    for (const auto& idx : index_) {
        DataEntry entry;
        if (readBlock(idx.offset, entry)) {
            entries.push_back(entry);
        }
    }
    return entries;
}
// 根据偏移量从磁盘读取一条完整的数据记录，是 SSTable 查询的最终执行者
// 输入：offset  数据在文件中的位置
// 输出：entry  读取到的数据
bool SSTable::readBlock(uint64_t offset, DataEntry& entry) {
    // 每次读取都独立打开文件
    std::fstream file(filename_, std::ios::in | std::ios::binary);
    if (!file.is_open()) return false;
    // 将文件指针移动到 offset 位置（数据块的起始位置）
    file.seekg(offset);
    if (file.fail()) {
        file.close();
        return false;
    }
    // 读取 key_size 和 value_size
    uint32_t key_size, value_size;
    file.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));
    file.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    // 安全检查：防止恶意数据导致内存溢出
    if (key_size > 1024 || value_size > 1024 * 1024) {
        file.close();
        return false;
    }
    // 读取 key 和 value（先分配空间）
    entry.key.resize(key_size);
    entry.value.resize(value_size);
    file.read(&entry.key[0], key_size);
    file.read(&entry.value[0], value_size);
    // 读取 8 字节的时间戳
    file.read(reinterpret_cast<char*>(&entry.timestamp), sizeof(entry.timestamp));
    // 读取删除标志
    uint8_t deleted;
    file.read(reinterpret_cast<char*>(&deleted), sizeof(deleted));
    entry.deleted = deleted != 0;
    
    file.close();
    return true;
}
// 主要查询接口，整合了布隆过滤器、索引查找、数据读取的完整流程
// 输入：要查找的 key
// 输出：value - 找到的值，timestamp - 时间戳
bool SSTable::get(const std::string& key, std::string& value, uint64_t& timestamp) {
    // 懒加载索引：如果索引未加载，自动加载
    if (!index_loaded_ && !loadIndex()) {
        return false;
    }
    // 布隆过滤器快速判断 key 是否可能存在
    if (!bloom_filter_->mightContain(key)) {
        return false;
    }
    // 在索引中二分查找，找到 key 对应的位置信息
    IndexBlock idx;
    if (!searchIndex(key, idx)) {
        return false;
    }
    // 根据 offset 从磁盘读取完整的数据块
    DataEntry entry;
    if (!readBlock(idx.offset, entry)) {
        return false;
    }
    // 如果数据未被删除，返回结果；否则返回 false
    if (!entry.deleted) {
        value = entry.value;
        timestamp = entry.timestamp;
        return true;
    }
    
    return false;
}
// 删除磁盘上的 SSTable 文件
bool SSTable::remove() {
    // 为了平台的兼容性应保持先关闭再删除的习惯
    if (file_.is_open()) {
        file_.close();
    }
    // unlink() 是 POSIX 系统调用
    // 作用：删除文件的目录项，减少链接计数，当链接计数变为 0 时，文件被删除
    return unlink(filename_.c_str()) == 0;
}
// 从 MemTable 的数据创建 SSTable 文件, 成功返回 shared_ptr，失败返回空指针
// 使用静态工厂方法，一步完成创建和写入
std::shared_ptr<SSTable> SSTable::createFromMemTable(
    const std::string& dir, int level, int id,
    const std::vector<DataEntry>& entries) {
    // 在堆上创建 SSTable 对象
    std::shared_ptr<SSTable> sstable(new SSTable(dir, level, id));
    // 将数据写入磁盘文件。
    // writeFile 执行：
    // 1. 写入数据块
    // 2. 写入索引块
    // 3. 写入布隆过滤器
    // 4. 写入元数据
    if (sstable->writeFile(entries)) {
        // 将索引加载到内存
        sstable->loadIndex();
        return sstable;
    }
    return std::shared_ptr<SSTable>();
}

} // namespace lsm