# LSM Tree 单机存储引擎
一个基于 LSM Tree 架构的高性能单机键值存储引擎，支持 WAL 崩溃恢复和布隆过滤器。

#特性
高性能写入：所有写入都是顺序 IO，写入性能极高
WAL 支持：预写日志保证数据持久化，支持崩溃恢复
布隆过滤器：快速判断 key 是否存在，减少无效磁盘读取
多层级合并：自动 Compaction，有效管理磁盘空间
并发安全：支持多线程并发读写
轻量级：无外部依赖，纯 C++11 实现
#性能指标
操作             吞吐量           延迟
写入 
读取 
崩溃恢复 

内存表大小参数调优建议：
// 写多读少场景
static constexpr size_t MEMTABLE_SIZE_MAX = 16 * 1024 * 1024;  // 16MB
// 内存受限场景
static constexpr size_t MEMTABLE_SIZE_MAX = 1 * 1024 * 1024;   // 1MB
// 追求极致读性能
static constexpr size_t MEMTABLE_SIZE_MAX = 2 * 1024 * 1024;   // 2MB（更快刷盘）

布隆过滤器参数调优建议：
// SSD 磁盘（随机读快，可以容忍稍高假阳性）
static constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 8;
// HDD 磁盘（随机读慢，需要低假阳性）
static constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 12;
// 内存敏感场景
static constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 6;

LSMTree层数参数调优建议：
// 层级计算公式
level_size = MEMTABLE_SIZE_MAX * (LEVEL_SIZE_MULTIPLIER ^ level)
4 层可支持数据量：
L0: 4MB × 4文件 = 16MB
L1: 16MB × 10 = 160MB
L2: 160MB × 10 = 1.6GB
L3: 1.6GB × 10 = 16GB
总容量 ≈ 18GB
对于单机存储引擎，18GB 是合理范围
如果需要更大，增加 MAX_LEVEL

刷盘线程检查间隔参数调优建议：
// 写入频繁场景
FLUSH_INTERVAL_MS = 50   // 更及时刷盘
// 写入不频繁
FLUSH_INTERVAL_MS = 500  // 减少 CPU 唤醒
