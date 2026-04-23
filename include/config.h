#ifndef CONFIG_H
#define CONFIG_H

#include <cstddef>

namespace lsm {
// 所有配置都是静态常量，编译时确定
    // 这样设计：
    // 1. 零运行时开销
    // 2. 编译器可以优化
    // 3. 修改后重新编译即可
    struct Config {
        // -----------------------------MemTable 配置------------------------------
        // 4MB：内存表的最大大小，达到这个阈值就会触发刷盘
        static constexpr size_t MEMTABLE_SIZE_MAX = 4 * 1024 * 1024;
        // 跳表的最大层数，代表着查找效率(层数 = log₂(元素个数))
        static constexpr int MEMTABLE_MAX_LEVEL = 12;
        // -----------------------------SSTable 配置-------------------------------
        // 4KB 是磁盘 IO 的最小单位
        // 好处：
        // 与文件系统块大小对齐
        // 减少读放大
        // 缓存效率高
        static constexpr size_t SSTABLE_BLOCK_SIZE = 4096;
        // 每个 key 使用多少比特的布隆过滤器空间:10 bits/key → 约 1% 假阳性率，也就是说：100 个不存在的 key，只有 1 个需要真正读磁盘
        static constexpr size_t BLOOM_FILTER_BITS_PER_KEY = 10;
        // -----------------------------Compaction 配置-----------------------------
        // LSMTree层数（L0 L1 L2 L3）
        static constexpr int MAX_LEVEL = 4;
        // L0 层文件数量达到 4 时触发合并
        static constexpr int LEVEL0_FILE_NUM = 4;
        // 下一层是上一层大小的倍数，保证每层大小以乘10的指数级增长，根据论文分析此时读写平衡性能最好
        static constexpr double LEVEL_SIZE_MULTIPLIER = 10.0;
        // -----------------------------后台任务配置----------------------------------
        // 刷盘线程检查间隔 100 毫秒
        static constexpr int FLUSH_INTERVAL_MS = 100;
        // 合并线程检查间隔 1 秒
        static constexpr int COMPACTION_INTERVAL_SECONDS = 1;
    };
} // namespace lsm
#endif