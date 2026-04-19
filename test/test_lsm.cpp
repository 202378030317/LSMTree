#include "lsm_tree.h"
#include <iostream>
#include <chrono>
#include <unistd.h>

using namespace lsm;

void testBasicOperations() {
    std::cout << "\n=== Test 1: Basic Operations ===" << std::endl;
    
    LSMTree db("./test_db");
    
    db.put("name", "张三");
    db.put("age", "25");
    db.put("city", "北京");
    
    std::string value;
    if (db.get("name", value) == Status::OK) {
        std::cout << "name = " << value << std::endl;
    }
    
    db.put("age", "26");
    if (db.get("age", value) == Status::OK) {
        std::cout << "age = " << value << std::endl;
    }
    
    db.remove("city");
    if (db.get("city", value) != Status::OK) {
        std::cout << "city deleted successfully" << std::endl;
    }
}

void testWALRecovery() {
    std::cout << "\n=== Test 2: WAL Recovery ===" << std::endl;
    
    {
        LSMTree db("./test_db2");
        db.put("key1", "value1");
        db.put("key2", "value2");
        db.put("key3", "value3");
        db.remove("key2");
        
        std::cout << "Data written, shutting down..." << std::endl;
    }
    
    std::cout << "Recovering from WAL..." << std::endl;
    {
        LSMTree db("./test_db2");
        
        std::string value;
        if (db.get("key1", value) == Status::OK) {
            std::cout << "key1 = " << value << std::endl;
        }
        if (db.get("key2", value) != Status::OK) {
            std::cout << "key2 deleted" << std::endl;
        }
        if (db.get("key3", value) == Status::OK) {
            std::cout << "key3 = " << value << std::endl;
        }
    }
}

void testCrashRecovery() {
    std::cout << "\n=== Test 3: Crash Recovery ===" << std::endl;
    
    // 写入数据但不 flush，模拟崩溃时 WAL 应该恢复
    {
        LSMTree db("./test_db3");
        
        for (int i = 0; i < 100; i++) {
            db.put("key_" + std::to_string(i), "value_" + std::to_string(i));
        }
        
        // 不调用 manualCompaction，让数据留在 MemTable 和 WAL 中
        std::cout << "100 keys written, WAL should have data, simulating crash..." << std::endl;
        // 直接析构，模拟崩溃
    }
    
    // 恢复并验证
    {
        LSMTree db("./test_db3");
        
        std::string value;
        int found = 0;
        for (int i = 0; i < 100; i++) {
            if (db.get("key_" + std::to_string(i), value) == Status::OK) {
                found++;
            }
        }
        
        std::cout << "Recovered " << found << "/100 keys from WAL" << std::endl;
    }
}

void testBloomFilter() {
    std::cout << "\n=== Test 4: Bloom Filter Effect ===" << std::endl;
    
    LSMTree db("./test_db4");
    
    for (int i = 0; i < 1000; i++) {
        db.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    auto start = std::chrono::high_resolution_clock::now();
    
    std::string value;
    int found = 0;
    for (int i = 1000; i < 2000; i++) {
        if (db.get("key_" + std::to_string(i), value) == Status::OK) {
            found++;
        }
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    
    std::cout << "Query 1000 non-existent keys: " << duration.count() << " us" << std::endl;
    std::cout << "False positives: " << found << std::endl;
}

void testPerformance() {
    std::cout << "\n=== Test 5: Performance ===" << std::endl;
    
    LSMTree db("./test_db5");
    
    auto start = std::chrono::high_resolution_clock::now();
    
    const int COUNT = 5000;
    for (int i = 0; i < COUNT; i++) {
        db.put("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Write " << COUNT << " entries: " << duration.count() << " ms" << std::endl;
    
    start = std::chrono::high_resolution_clock::now();
    
    std::string value;
    int found = 0;
    for (int i = 0; i < COUNT; i++) {
        if (db.get("key_" + std::to_string(i), value) == Status::OK) {
            found++;
        }
    }
    
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Read " << COUNT << " entries: " << duration.count() << " ms" << std::endl;
    std::cout << "Found: " << found << "/" << COUNT << std::endl;
    
    db.printStats();
}

int main() {
    std::cout << "LSM Tree with WAL and Bloom Filter Test" << std::endl;
    std::cout << "========================================" << std::endl;
    
    testBasicOperations();
    testWALRecovery();
    testCrashRecovery();
    testBloomFilter();
    testPerformance();
    
    std::cout << "\n=== All tests completed ===" << std::endl;
    return 0;
}