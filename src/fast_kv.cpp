#include <iostream>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <string>
#include <fstream>
#include <vector>
#include <deque>
#include <cstdint>
#include <thread>
#include <functional>
#include <cassert>
#include <filesystem>

enum class Opcode : uint8_t {
    PUT = 0x01,
    DEL = 0x02
};

struct Shard {
    std::unordered_map<std::string, std::string> data;
    mutable std::shared_mutex rw_lock;
    Shard() = default;
    Shard(const Shard&) = delete;            // Запрещаем копирование
    Shard& operator=(const Shard&) = delete; 
    Shard(Shard&&) = default;                // Разрешаем перемещение
    Shard& operator=(Shard&&) = default;
};

class ShardedKVStore {
private:
    std::deque<Shard> shards_; 
    size_t num_shards_;

    std::ofstream wal_stream_;
    std::string wal_path_;
    std::string snap_path_;
    
    std::mutex wal_mutex_; // Защита файла лога от перемешивания записей

    size_t GetShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % num_shards_;
    }

    void AppendToWAL(Opcode op, const std::string& key, const std::string& value = "") {
        std::lock_guard<std::mutex> lock(wal_mutex_);
        if (!wal_stream_.is_open()) return;

        char op_byte = static_cast<char>(op);
        wal_stream_.write(&op_byte, sizeof(op_byte));

        uint32_t k_len = static_cast<uint32_t>(key.size());
        wal_stream_.write(reinterpret_cast<const char*>(&k_len), sizeof(k_len));
        wal_stream_.write(key.data(), k_len);

        if (op == Opcode::PUT) {
            uint32_t v_len = static_cast<uint32_t>(value.size());
            wal_stream_.write(reinterpret_cast<const char*>(&v_len), sizeof(v_len));
            wal_stream_.write(value.data(), v_len);
        }
        wal_stream_.flush(); 
    }

    void PutInternal(const std::string& key, const std::string& value) {
        size_t idx = GetShardIndex(key);
        std::unique_lock<std::shared_mutex> lock(shards_[idx].rw_lock);
        shards_[idx].data[key] = value;
    }

    void DeleteInternal(const std::string& key) {
        size_t idx = GetShardIndex(key);
        std::unique_lock<std::shared_mutex> lock(shards_[idx].rw_lock);
        shards_[idx].data.erase(key);
    }

public:
    ShardedKVStore(size_t num_shards, const std::string& wal_path, const std::string& snap_path) 
        : num_shards_(num_shards), wal_path_(wal_path), snap_path_(snap_path) {
        
	for(size_t i = 0; i < num_shards_; ++i) {
            shards_.emplace_back();
        }

        RecoverFromWAL();
        wal_stream_.open(wal_path_, std::ios::binary | std::ios::app);
    }

    void RecoverFromWAL() {
        std::ifstream wal_in(wal_path_, std::ios::binary);
        if (!wal_in.is_open()) return;

        int ops_count = 0;
        while (wal_in.peek() != EOF) {
            char op_byte;
            if (!wal_in.read(&op_byte, sizeof(op_byte))) break;
            Opcode op = static_cast<Opcode>(op_byte);

            uint32_t k_len;
            wal_in.read(reinterpret_cast<char*>(&k_len), sizeof(k_len));
            std::string key(k_len, '\0');
            wal_in.read(&key[0], k_len);

            if (op == Opcode::PUT) {
                uint32_t v_len;
                wal_in.read(reinterpret_cast<char*>(&v_len), sizeof(v_len));
                std::string value(v_len, '\0');
                wal_in.read(&value[0], v_len);
                PutInternal(key, value);
            } else if (op == Opcode::DEL) {
                DeleteInternal(key);
            }
            ops_count++;
        }
        if(ops_count > 0) std::cout << "[Recovery] Recovered " << ops_count << " operations from WAL.\n";
    }

    void Put(const std::string& key, const std::string& value) {
        AppendToWAL(Opcode::PUT, key, value);
        PutInternal(key, value);
    }

    bool Get(const std::string& key, std::string& value) const {
        size_t idx = GetShardIndex(key);
        std::shared_lock<std::shared_mutex> lock(shards_[idx].rw_lock);
        
        auto it = shards_[idx].data.find(key);
        if (it != shards_[idx].data.end()) {
            value = it->second;
            return true;
        }
        return false;
    }

    void Delete(const std::string& key) {
        AppendToWAL(Opcode::DEL, key);
        DeleteInternal(key);
    }
};

class TestRunner {
public:
    static void RunAllTests() {
        std::cout << "--- Starting Test Suite ---\n";
        TestBasicOperations();
        TestConcurrency();
        TestWALRecovery();
        std::cout << "--- All Tests Passed! ---\n";
    }

private:
    static void CleanupFiles() {
        if (std::filesystem::exists("test.wal")) std::filesystem::remove("test.wal");
        if (std::filesystem::exists("test.snap")) std::filesystem::remove("test.snap");
    }

    static void TestBasicOperations() {
        CleanupFiles();
        {
            ShardedKVStore db(4, "test.wal", "test.snap");
            db.Put("key1", "value1");
            std::string val;
            assert(db.Get("key1", val) && val == "value1");
            db.Delete("key1");
            assert(!db.Get("key1", val));
        }
        std::cout << "[OK] Basic Operations\n";
    }

    static void TestConcurrency() {
        CleanupFiles();
        ShardedKVStore db(16, "test.wal", "test.snap");

        auto worker = [&db](int id) {
            for (int i = 0; i < 500; ++i) {
                std::string key = "k" + std::to_string(id) + "_" + std::to_string(i);
                db.Put(key, "v");
                std::string tmp;
                db.Get(key, tmp);
            }
        };

        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) threads.emplace_back(worker, i);
        for (auto& t : threads) t.join();

        std::cout << "[OK] Concurrency (Multi-sharding working)\n";
    }

    static void TestWALRecovery() {
        CleanupFiles();
        {
            ShardedKVStore db(4, "test.wal", "test.snap");
            db.Put("check", "it_works");
        } 
        {
            ShardedKVStore db(4, "test.wal", "test.snap");
            std::string val;
            assert(db.Get("check", val) && val == "it_works");
        }
        std::cout << "[OK] WAL Crash Recovery\n";
    }
};

int main() {
    try {
        TestRunner::RunAllTests();
        
        std::cout << "\nDemo mode:\n";
        ShardedKVStore db(8, "app.wal", "app.snap");
        db.Put("user:name", "Erik Peterson");
        db.Put("group", "IU9-12B");
        
        std::string res;
        if(db.Get("user:name", res)) std::cout << "Key 'user:name' -> " << res << "\n";
        
    } catch (const std::exception& e) {
        std::cerr << "Critical error: " << e.what() << "\n";
    }
    return 0;
}
