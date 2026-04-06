# Sharded In-Memory Key-Value Store (C++23)

A high-performance, thread-safe in-memory key-value storage engine featuring data sharding and Write-Ahead Logging (WAL) for fault tolerance.

##  Key Features

- **Sharded Architecture:** Uses a sharded `unordered_map` structure to minimize lock contention and improve throughput in multi-threaded environments.
- **Thread Safety:** Implements fine-grained locking using `std::shared_mutex` (Readers-Writer lock pattern).
- **Durability (WAL):** Write-Ahead Logging ensures that all operations are persisted to disk and can be recovered after a crash.
- **C++23 Standard:** Leverages modern C++ features for efficiency and safety.
- **Crash Recovery:** Automatic state restoration from WAL files on startup.

##  Tech Stack
- **Language:** C++23
- **Tools:** CMake, GCC/Clang
- **Concepts:** Multithreading, Sharding, File I/O Serialization, RAII.

##  How to Build and Run

### Prerequisites
- A C++23 compatible compiler (GCC 12+ or Clang 15+).
- CMake 3.10 or higher.

### Build
```bash
mkdir build && cd build
cmake ..
make
