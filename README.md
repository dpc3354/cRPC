# cRPC

A lightweight C++20 RPC framework built on `io_uring` for high-performance, low-latency network communication on Linux.

## Features

- **io_uring**-based async I/O — batched syscalls, minimal kernel transitions
- **Fixed registered buffers** — eliminates per-I/O page-pinning overhead
- **Fixed registered files** — bypasses fd table RCU lookup on every I/O operation
- **Multi-threaded with SO_REUSEPORT** — each thread owns an independent io_uring ring and listen socket; kernel distributes connections with no user-space locking
- **CPU affinity** — each worker thread is pinned to a dedicated core
- **Object pool** for I/O request metadata — zero hot-path heap allocation in the framework layer
- C++20 coroutine connection handler — sequential-looking code with no callbacks
- `TCP_NODELAY` on all connections — no Nagle algorithm delay
- Protobuf wire protocol with a compact binary framing header

## Requirements

- Linux kernel 5.1+ (6.x recommended)
- Clang with C++20 support
- CMake 3.16+
- liburing
- protobuf + protoc (for the echo server example)

## Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

## Wire Protocol

Each message is prefixed with a 10-byte header (network byte order):

| Field       | Size | Description                  |
|-------------|------|------------------------------|
| magic       | 2B   | `0x5452` (`TR`)              |
| version     | 1B   | Protocol version             |
| msg_type    | 1B   | `0` = Request, `1` = Response |
| seq_id      | 4B   | Request sequence number      |
| payload_len | 4B   | Payload length in bytes      |

## Running the Examples

**Coroutine echo server + single-connection latency benchmark:**

```bash
# Terminal 1 — starts one worker thread per logical CPU core
./build/examples/coro_echo

# Terminal 2 — sequential ping-pong, single connection
./build/examples/latency_bench [count] [msg_size]
# e.g.: ./build/examples/latency_bench 50000 256
```

**Multi-threaded latency benchmark (concurrent connections):**

```bash
# Terminal 2 — N threads each holding one persistent connection
./build/examples/latency_bench_mt [count] [msg_size] [threads]
# e.g.: ./build/examples/latency_bench_mt 50000 256 32
```

## Benchmark

**Test environment**

| Item | Detail |
|------|--------|
| CPU | Intel Core i7-11800H @ 2.30 GHz (8C/16T), 4 vCPUs allocated to WSL2 |
| Memory | 8 GB |
| OS | WSL2 — Linux 6.6.87.2-microsoft-standard-WSL2 |
| Transport | Loopback (127.0.0.1) |
| Payload | 256 bytes |
| Workload | 32 concurrent connections, 50 000 requests per connection |

**Optimization progression** (32 client threads × 50 000 requests = 1.6 M total, Debug build)

| Configuration | req/s | p50 | p90 | p99 | p99.9 |
|---------------|------:|----:|----:|----:|------:|
| Single-threaded server (baseline) | 58,720 | 554 µs | 585 µs | 746 µs | — |
| + SO_REUSEPORT multi-threaded | 229,658 | 82 µs | 197 µs | 342 µs | 1866 µs |
| + CPU affinity (pin per core) | 256,195 | 88 µs | 172 µs | 424 µs | 1663 µs |
| + Fixed registered files | 244,722 | 88 µs | 173 µs | 295 µs | 1305 µs |

SO_REUSEPORT removes the single-threaded server bottleneck (4× throughput gain). CPU affinity reduces scheduler migration overhead. Fixed registered files eliminates per-operation fd table RCU lookups, primarily benefiting tail latency (p99 −30%, p99.9 −21% vs previous step).

**Release build — final configuration** (`-DCMAKE_BUILD_TYPE=Release`, same workload)

| req/s | p50 | p90 | p99 | p99.9 | avg |
|------:|----:|----:|----:|------:|----:|
| 331,784 | 66 µs | 128 µs | 231 µs | 713 µs | 76.6 µs |

Release optimizations (coroutine frame inlining, protobuf serialization) add a further ~35% throughput gain and halve tail latency vs the Debug build.

## Writing a Server

```cpp
#include "io_context.h"
#include "coro_tcp_server.h"
#include "coro_connection.h"

Task handleConnection(std::shared_ptr<CoroConnection> conn) {
    while (true) {
        int n = co_await conn->Read();
        if (n <= 0) break;

        auto& buf = conn->GetReadBuffer();
        // ... parse buf, build response ...
        co_await conn->Write(response_data, response_len);
    }
}

int main() {
    IoContext io_ctx;
    CoroTcpServer server(&io_ctx, 8080);
    server.SetConnectionHandler(handleConnection);
    server.Start();
    io_ctx.Run();
}
```
