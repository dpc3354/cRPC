# cRPC

A lightweight C++20 RPC framework built on `io_uring` for high-performance, low-latency network communication on Linux.

## Features

- **io_uring**-based async I/O — batched syscalls, minimal kernel transitions
- **Fixed registered buffers** — eliminates per-I/O page-pinning overhead
- **Object pool** for I/O request metadata — zero hot-path heap allocation in the framework layer
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

**Protobuf echo server + latency benchmark:**

```bash
# Terminal 1
./build/examples/echo_server

# Terminal 2
./build/examples/latency_bench [count] [msg_size]
# e.g.: ./build/examples/latency_bench 50000 256
```

**Raw echo server + raw benchmark (pure framework latency):**

```bash
# Terminal 1
./build/examples/raw_server

# Terminal 2
./build/examples/raw_bench [count] [msg_size]
# e.g.: ./build/examples/raw_bench 50000 256
```

## Benchmark

Measured on a Linux server (Ubuntu 24.04, kernel 6.8), loopback, single connection, sequential ping-pong, 256-byte payload:

| Benchmark | p50 | p90 | p99 | p99.9 |
|-----------|-----|-----|-----|-------|
| raw (framework only) | 42 µs | 48 µs | 64 µs | 121 µs |
| with protobuf echo   | 78 µs | 138 µs | 217 µs | 364 µs |

The raw benchmark isolates the framework's I/O path (io_uring read → callback → io_uring write). The difference between the two reflects protobuf serialization and handler-side heap allocation overhead.

## Writing a Server

```cpp
#include "io_context.h"
#include "tcp_server.h"

int main() {
    IoContext io_ctx;
    TcpServer server(&io_ctx, 8080);

    server.SetMessageCallback([](std::shared_ptr<Connection> conn, Buffer& buffer) {
        // process buffer, send response via conn->Send(...)
        buffer.HasRead(buffer.ReadableBytes());
    });

    server.Start();
    io_ctx.Run();
}
```
