// Pipelined (multiplexed) RPC benchmark.
//
// Sends up to `window` requests in-flight simultaneously on ONE connection,
// receives responses concurrently in a separate thread, and uses seq_id to
// match each response back to its original request for per-request RTT.
//
// Compare with latency_bench (window=1, sequential ping-pong):
//   window=1   → same as latency_bench, RTT-bound
//   window=64  → throughput-bound, much higher req/s
//
// Usage: ./multiplex_bench [count] [window] [msg_size]
//   count    - total requests          (default 100000)
//   window   - max in-flight requests  (default 64)
//   msg_size - payload string length   (default 64)

#include "protocol.h"
#include "rpc_message.pb.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using Clock     = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;

// ---------- helpers ----------

static int ConnectTcp(const char* host, uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");
    int flag = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (::inet_pton(AF_INET, host, &addr.sin_addr) != 1)
        throw std::runtime_error("inet_pton failed");
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
        throw std::runtime_error(std::string("connect() failed: ") + strerror(errno));
    return fd;
}

static void SendAll(int fd, const void* data, size_t len) {
    const char* ptr = static_cast<const char*>(data);
    while (len > 0) {
        ssize_t n = ::send(fd, ptr, len, MSG_NOSIGNAL);
        if (n <= 0) throw std::runtime_error("send failed");
        ptr += n;
        len -= static_cast<size_t>(n);
    }
}

// Blocks until `buf` contains at least `need` bytes.
static void RecvExact(int fd, std::vector<char>& buf, size_t need) {
    while (buf.size() < need) {
        char tmp[65536];
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) throw std::runtime_error("recv failed / connection closed");
        buf.insert(buf.end(), tmp, tmp + n);
    }
}

// ---------- main ----------

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int         count    = (argc > 1) ? std::stoi(argv[1]) : 100000;
    int         window   = (argc > 2) ? std::stoi(argv[2]) : 64;
    int         msg_size = (argc > 3) ? std::stoi(argv[3]) : 64;
    const char* host     = "127.0.0.1";
    uint16_t    port     = 8080;

    std::cout << "multiplex_bench"
              << "  count="    << count
              << "  window="   << window
              << "  msg_size=" << msg_size << "\n";

    int fd = ConnectTcp(host, port);

    // seq_id → send timestamp; shared between sender and receiver threads.
    std::mutex                              pending_mu;
    std::unordered_map<uint32_t, TimePoint> pending;
    pending.reserve(static_cast<size_t>(window) * 2);

    // Receiver is the sole writer; main reads only after receiver.join().
    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<size_t>(count));

    // Sliding-window counter: sender blocks when in_flight >= window.
    std::atomic<int>  in_flight{0};
    std::atomic<bool> recv_error{false};

    // ---------- receiver thread ----------
    std::thread receiver([&] {
        std::vector<char> buf;
        buf.reserve(1 << 16);
        try {
            for (int i = 0; i < count; ++i) {
                RecvExact(fd, buf, Protocol::HEADER_SIZE);

                RpcHeader hdr;
                std::memcpy(&hdr, buf.data(), Protocol::HEADER_SIZE);
                buf.erase(buf.begin(), buf.begin() + Protocol::HEADER_SIZE);

                hdr.magic       = ntohs(hdr.magic);
                hdr.seq_id      = ntohl(hdr.seq_id);
                hdr.payload_len = ntohl(hdr.payload_len);

                if (hdr.magic != Protocol::MAGIC_NUMBER)
                    throw std::runtime_error("bad magic in response");

                RecvExact(fd, buf, hdr.payload_len);
                buf.erase(buf.begin(), buf.begin() + hdr.payload_len);

                auto now = Clock::now();

                {
                    std::lock_guard<std::mutex> lk(pending_mu);
                    auto it = pending.find(hdr.seq_id);
                    if (it != pending.end()) {
                        int64_t us = std::chrono::duration_cast<
                            std::chrono::microseconds>(now - it->second).count();
                        latencies.push_back(us);
                        pending.erase(it);
                    }
                }
                in_flight.fetch_sub(1, std::memory_order_release);
            }
        } catch (const std::exception& e) {
            std::cerr << "[receiver] " << e.what() << "\n";
            recv_error.store(true);
        }
    });

    // ---------- sender loop ----------
    const std::string payload_str(static_cast<size_t>(msg_size), 'x');
    auto wall_start = Clock::now();

    try {
        for (int i = 0; i < count && !recv_error.load(); ++i) {
            // Block until a window slot opens up.
            while (in_flight.load(std::memory_order_acquire) >= window)
                std::this_thread::yield();

            crpc::EchoRequest req;
            req.set_message(payload_str);
            std::string req_bytes;
            req.SerializeToString(&req_bytes);

            RpcHeader hdr{};
            hdr.magic       = Protocol::MAGIC_NUMBER;
            hdr.version     = 1;
            hdr.msg_type    = 0;
            hdr.seq_id      = static_cast<uint32_t>(i);
            hdr.payload_len = static_cast<uint32_t>(req_bytes.size());

            Buffer send_buf;
            Protocol::EncodeMessage(send_buf, hdr, req_bytes);

            // Record time and increment counter BEFORE sending so the receiver
            // can always find the entry in the pending map.
            auto send_time = Clock::now();
            {
                std::lock_guard<std::mutex> lk(pending_mu);
                pending[hdr.seq_id] = send_time;
            }
            in_flight.fetch_add(1, std::memory_order_release);

            SendAll(fd, send_buf.ReadBegin(), send_buf.ReadableBytes());
        }
    } catch (const std::exception& e) {
        std::cerr << "[sender] " << e.what() << "\n";
    }

    receiver.join();
    auto wall_end = Clock::now();
    ::close(fd);

    if (latencies.empty()) {
        std::cerr << "No latency data collected.\n";
        return 1;
    }

    // ---------- stats ----------
    std::sort(latencies.begin(), latencies.end());
    double wall_sec = std::chrono::duration<double>(wall_end - wall_start).count();
    double total    = static_cast<double>(latencies.size());
    double avg      = static_cast<double>(
        std::accumulate(latencies.begin(), latencies.end(), int64_t{0})) / total;

    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * total);
        if (idx >= latencies.size()) idx = latencies.size() - 1;
        return latencies[idx];
    };

    std::cout << "\n--- RTT (microseconds) ---\n"
              << "  min  : " << latencies.front() << " us\n"
              << "  p50  : " << pct(50)            << " us\n"
              << "  p90  : " << pct(90)            << " us\n"
              << "  p99  : " << pct(99)            << " us\n"
              << "  p99.9: " << pct(99.9)          << " us\n"
              << "  max  : " << latencies.back()   << " us\n"
              << "  avg  : " << avg                << " us\n"
              << "\n--- Throughput ---\n"
              << "  wall time : " << wall_sec             << " s\n"
              << "  req/s     : " << total / wall_sec     << "\n"
              << "  total reqs: " << latencies.size()     << "\n";

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
