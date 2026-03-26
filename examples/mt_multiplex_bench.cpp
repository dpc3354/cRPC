// Multi-connection multiplexed benchmark.
//
// Opens `nconn` connections simultaneously, each running a sliding-window
// pipeline of `window` in-flight requests.  Combines the concurrency of
// latency_bench_mt (multiple connections → multiple server threads) with
// the pipelining of multiplex_bench (window > 1 per connection).
//
// Each connection has one sender thread (the outer worker) and one receiver
// thread, so total client threads = 2 * nconn.
//
// Usage: ./mt_multiplex_bench [count] [nconn] [window] [msg_size]
//   count    - requests per connection     (default 50000)
//   nconn    - number of connections       (default 8)
//   window   - max in-flight per conn      (default 64)
//   msg_size - payload string length       (default 64)

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

static void RecvExact(int fd, std::vector<char>& buf, size_t need) {
    while (buf.size() < need) {
        char tmp[65536];
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) throw std::runtime_error("recv failed / connection closed");
        buf.insert(buf.end(), tmp, tmp + n);
    }
}

// ---------- per-connection worker ----------

struct ConnResult {
    std::vector<int64_t> latencies;
    std::string          error;
};

// Called from an outer worker thread (acts as sender).
// Spawns one inner receiver thread per connection.
static void RunConnection(int conn_id, const char* host, uint16_t port,
                          int count, int window, int msg_size,
                          std::atomic<bool>& start_flag,
                          ConnResult& result) {
    try {
        int fd = ConnectTcp(host, port);

        std::mutex                              pending_mu;
        std::unordered_map<uint32_t, TimePoint> pending;
        pending.reserve(static_cast<size_t>(window) * 2);

        std::vector<int64_t> latencies;
        latencies.reserve(static_cast<size_t>(count));

        std::atomic<int>  in_flight{0};
        std::atomic<bool> recv_error{false};

        // Receiver thread: reads responses, matches seq_id, records RTT.
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
                        throw std::runtime_error("bad magic");

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
                std::cerr << "[conn " << conn_id << " rx] " << e.what() << "\n";
                recv_error.store(true);
            }
        });

        // Wait for the global start signal so all connections begin together.
        while (!start_flag.load(std::memory_order_acquire))
            std::this_thread::yield();

        // Sender loop (runs on the outer worker thread).
        const std::string payload_str(static_cast<size_t>(msg_size), 'x');
        try {
            for (int i = 0; i < count && !recv_error.load(); ++i) {
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

                auto send_time = Clock::now();
                {
                    std::lock_guard<std::mutex> lk(pending_mu);
                    pending[hdr.seq_id] = send_time;
                }
                in_flight.fetch_add(1, std::memory_order_release);
                SendAll(fd, send_buf.ReadBegin(), send_buf.ReadableBytes());
            }
        } catch (const std::exception& e) {
            std::cerr << "[conn " << conn_id << " tx] " << e.what() << "\n";
        }

        receiver.join();
        ::close(fd);
        result.latencies = std::move(latencies);

    } catch (const std::exception& e) {
        result.error = e.what();
    }
}

// ---------- stats ----------

static void PrintStats(const std::vector<int64_t>& v, double wall_sec) {
    if (v.empty()) { std::cout << "  (no data)\n"; return; }

    double total = static_cast<double>(v.size());
    double avg   = static_cast<double>(
        std::accumulate(v.begin(), v.end(), int64_t{0})) / total;

    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * total);
        if (idx >= v.size()) idx = v.size() - 1;
        return v[idx];
    };

    std::cout << "  min  : " << v.front() << " us\n"
              << "  p50  : " << pct(50)   << " us\n"
              << "  p90  : " << pct(90)   << " us\n"
              << "  p99  : " << pct(99)   << " us\n"
              << "  p99.9: " << pct(99.9) << " us\n"
              << "  max  : " << v.back()  << " us\n"
              << "  avg  : " << avg       << " us\n"
              << "  total: " << v.size()  << " requests\n"
              << "\n--- Throughput ---\n"
              << "  wall time : " << wall_sec           << " s\n"
              << "  req/s     : " << total / wall_sec   << "\n";
}

// ---------- main ----------

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int         count    = (argc > 1) ? std::stoi(argv[1]) : 50000;
    int         nconn    = (argc > 2) ? std::stoi(argv[2]) : 8;
    int         window   = (argc > 3) ? std::stoi(argv[3]) : 64;
    int         msg_size = (argc > 4) ? std::stoi(argv[4]) : 64;
    const char* host     = "127.0.0.1";
    uint16_t    port     = 8080;

    std::cout << "mt_multiplex_bench"
              << "  count="    << count
              << "  nconn="    << nconn
              << "  window="   << window
              << "  msg_size=" << msg_size << "\n";

    std::vector<ConnResult>  results(static_cast<size_t>(nconn));
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(nconn));

    std::atomic<bool> start_flag{false};

    for (int i = 0; i < nconn; ++i)
        workers.emplace_back(RunConnection,
                             i, host, port, count, window, msg_size,
                             std::ref(start_flag), std::ref(results[i]));

    // Let all connections finish the TCP handshake before firing.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto wall_start = Clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& w : workers) w.join();
    auto wall_end = Clock::now();

    double wall_sec = std::chrono::duration<double>(wall_end - wall_start).count();

    // Check errors
    for (int i = 0; i < nconn; ++i)
        if (!results[i].error.empty())
            std::cerr << "[conn " << i << "] error: " << results[i].error << "\n";

    // Merge all latency samples
    std::vector<int64_t> all;
    all.reserve(static_cast<size_t>(count) * nconn);
    for (auto& r : results)
        all.insert(all.end(), r.latencies.begin(), r.latencies.end());
    std::sort(all.begin(), all.end());

    std::cout << "\n--- RTT (microseconds, all connections merged) ---\n";
    PrintStats(all, wall_sec);

    // Per-connection p99
    std::cout << "\n--- Per-connection p99 (us) ---\n";
    for (int i = 0; i < nconn; ++i) {
        auto& v = results[i].latencies;
        if (v.empty()) { std::cout << "  conn " << i << ": (no data)\n"; continue; }
        std::sort(v.begin(), v.end());
        size_t idx = static_cast<size_t>(0.99 * v.size());
        if (idx >= v.size()) idx = v.size() - 1;
        std::cout << "  conn " << i << ": " << v[idx] << " us\n";
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
