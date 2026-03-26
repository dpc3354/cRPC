// Multi-threaded latency benchmark for cRPC echo server.
// Each thread owns one persistent TCP connection and runs sequential ping-pong.
// All per-thread latency samples are merged for global statistics.
//
// Usage: ./latency_bench_mt [count] [msg_size] [threads]
//   count    - requests per thread (default 10000)
//   msg_size - payload string length (default 64)
//   threads  - number of client threads / connections (default 4)

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
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
        char tmp[4096];
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) throw std::runtime_error("recv failed / connection closed");
        buf.insert(buf.end(), tmp, tmp + n);
    }
}

// ---------- per-thread worker ----------

struct ThreadResult {
    std::vector<int64_t> latencies_us;
    std::string          error;          // non-empty on failure
};

static void WorkerThread(
    int thread_id,
    const char* host, uint16_t port,
    int count, int msg_size,
    std::atomic<bool>& start_flag,
    ThreadResult& result)
{
    try {
        int fd = ConnectTcp(host, port);

        const std::string payload_str(msg_size, 'x');
        result.latencies_us.reserve(count);

        std::vector<char> recv_staging;
        recv_staging.reserve(4096);

        // 等待主线程发出统一开始信号，让所有线程尽量同时开始
        while (!start_flag.load(std::memory_order_acquire))
            std::this_thread::yield();

        for (int i = 0; i < count; ++i) {
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

            auto t0 = std::chrono::high_resolution_clock::now();
            SendAll(fd, send_buf.ReadBegin(), send_buf.ReadableBytes());

            RecvExact(fd, recv_staging, Protocol::HEADER_SIZE);

            RpcHeader resp_hdr;
            std::memcpy(&resp_hdr, recv_staging.data(), Protocol::HEADER_SIZE);
            recv_staging.erase(recv_staging.begin(),
                               recv_staging.begin() + Protocol::HEADER_SIZE);

            resp_hdr.magic       = ntohs(resp_hdr.magic);
            resp_hdr.seq_id      = ntohl(resp_hdr.seq_id);
            resp_hdr.payload_len = ntohl(resp_hdr.payload_len);

            if (resp_hdr.magic != Protocol::MAGIC_NUMBER)
                throw std::runtime_error("bad magic in response");

            RecvExact(fd, recv_staging, resp_hdr.payload_len);
            recv_staging.erase(recv_staging.begin(),
                               recv_staging.begin() + resp_hdr.payload_len);

            auto t1 = std::chrono::high_resolution_clock::now();
            result.latencies_us.push_back(
                std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
        }

        ::close(fd);
    } catch (const std::exception& e) {
        result.error = e.what();
    }
}

// ---------- stats helpers ----------

static void PrintStats(const std::vector<int64_t>& sorted, const char* label) {
    if (sorted.empty()) return;

    size_t n = sorted.size();
    double avg = static_cast<double>(
        std::accumulate(sorted.begin(), sorted.end(), int64_t{0})) / static_cast<double>(n);

    auto pct = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * n);
        if (idx >= n) idx = n - 1;
        return sorted[idx];
    };

    std::cout << label << "\n"
              << "  min  : " << sorted.front() << " us\n"
              << "  p50  : " << pct(50)         << " us\n"
              << "  p90  : " << pct(90)         << " us\n"
              << "  p99  : " << pct(99)         << " us\n"
              << "  p99.9: " << pct(99.9)       << " us\n"
              << "  max  : " << sorted.back()   << " us\n"
              << "  avg  : " << avg             << " us\n"
              << "  total: " << n               << " requests\n";
}

// ---------- main ----------

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int         count    = (argc > 1) ? std::stoi(argv[1]) : 10000;
    int         msg_size = (argc > 2) ? std::stoi(argv[2]) : 64;
    int         nthreads = (argc > 3) ? std::stoi(argv[3]) : 4;
    const char* host     = "127.0.0.1";
    uint16_t    port     = 8080;

    std::cout << "cRPC multi-thread latency bench"
              << "  host="     << host
              << "  port="     << port
              << "  count="    << count
              << "  msg_size=" << msg_size
              << "  threads="  << nthreads << "\n";

    std::vector<ThreadResult> results(nthreads);
    std::vector<std::thread>  threads;
    threads.reserve(nthreads);

    std::atomic<bool> start_flag{false};

    for (int t = 0; t < nthreads; ++t)
        threads.emplace_back(WorkerThread,
                             t, host, port, count, msg_size,
                             std::ref(start_flag), std::ref(results[t]));

    // 给线程时间完成连接建立，然后统一放行
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    start_flag.store(true, std::memory_order_release);

    auto wall_start = std::chrono::high_resolution_clock::now();
    for (auto& th : threads) th.join();
    auto wall_end   = std::chrono::high_resolution_clock::now();

    // 检查错误
    for (int t = 0; t < nthreads; ++t) {
        if (!results[t].error.empty())
            std::cerr << "[thread " << t << "] error: " << results[t].error << "\n";
    }

    // 合并所有延迟样本
    std::vector<int64_t> all_latencies;
    all_latencies.reserve(static_cast<size_t>(count) * nthreads);
    for (auto& r : results)
        all_latencies.insert(all_latencies.end(), r.latencies_us.begin(), r.latencies_us.end());

    std::sort(all_latencies.begin(), all_latencies.end());

    double wall_sec = std::chrono::duration<double>(wall_end - wall_start).count();
    double total_reqs = static_cast<double>(all_latencies.size());

    std::cout << "\n--- Global RTT (microseconds, all threads merged) ---\n";
    PrintStats(all_latencies, "");

    std::cout << "\n--- Throughput ---\n"
              << "  wall time : " << wall_sec       << " s\n"
              << "  req/s     : " << total_reqs / wall_sec << "\n";

    // 每线程单独统计
    std::cout << "\n--- Per-thread p99 (us) ---\n";
    for (int t = 0; t < nthreads; ++t) {
        auto& v = results[t].latencies_us;
        if (v.empty()) { std::cout << "  thread " << t << ": (no data)\n"; continue; }
        std::sort(v.begin(), v.end());
        size_t idx = static_cast<size_t>(0.99 * v.size());
        if (idx >= v.size()) idx = v.size() - 1;
        std::cout << "  thread " << t << ": " << v[idx] << " us\n";
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
