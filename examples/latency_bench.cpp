// Latency benchmark client for cRPC echo server.
// Connects to localhost:8080, sends N sequential ping-pong requests,
// and reports RTT statistics (min/avg/p50/p99/max).
//
// Usage: ./latency_bench [count] [msg_size]
//   count    - number of requests (default 10000)
//   msg_size - payload string length (default 64)

#include "protocol.h"
#include "rpc_message.pb.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
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

// Blocking full-write
static void SendAll(int fd, const void* data, size_t len) {
    const char* ptr = static_cast<const char*>(data);
    while (len > 0) {
        ssize_t n = ::send(fd, ptr, len, MSG_NOSIGNAL);
        if (n <= 0) throw std::runtime_error("send failed");
        ptr += n;
        len -= static_cast<size_t>(n);
    }
}

// Blocking full-read into a growing staging buffer, returns when `need` bytes are available
static void RecvExact(int fd, std::vector<char>& buf, size_t need) {
    while (buf.size() < need) {
        char tmp[4096];
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) throw std::runtime_error("recv failed / connection closed");
        buf.insert(buf.end(), tmp, tmp + n);
    }
}

// ---------- main ----------

int main(int argc, char* argv[]) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    int  count    = (argc > 1) ? std::stoi(argv[1]) : 10000;
    int  msg_size = (argc > 2) ? std::stoi(argv[2]) : 64;
    const char* host = "127.0.0.1";
    uint16_t    port = 8080;

    std::cout << "cRPC latency bench  host=" << host << "  port=" << port
              << "  count=" << count << "  msg_size=" << msg_size << "\n";

    int fd = ConnectTcp(host, port);

    const std::string payload_str(msg_size, 'x');
    std::vector<int64_t> latencies_us;
    latencies_us.reserve(count);

    // Staging buffer for incoming bytes
    std::vector<char> recv_staging;
    recv_staging.reserve(4096);

    for (int i = 0; i < count; ++i) {
        // --- Build request ---
        crpc::EchoRequest req;
        req.set_message(payload_str);
        std::string req_bytes;
        req.SerializeToString(&req_bytes);

        RpcHeader hdr{};
        hdr.magic       = Protocol::MAGIC_NUMBER;
        hdr.version     = 1;
        hdr.msg_type    = 0;          // Request
        hdr.seq_id      = static_cast<uint32_t>(i);
        hdr.payload_len = static_cast<uint32_t>(req_bytes.size());

        Buffer send_buf;
        Protocol::EncodeMessage(send_buf, hdr, req_bytes);

        // --- Send & time ---
        auto t0 = std::chrono::high_resolution_clock::now();
        SendAll(fd, send_buf.ReadBegin(), send_buf.ReadableBytes());

        // --- Receive header (wire format is network byte order) ---
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

        // --- Receive payload ---
        RecvExact(fd, recv_staging, resp_hdr.payload_len);
        recv_staging.erase(recv_staging.begin(),
                           recv_staging.begin() + resp_hdr.payload_len);

        auto t1 = std::chrono::high_resolution_clock::now();
        int64_t us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
        latencies_us.push_back(us);
    }

    ::close(fd);
    google::protobuf::ShutdownProtobufLibrary();

    // --- Stats ---
    std::sort(latencies_us.begin(), latencies_us.end());
    double avg = static_cast<double>(
        std::accumulate(latencies_us.begin(), latencies_us.end(), int64_t{0}))
        / count;

    auto percentile = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * count);
        if (idx >= static_cast<size_t>(count)) idx = count - 1;
        return latencies_us[idx];
    };

    std::cout << "\n--- RTT latency (microseconds) ---\n"
              << "  min  : " << latencies_us.front()  << " us\n"
              << "  p50  : " << percentile(50)         << " us\n"
              << "  p90  : " << percentile(90)         << " us\n"
              << "  p99  : " << percentile(99)         << " us\n"
              << "  p99.9: " << percentile(99.9)       << " us\n"
              << "  max  : " << latencies_us.back()    << " us\n"
              << "  avg  : " << avg                    << " us\n"
              << "  total: " << count << " requests\n";
    return 0;
}
