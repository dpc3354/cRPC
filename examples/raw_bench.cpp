// Raw latency benchmark — no protobuf, no RPC framing.
// Sends N sequential raw byte payloads and measures RTT.
// Use with raw_server to measure pure framework I/O latency.
//
// Usage: ./raw_bench [count] [msg_size] [host] [port]
//   count    - number of requests (default 10000)
//   msg_size - payload bytes (default 64)
//   host     - server address (default 127.0.0.1)
//   port     - server port (default 8081)

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

int main(int argc, char* argv[]) {
    int         count    = (argc > 1) ? std::stoi(argv[1]) : 10000;
    int         msg_size = (argc > 2) ? std::stoi(argv[2]) : 64;
    const char* host     = (argc > 3) ? argv[3] : "127.0.0.1";
    uint16_t    port     = (argc > 4) ? static_cast<uint16_t>(std::stoi(argv[4])) : 8081;

    std::cout << "raw latency bench  host=" << host << "  port=" << port
              << "  count=" << count << "  msg_size=" << msg_size << "\n";

    int fd = ConnectTcp(host, port);

    const std::string payload(msg_size, 'x');
    std::vector<int64_t> latencies_us;
    latencies_us.reserve(count);

    std::vector<char> recv_staging;
    recv_staging.reserve(msg_size * 2);

    for (int i = 0; i < count; ++i) {
        auto t0 = std::chrono::high_resolution_clock::now();
        SendAll(fd, payload.data(), msg_size);
        RecvExact(fd, recv_staging, msg_size);
        auto t1 = std::chrono::high_resolution_clock::now();

        recv_staging.clear();
        latencies_us.push_back(
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count());
    }

    ::close(fd);

    std::sort(latencies_us.begin(), latencies_us.end());
    double avg = static_cast<double>(
        std::accumulate(latencies_us.begin(), latencies_us.end(), int64_t{0})) / count;

    auto percentile = [&](double p) -> int64_t {
        size_t idx = static_cast<size_t>(p / 100.0 * count);
        if (idx >= static_cast<size_t>(count)) idx = count - 1;
        return latencies_us[idx];
    };

    std::cout << "\n--- RTT latency (microseconds) ---\n"
              << "  min  : " << latencies_us.front() << " us\n"
              << "  p50  : " << percentile(50)        << " us\n"
              << "  p90  : " << percentile(90)        << " us\n"
              << "  p99  : " << percentile(99)        << " us\n"
              << "  p99.9: " << percentile(99.9)      << " us\n"
              << "  max  : " << latencies_us.back()   << " us\n"
              << "  avg  : " << avg                   << " us\n"
              << "  total: " << count << " requests\n";
    return 0;
}
