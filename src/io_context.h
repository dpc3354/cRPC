#pragma once

#include <liburing.h>
#include <functional>
#include <atomic>
#include "logger.h"

struct RequestData {
    std::function<void(int res)> callback;
};

class IoContext {
public:
    IoContext(unsigned entries = 1024);
    ~IoContext();

    IoContext(const IoContext&) = delete;
    IoContext& operator=(const IoContext&) = delete;

    void Run();
    void Stop();

    io_uring* GetRing() { return &ring_; }

    void Submit(io_uring_sqe* sqe, std::function<void(int res)> callback);

private:
    io_uring ring_;
    std::atomic<bool> running_;
};
