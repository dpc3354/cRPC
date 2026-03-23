#pragma once

#include <liburing.h>
#include <functional>
#include <atomic>
#include <vector>
#include <mutex>
#include <sys/uio.h>
#include "logger.h"

struct RequestData {
    std::function<void(int res)> callback;
    RequestData* next_free = nullptr; // intrusive free-list link
};

class IoContext {
public:
    IoContext(unsigned entries = 1024, bool enable_registered_bufs = true,
              size_t buf_count = 64, size_t buf_size = 65536);
    ~IoContext();

    IoContext(const IoContext&) = delete;
    IoContext& operator=(const IoContext&) = delete;

    void Run();
    void Stop();

    io_uring* GetRing() { return &ring_; }

    void Submit(io_uring_sqe* sqe, std::function<void(int res)> callback);

    size_t BufSize() const { return buf_size_; }

    // Fixed Buffer Memory Pool
    int AllocateBuffer(char** out_ptr);
    void FreeBuffer(int index);

    // RequestData object pool (free list, single-threaded event loop only)
    RequestData* AllocRequestData(std::function<void(int)> cb);
    void FreeRequestData(RequestData* req);

private:
    io_uring ring_;
    std::atomic<bool> running_;

    size_t buf_size_ = 0;
    std::vector<struct iovec> iovecs_;
    std::vector<char*> buf_memory_;
    std::vector<int> free_buf_indices_;
    std::mutex buf_mutex_;

    RequestData* req_pool_head_ = nullptr; // free-list head
};
