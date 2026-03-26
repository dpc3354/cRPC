#pragma once

#include <liburing.h>
#include <coroutine>
#include <atomic>
#include <vector>
#include <mutex>
#include <sys/uio.h>
#include "logger.h"

struct RequestData {
    std::coroutine_handle<> handle;
    int* result;
    RequestData* next_free = nullptr;  // intrusive free-list link
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

    void Submit(io_uring_sqe* sqe, std::coroutine_handle<> handle, int* result);

    size_t BufSize() const { return buf_size_; }

    // Fixed Buffer Memory Pool
    int AllocateBuffer(char** out_ptr);
    void FreeBuffer(int index);

    // Fixed File Table
    int  RegisterFile(int fd);   // returns fixed index, or -1 on failure
    void UnregisterFile(int idx);

private:
    RequestData* AllocRequestData(std::coroutine_handle<> handle, int* result);
    void FreeRequestData(RequestData* req);

    io_uring ring_;
    std::atomic<bool> running_;

    size_t buf_size_ = 0;
    std::vector<struct iovec> iovecs_;
    std::vector<char*> buf_memory_;
    std::vector<int> free_buf_indices_;
    std::mutex buf_mutex_;

    RequestData* req_pool_head_ = nullptr;

    bool files_enabled_ = false;
    int  max_fixed_files_ = 0;
    std::vector<int> free_file_indices_;
};
