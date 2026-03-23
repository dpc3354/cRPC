#include "io_context.h"
#include <stdexcept>
#include <cstring>
#include <sys/eventfd.h>
#include <cstdlib>

IoContext::IoContext(unsigned entries, bool enable_registered_bufs,
                   size_t buf_count, size_t buf_size) : running_(false) {
    int ret = io_uring_queue_init(entries, &ring_, 0);
    if (ret < 0) {
        LOG_ERROR("Failed to initialize io_uring: " << strerror(-ret));
        throw std::runtime_error("io_uring init failed");
    }
    LOG_INFO("IoContext initialized with " << entries << " entries.");

    if (enable_registered_bufs) {
        iovecs_.resize(buf_count);

        char* pool = nullptr;
        if (posix_memalign((void**)&pool, 4096, buf_count * buf_size) == 0) {
            for (size_t i = 0; i < buf_count; ++i) {
                iovecs_[i].iov_base = pool + i * buf_size;
                iovecs_[i].iov_len = buf_size;
                free_buf_indices_.push_back(i);
            }
            buf_memory_.push_back(pool);

            ret = io_uring_register_buffers(&ring_, iovecs_.data(), buf_count);
            if (ret < 0) {
                LOG_WARN("io_uring_register_buffers failed (" << strerror(-ret) << "), falling back to normal I/O");
                iovecs_.clear();
                free_buf_indices_.clear();
                free(pool);
                buf_memory_.clear();
            } else {
                buf_size_ = buf_size;
                LOG_INFO("Registered " << buf_count << " fixed buffers of " << buf_size/1024 << "KB each (" << buf_count*buf_size/1024/1024 << "MB total).");
            }
        }
    }
}

IoContext::~IoContext() {
    Stop();
    if (!iovecs_.empty()) {
        io_uring_unregister_buffers(&ring_);
    }
    io_uring_queue_exit(&ring_);

    for (char* p : buf_memory_) {
        free(p);
    }

    // Drain RequestData free-list pool
    while (req_pool_head_) {
        RequestData* next = req_pool_head_->next_free;
        delete req_pool_head_;
        req_pool_head_ = next;
    }

    LOG_INFO("IoContext destroyed.");
}

int IoContext::AllocateBuffer(char** out_ptr) {
    std::lock_guard<std::mutex> lock(buf_mutex_);
    if (free_buf_indices_.empty()) return -1;
    int idx = free_buf_indices_.back();
    free_buf_indices_.pop_back();
    *out_ptr = (char*)iovecs_[idx].iov_base;
    return idx;
}

void IoContext::FreeBuffer(int idx) {
    std::lock_guard<std::mutex> lock(buf_mutex_);
    free_buf_indices_.push_back(idx);
}

void IoContext::Submit(io_uring_sqe* sqe, std::function<void(int res)> callback) {
    auto* req = AllocRequestData(std::move(callback));
    io_uring_sqe_set_data(sqe, req);
    // Do NOT call io_uring_submit() here — the event loop batches all pending
    // SQEs with a single io_uring_submit_and_wait() syscall per iteration.
}

RequestData* IoContext::AllocRequestData(std::function<void(int)> cb) {
    if (req_pool_head_) {
        RequestData* req = req_pool_head_;
        req_pool_head_ = req->next_free;
        req->callback = std::move(cb);
        req->next_free = nullptr;
        return req;
    }
    return new RequestData{std::move(cb)};
}

void IoContext::FreeRequestData(RequestData* req) {
    req->callback = nullptr;
    req->next_free = req_pool_head_;
    req_pool_head_ = req;
}

void IoContext::Run() {
    running_ = true;
    LOG_INFO("IoContext event loop started.");

    io_uring_cqe* cqe;
    while (running_) {
        // Submit all pending SQEs and wait for at least one completion — one
        // syscall covers both submission and blocking wait.
        int ret = io_uring_submit_and_wait(&ring_, 1);
        if (ret < 0) {
            if (-ret == EINTR) continue;
            LOG_ERROR("io_uring_submit_and_wait failed: " << strerror(-ret));
            break;
        }

        // Drain every available CQE in one pass, advancing the ring tail once.
        unsigned head;
        unsigned count = 0;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            if (cqe->user_data) {
                auto* req = reinterpret_cast<RequestData*>(cqe->user_data);
                if (req->callback) req->callback(cqe->res);
                FreeRequestData(req);
            }
            ++count;
        }
        io_uring_cq_advance(&ring_, count);
    }
    LOG_INFO("IoContext event loop stopped.");
}

void IoContext::Stop() {
    running_ = false;
    // Note: If blocked on io_uring_wait_cqe, this might not immediately unblock.
    // In a fully featured version, we use an eventfd to wake it up.
}
