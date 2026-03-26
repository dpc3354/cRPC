#include "io_context.h"
#include "logger.h"
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <numeric>

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
                LOG_INFO("Registered " << buf_count << " fixed buffers of " << buf_size/1024 << "KB each.");
            }
        }
    }

    // Fixed file table: pre-register 1024 empty slots (-1 means unused)
    max_fixed_files_ = 1024;
    std::vector<int> empty_fds(max_fixed_files_, -1);
    if (io_uring_register_files(&ring_, empty_fds.data(), max_fixed_files_) == 0) {
        files_enabled_ = true;
        free_file_indices_.resize(max_fixed_files_);
        std::iota(free_file_indices_.begin(), free_file_indices_.end(), 0);
        LOG_INFO("Registered fixed file table with " << max_fixed_files_ << " slots.");
    } else {
        LOG_WARN("io_uring_register_files failed, fixed file optimization disabled.");
    }
}

IoContext::~IoContext() {
    Stop();
    if (!iovecs_.empty()) {
        io_uring_unregister_buffers(&ring_);
    }
    if (files_enabled_) {
        io_uring_unregister_files(&ring_);
    }
    io_uring_queue_exit(&ring_);

    for (char* p : buf_memory_) free(p);

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

int IoContext::RegisterFile(int fd) {
    if (!files_enabled_ || free_file_indices_.empty()) return -1;
    int idx = free_file_indices_.back();
    free_file_indices_.pop_back();
    if (io_uring_register_files_update(&ring_, idx, &fd, 1) < 0) {
        free_file_indices_.push_back(idx);
        return -1;
    }
    return idx;
}

void IoContext::UnregisterFile(int idx) {
    int neg = -1;
    io_uring_register_files_update(&ring_, idx, &neg, 1);
    free_file_indices_.push_back(idx);
}

RequestData* IoContext::AllocRequestData(std::coroutine_handle<> handle, int* result) {
    if (req_pool_head_) {
        RequestData* req = req_pool_head_;
        req_pool_head_ = req->next_free;
        req->handle = handle;
        req->result = result;
        req->next_free = nullptr;
        return req;
    }
    return new RequestData{handle, result};
}

void IoContext::FreeRequestData(RequestData* req) {
    req->handle  = {};
    req->result  = nullptr;
    req->next_free = req_pool_head_;
    req_pool_head_ = req;
}

void IoContext::Submit(io_uring_sqe* sqe, std::coroutine_handle<> handle, int* result) {
    auto* req = AllocRequestData(handle, result);
    io_uring_sqe_set_data(sqe, req);
    ++in_flight_;
}

void IoContext::Run() {
    running_ = true;
    LOG_INFO("IoContext event loop started.");

    struct __kernel_timespec ts{0, 200'000'000};  // 200ms
    io_uring_cqe* cqe;
    while (running_) {
        int ret = io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, &ts, nullptr);
        if (ret < 0) {
            if (-ret == EINTR || -ret == ETIME) continue;
            LOG_ERROR("io_uring_submit_and_wait_timeout failed: " << strerror(-ret));
            break;
        }

        unsigned head, count = 0;
        io_uring_for_each_cqe(&ring_, head, cqe) {
            if (cqe->user_data) {
                auto* req = reinterpret_cast<RequestData*>(cqe->user_data);
                *req->result = cqe->res;
                auto handle = req->handle;
                FreeRequestData(req);
                --in_flight_;
                handle.resume();
            }
            ++count;
        }
        io_uring_cq_advance(&ring_, count);
    }

    // Drain phase: cancel all in-flight SQEs so suspended coroutines are
    // resumed with -ECANCELED, their frames freed, and held resources
    // (CoroConnection, fixed buffers, fixed file slots) properly released.
    if (in_flight_ > 0) {
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        if (sqe) {
            io_uring_prep_cancel(sqe, nullptr,
                                 IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_ANY);
            io_uring_sqe_set_data(sqe, nullptr);  // not tracked in in_flight_
            io_uring_submit(&ring_);
        }

        struct __kernel_timespec drain_ts{0, 50'000'000};  // 50 ms per iteration
        while (in_flight_ > 0) {
            int ret = io_uring_submit_and_wait_timeout(&ring_, &cqe, 1, &drain_ts, nullptr);
            if (ret < 0) break;

            unsigned head, count = 0;
            io_uring_for_each_cqe(&ring_, head, cqe) {
                if (cqe->user_data) {
                    auto* req = reinterpret_cast<RequestData*>(cqe->user_data);
                    *req->result = cqe->res;
                    auto handle = req->handle;
                    FreeRequestData(req);
                    --in_flight_;
                    handle.resume();
                }
                ++count;
            }
            io_uring_cq_advance(&ring_, count);
        }
    }
    LOG_INFO("IoContext event loop stopped.");
}

void IoContext::Stop() {
    running_ = false;
}
