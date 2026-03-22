#include "io_context.h"
#include <stdexcept>
#include <cstring>
#include <sys/eventfd.h>

IoContext::IoContext(unsigned entries) : running_(false) {
    int ret = io_uring_queue_init(entries, &ring_, 0);
    if (ret < 0) {
        LOG_ERROR("Failed to initialize io_uring: " << strerror(-ret));
        throw std::runtime_error("io_uring init failed");
    }
    LOG_INFO("IoContext initialized with " << entries << " entries.");
}

IoContext::~IoContext() {
    Stop();
    io_uring_queue_exit(&ring_);
    LOG_INFO("IoContext destroyed.");
}

void IoContext::Submit(io_uring_sqe* sqe, std::function<void(int res)> callback) {
    auto req = new RequestData{std::move(callback)};
    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&ring_); // For simplicity we submit immediately
}

void IoContext::Run() {
    running_ = true;
    LOG_INFO("IoContext event loop started.");

    io_uring_cqe* cqe;
    while (running_) {
        int ret = io_uring_wait_cqe(&ring_, &cqe);
        if (ret < 0) {
            if (-ret == EINTR) continue;
            LOG_ERROR("io_uring_wait_cqe failed: " << strerror(-ret));
            break;
        }

        if (cqe->user_data) {
            auto req = reinterpret_cast<RequestData*>(cqe->user_data);
            if (req->callback) {
                req->callback(cqe->res);
            }
            delete req;
        }

        io_uring_cqe_seen(&ring_, cqe);
    }
    LOG_INFO("IoContext event loop stopped.");
}

void IoContext::Stop() {
    running_ = false;
    // Note: If blocked on io_uring_wait_cqe, this might not immediately unblock.
    // In a fully featured version, we use an eventfd to wake it up.
}
