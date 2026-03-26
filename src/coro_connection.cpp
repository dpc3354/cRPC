#include "coro_connection.h"
#include "logger.h"
#include <algorithm>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

CoroConnection::CoroConnection(IoContext* io_ctx, int fd)
    : io_ctx_(io_ctx), fd_(fd), closed_(false),
      fixed_file_idx_(-1),
      read_buf_idx_(-1), read_fixed_ptr_(nullptr),
      write_buf_idx_(-1), write_fixed_ptr_(nullptr) {

    int flag = 1;
    if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0)
        LOG_WARN("CoroConnection fd=" << fd << ": failed to set TCP_NODELAY");

    fixed_file_idx_ = io_ctx_->RegisterFile(fd_);
    if (fixed_file_idx_ >= 0)
        LOG_INFO("CoroConnection fd=" << fd_ << " registered as fixed file #" << fixed_file_idx_);

    read_buf_idx_ = io_ctx_->AllocateBuffer(&read_fixed_ptr_);
    if (read_buf_idx_ >= 0) {
        read_buffer_ = Buffer(read_fixed_ptr_, io_ctx_->BufSize());
        LOG_INFO("CoroConnection fd=" << fd_ << " using fixed read buffer #" << read_buf_idx_);
    } else {
        read_buffer_ = Buffer(65536);
        LOG_WARN("CoroConnection fd=" << fd_ << ": no fixed read buffer, using dynamic");
    }

    write_buf_idx_ = io_ctx_->AllocateBuffer(&write_fixed_ptr_);
    if (write_buf_idx_ >= 0) {
        write_buffer_ = Buffer(write_fixed_ptr_, io_ctx_->BufSize());
        LOG_INFO("CoroConnection fd=" << fd_ << " using fixed write buffer #" << write_buf_idx_);
    } else {
        write_buffer_ = Buffer(8192);
        LOG_WARN("CoroConnection fd=" << fd_ << ": no fixed write buffer, using dynamic");
    }
}

CoroConnection::~CoroConnection() { Close(); }

void CoroConnection::Close() {
    if (closed_) return;
    closed_ = true;

    if (fixed_file_idx_ >= 0) {
        io_ctx_->UnregisterFile(fixed_file_idx_);
        fixed_file_idx_ = -1;
    }

    if (read_buf_idx_ >= 0) {
        io_ctx_->FreeBuffer(read_buf_idx_);
        read_buf_idx_ = -1;
        read_fixed_ptr_ = nullptr;
    }
    if (write_buf_idx_ >= 0) {
        io_ctx_->FreeBuffer(write_buf_idx_);
        write_buf_idx_ = -1;
        write_fixed_ptr_ = nullptr;
    }
    ::close(fd_);
    LOG_INFO("CoroConnection fd=" << fd_ << " closed.");
}

ReadAwaitable CoroConnection::Read() {
    io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
    int target = (fixed_file_idx_ >= 0) ? fixed_file_idx_ : fd_;

    if (read_buf_idx_ >= 0) {
        read_buffer_.EnsureWritable(1);
        io_uring_prep_read_fixed(sqe, target,
                                 read_buffer_.WriteBegin(),
                                 read_buffer_.WritableBytes(),
                                 0, read_buf_idx_);
    } else {
        read_buffer_.EnsureWritable(4096);
        io_uring_prep_recv(sqe, target,
                           read_buffer_.WriteBegin(),
                           read_buffer_.WritableBytes(), 0);
    }

    if (fixed_file_idx_ >= 0)
        sqe->flags |= IOSQE_FIXED_FILE;

    return ReadAwaitable{io_ctx_, sqe, &read_buffer_, fd_};
}

TaskT<int> CoroConnection::Write(const void* data, size_t len) {
    const char* ptr = static_cast<const char*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        int w = co_await WriteOnce(ptr, remaining);
        if (w <= 0) co_return w;
        ptr       += w;
        remaining -= static_cast<size_t>(w);
    }
    co_return static_cast<int>(len);
}

WriteAwaitable CoroConnection::WriteOnce(const void* data, size_t len) {
    io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
    int target = (fixed_file_idx_ >= 0) ? fixed_file_idx_ : fd_;

    if (write_buf_idx_ >= 0) {
        size_t to_write = std::min(len, io_ctx_->BufSize());
        std::memcpy(write_fixed_ptr_, data, to_write);
        write_buffer_ = Buffer(write_fixed_ptr_, io_ctx_->BufSize());
        write_buffer_.HasWritten(to_write);

        io_uring_prep_write_fixed(sqe, target,
                                  write_buffer_.ReadBegin(),
                                  write_buffer_.ReadableBytes(),
                                  0, write_buf_idx_);
    } else {
        write_buffer_ = Buffer(len);
        std::memcpy(write_buffer_.WriteBegin(), data, len);
        write_buffer_.HasWritten(len);

        io_uring_prep_send(sqe, target,
                           write_buffer_.ReadBegin(),
                           write_buffer_.ReadableBytes(), 0);
    }

    if (fixed_file_idx_ >= 0)
        sqe->flags |= IOSQE_FIXED_FILE;

    return WriteAwaitable{io_ctx_, sqe, fd_};
}
