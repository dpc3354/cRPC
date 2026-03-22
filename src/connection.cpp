#include "connection.h"
#include <unistd.h>
#include <sys/socket.h>
#include <cstring>

Connection::Connection(IoContext* io_ctx, int fd)
    : io_ctx_(io_ctx), fd_(fd), closed_(false), is_writing_(false) {
}

Connection::~Connection() {
    Close();
}

void Connection::Start() {
    AsyncRead();
}

void Connection::Close() {
    if (closed_) return;
    closed_ = true;
    close(fd_);
    if (close_cb_) {
        close_cb_(shared_from_this());
    }
}

void Connection::Send(const void* data, size_t len) {
    if (closed_) return;
    write_buffer_.EnsureWritable(len);
    std::memcpy(write_buffer_.WriteBegin(), data, len);
    write_buffer_.HasWritten(len);

    if (!is_writing_) {
        AsyncWrite();
    }
}

void Connection::AsyncRead() {
    if (closed_) return;
    read_buffer_.EnsureWritable(4096);
    
    io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
    if (!sqe) {
        LOG_ERROR("Failed to get SQE from io_uring for read.");
        return;
    }

    io_uring_prep_recv(sqe, fd_, read_buffer_.WriteBegin(), read_buffer_.WritableBytes(), 0);

    auto self = shared_from_this();
    io_ctx_->Submit(sqe, [self, this](int res) {
        OnReadEnd(res);
    });
}

void Connection::OnReadEnd(int res) {
    if (closed_) return;
    if (res <= 0) {
        LOG_INFO("Connection closed or read error, fd: " << fd_);
        Close();
        return;
    }

    read_buffer_.HasWritten(res);
    if (message_cb_) {
        message_cb_(shared_from_this(), read_buffer_);
    }

    AsyncRead();
}

void Connection::AsyncWrite() {
    if (closed_) return;
    if (write_buffer_.ReadableBytes() == 0) {
        is_writing_ = false;
        return;
    }

    is_writing_ = true;
    io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
    if (!sqe) {
        LOG_ERROR("Failed to get SQE from io_uring for write.");
        return;
    }

    io_uring_prep_send(sqe, fd_, write_buffer_.ReadBegin(), write_buffer_.ReadableBytes(), 0);

    auto self = shared_from_this();
    io_ctx_->Submit(sqe, [self, this](int res) {
        OnWriteEnd(res);
    });
}

void Connection::OnWriteEnd(int res) {
    if (closed_) return;
    if (res <= 0) {
        LOG_ERROR("Write error, fd: " << fd_);
        Close();
        return;
    }

    write_buffer_.HasRead(res);
    if (write_buffer_.ReadableBytes() > 0) {
        AsyncWrite();
    } else {
        is_writing_ = false;
    }
}
