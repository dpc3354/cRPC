#include "connection.h"
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

Connection::Connection(IoContext *io_ctx, int fd)
    : io_ctx_(io_ctx), fd_(fd), closed_(false), is_writing_(false),
      read_buf_idx_(-1), read_fixed_ptr_(nullptr), write_buf_idx_(-1),
      write_fixed_ptr_(nullptr), read_buffer_(), write_buffer_() {

  int flag = 1;
  if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) < 0) {
    LOG_WARN("Failed to set TCP_NODELAY on fd=" << fd);
  }

  read_buf_idx_ = io_ctx_->AllocateBuffer(&read_fixed_ptr_);
  if (read_buf_idx_ >= 0) {
    read_buffer_ = Buffer(read_fixed_ptr_, io_ctx_->BufSize());
    LOG_INFO("Connection fd=" << fd_ << " using fixed read buffer slot #"
                              << read_buf_idx_);
  } else {
    read_buffer_ = Buffer(65536);
    LOG_WARN("No fixed read buffer for fd="
             << fd_ << ", falling back to dynamic recv");
  }

  write_buf_idx_ = io_ctx_->AllocateBuffer(&write_fixed_ptr_);
  if (write_buf_idx_ >= 0) {
    write_buffer_ = Buffer(write_fixed_ptr_, io_ctx_->BufSize());
    LOG_INFO("Connection fd=" << fd_ << " using fixed write buffer slot #"
                              << write_buf_idx_);
  } else {
    write_buffer_ = Buffer(8192);
    LOG_WARN("No fixed write buffer for fd="
             << fd_ << ", falling back to dynamic send");
  }
}

Connection::~Connection() { Close(); }

void Connection::Start() { AsyncRead(); }

void Connection::Close() {
  if (closed_)
    return;
  closed_ = true;

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

  close(fd_);
  if (close_cb_) {
    close_cb_(shared_from_this());
  }
}

void Connection::Send(const void *data, size_t len) {
  if (closed_)
    return;
  write_buffer_.EnsureWritable(len);
  std::memcpy(write_buffer_.WriteBegin(), data, len);
  write_buffer_.HasWritten(len);

  if (!is_writing_) {
    AsyncWrite();
  }
}

void Connection::AsyncRead() {
  if (closed_)
    return;

  io_uring_sqe *sqe = io_uring_get_sqe(io_ctx_->GetRing());
  if (!sqe) {
    LOG_ERROR("Failed to get SQE for read on fd=" << fd_);
    return;
  }

  if (read_buf_idx_ >= 0) {
    // Compact first so WriteBegin() is as far back as possible
    read_buffer_.EnsureWritable(1);
    if (read_buffer_.WritableBytes() == 0) {
      LOG_ERROR("Read buffer full for fd=" << fd_);
      Close();
      return;
    }
    // Write directly into the registered memory at the current write position
    io_uring_prep_read_fixed(sqe, fd_, read_buffer_.WriteBegin(),
                             read_buffer_.WritableBytes(), 0, read_buf_idx_);
  } else {
    // Fallback: normal recv into dynamically allocated buffer
    read_buffer_.EnsureWritable(4096);
    io_uring_prep_recv(sqe, fd_, read_buffer_.WriteBegin(),
                       read_buffer_.WritableBytes(), 0);
  }

  io_ctx_->Submit(sqe, [this](int res) { OnReadEnd(res); });
}

void Connection::OnReadEnd(int res) {
  if (closed_)
    return;
  if (res <= 0) {
    if (res == 0) {
      LOG_INFO("Connection closed by peer, fd=" << fd_);
    } else {
      LOG_ERROR("Read error on fd=" << fd_ << ": " << strerror(-res));
    }
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
  if (closed_)
    return;
  if (write_buffer_.ReadableBytes() == 0) {
    is_writing_ = false;
    return;
  }

  is_writing_ = true;
  io_uring_sqe *sqe = io_uring_get_sqe(io_ctx_->GetRing());
  if (!sqe) {
    LOG_ERROR("Failed to get SQE for write on fd=" << fd_);
    return;
  }

  if (write_buf_idx_ >= 0) {
    io_uring_prep_write_fixed(sqe, fd_, write_buffer_.ReadBegin(),
                              write_buffer_.ReadableBytes(), 0, write_buf_idx_);
  } else {
    io_uring_prep_send(sqe, fd_, write_buffer_.ReadBegin(),
                       write_buffer_.ReadableBytes(), 0);
  }

  io_ctx_->Submit(sqe, [this](int res) { OnWriteEnd(res); });
}

void Connection::OnWriteEnd(int res) {
  if (closed_)
    return;
  if (res <= 0) {
    LOG_ERROR("Write error on fd=" << fd_);
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
