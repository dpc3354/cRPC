#pragma once
#include "io_context.h"
#include "buffer.h"
#include <memory>
#include <functional>

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(IoContext* io_ctx, int fd);
    ~Connection();

    void Start();
    void Send(const void* data, size_t len);
    void Close();

    using MessageCallback = std::function<void(std::shared_ptr<Connection>, Buffer&)>;
    using CloseCallback = std::function<void(std::shared_ptr<Connection>)>;

    void SetMessageCallback(MessageCallback cb) { message_cb_ = std::move(cb); }
    void SetCloseCallback(CloseCallback cb) { close_cb_ = std::move(cb); }

    int Fd() const { return fd_; }

private:
    void AsyncRead();
    void OnReadEnd(int res);

    void AsyncWrite();
    void OnWriteEnd(int res);

    IoContext* io_ctx_;
    int fd_;
    bool closed_;

    // Each connection borrows fixed registered buffer slots for reads and writes
    int read_buf_idx_;    // -1 if unavailable
    char* read_fixed_ptr_;
    int write_buf_idx_;   // -1 if unavailable
    char* write_fixed_ptr_;

    Buffer read_buffer_;     // staging for protocol parsing
    Buffer write_buffer_;    // dynamic write staging
    bool is_writing_;

    MessageCallback message_cb_;
    CloseCallback close_cb_;
};
