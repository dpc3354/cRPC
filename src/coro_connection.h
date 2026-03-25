#pragma once
#include "coro.h"
#include "buffer.h"
#include <memory>

// ---------------------------------------------------------------------------
// CoroConnection — coroutine-friendly TCP connection.
//
// Read() and Write() are co_await points:
//
//   int n = co_await conn->Read();      // suspends until data arrives
//   int w = co_await conn->Write(p, n); // suspends until kernel accepts data
//
// Read() automatically advances the read buffer's write pointer on success.
// Write() resets its internal write staging buffer on every call, so it is
// safe to call repeatedly without needing to manage buffer state.
//
// Partial writes: Write() returns the number of bytes the kernel accepted,
// which may be less than `len`. Use a loop (or the WriteAll helper in
// coro_utils.h) if you need all bytes to be sent.
// ---------------------------------------------------------------------------
class CoroConnection : public std::enable_shared_from_this<CoroConnection> {
public:
    CoroConnection(IoContext* io_ctx, int fd);
    ~CoroConnection();

    // Suspend until data arrives.
    // On success (n > 0): n bytes are appended to GetReadBuffer().
    // On EOF / error (n <= 0): connection should be closed.
    ReadAwaitable Read();

    // Sends all len bytes, looping internally on partial writes.
    // Returns total bytes sent, or <=0 if the connection was lost.
    TaskT<int> Write(const void* data, size_t len);

    Buffer& GetReadBuffer() { return read_buffer_; }
    int Fd() const { return fd_; }
    bool IsClosed() const { return closed_; }
    void Close();

private:
    WriteAwaitable WriteOnce(const void* data, size_t len);  // single io_uring op

    IoContext* io_ctx_;
    int fd_;
    bool closed_;

    int  read_buf_idx_;
    char* read_fixed_ptr_;
    int  write_buf_idx_;
    char* write_fixed_ptr_;

    Buffer read_buffer_;
    Buffer write_buffer_;  // staging; reset on each Write() call
};
