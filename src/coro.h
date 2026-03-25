#pragma once
#include <coroutine>
#include <cstdio>
#include <cstring>
#include <exception>
#include <utility>
#include "io_context.h"
#include "buffer.h"
#include "logger.h"

// ---------------------------------------------------------------------------
// IoAwaitable — wraps a single prepared io_uring SQE into a co_await point.
//
// Usage:
//   io_uring_sqe* sqe = io_uring_get_sqe(ring);
//   io_uring_prep_*(sqe, ...);
//   int result = co_await IoAwaitable{ctx, sqe};
// ---------------------------------------------------------------------------
struct IoAwaitable {
    IoContext* ctx;
    io_uring_sqe* sqe;
    int result = 0;
    std::coroutine_handle<> handle;

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle = h;
        ctx->Submit(sqe, [this](int res) {
            result = res;
            handle.resume();
        });
    }

    int await_resume() noexcept { return result; }
};

// ---------------------------------------------------------------------------
// ReadAwaitable — like IoAwaitable but also advances buf->HasWritten(n) on
// success and logs EOF / errors so CoroConnection doesn't need to repeat it.
// ---------------------------------------------------------------------------
struct ReadAwaitable {
    IoContext* ctx;
    io_uring_sqe* sqe;
    Buffer* buffer;
    int fd = -1;
    int result = 0;
    std::coroutine_handle<> handle;

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle = h;
        ctx->Submit(sqe, [this](int res) {
            result = res;
            handle.resume();
        });
    }

    int await_resume() noexcept {
        if (result > 0) {
            buffer->HasWritten(result);
        } else if (result == 0) {
            LOG_INFO("Connection closed by peer, fd=" << fd);
        } else {
            LOG_ERROR("Read error on fd=" << fd << ": " << strerror(-result));
        }
        return result;
    }
};

// ---------------------------------------------------------------------------
// WriteAwaitable — like IoAwaitable but logs write errors with the fd.
// ---------------------------------------------------------------------------
struct WriteAwaitable {
    IoContext* ctx;
    io_uring_sqe* sqe;
    int fd = -1;
    int result = 0;
    std::coroutine_handle<> handle;

    bool await_ready() noexcept { return false; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle = h;
        ctx->Submit(sqe, [this](int res) {
            result = res;
            handle.resume();
        });
    }

    int await_resume() noexcept {
        if (result <= 0)
            LOG_ERROR("Write error on fd=" << fd << ": " << strerror(-result));
        return result;
    }
};

// ---------------------------------------------------------------------------
// Task — fire-and-forget coroutine type for connection handlers.
//
// - initial_suspend = never  → coroutine starts immediately on call
// - final_suspend   = never  → frame is freed automatically on completion
// - Exceptions are caught and logged; they do NOT propagate to the caller.
// ---------------------------------------------------------------------------
struct Task {
    struct promise_type {
        Task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        void unhandled_exception() noexcept {
            try { std::rethrow_exception(std::current_exception()); }
            catch (const std::exception& e) {
                std::fprintf(stderr, "[coro] unhandled exception: %s\n", e.what());
            }
            catch (...) {
                std::fprintf(stderr, "[coro] unknown exception in coroutine\n");
            }
        }
    };
};

// ---------------------------------------------------------------------------
// TaskT<T> — awaitable coroutine that returns a value to its caller.
//
// Used for internal helpers that need to loop over multiple co_await points
// and hand a result back to the outer coroutine.  Uses C++20 symmetric
// transfer so resuming the continuation costs one tail-call, not a stack frame.
//
// Usage:
//   TaskT<bool> someHelper(...) { co_return true; }
//   bool ok = co_await someHelper(...);
// ---------------------------------------------------------------------------
template<typename T>
struct TaskT {
    struct promise_type;

    // Resumed at final_suspend; hands control back to the awaiting coroutine
    // via symmetric transfer (returns a handle instead of calling .resume()).
    struct FinalAwaiter {
        bool await_ready() noexcept { return false; }
        std::coroutine_handle<> await_suspend(
                std::coroutine_handle<promise_type> h) noexcept {
            auto cont = h.promise().continuation;
            return cont ? cont : std::noop_coroutine();
        }
        void await_resume() noexcept {}
    };

    struct promise_type {
        T value{};
        std::coroutine_handle<> continuation;

        TaskT get_return_object() noexcept {
            return TaskT{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_never initial_suspend() noexcept { return {}; }
        FinalAwaiter       final_suspend()   noexcept { return {}; }
        void return_value(T v) noexcept { value = std::move(v); }
        void unhandled_exception() noexcept { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle;

    // Awaiter interface — lets outer coroutine do: T v = co_await taskT;
    bool await_ready() noexcept { return handle.done(); }
    void await_suspend(std::coroutine_handle<> h) noexcept {
        handle.promise().continuation = h;
        // TaskT started immediately (suspend_never); it is now suspended at
        // some inner co_await.  When it completes, FinalAwaiter resumes h.
    }
    T await_resume() noexcept { return handle.promise().value; }

    ~TaskT() { if (handle) handle.destroy(); }
    TaskT(TaskT&& o) noexcept : handle(std::exchange(o.handle, {})) {}
    TaskT(const TaskT&) = delete;

private:
    explicit TaskT(std::coroutine_handle<promise_type> h) : handle(h) {}
};
