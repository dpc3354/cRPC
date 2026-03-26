#pragma once
#include <coroutine>
#include <deque>
#include <optional>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// ResponseQueue — single-producer / single-consumer queue for passing
// encoded response buffers from handler coroutines to the write-loop coroutine.
//
// All access must be on the same io_uring event-loop thread; no locking needed.
//
// Usage:
//   // producer (handler coroutine):
//   queue->Push(std::move(data));
//
//   // consumer (write-loop coroutine):
//   while (auto item = co_await queue->Pop()) {
//       co_await conn->Write(item->data(), item->size());
//   }
//   // Pop() returns nullopt after Close() is called and queue is drained.
// ---------------------------------------------------------------------------
class ResponseQueue {
public:
    struct PopAwaitable {
        ResponseQueue* q;

        bool await_ready() const noexcept {
            return !q->items_.empty() || q->closed_;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            q->waiter_ = h;
        }

        std::optional<std::vector<char>> await_resume() noexcept {
            if (q->items_.empty()) return std::nullopt;  // closed, no more data
            auto item = std::move(q->items_.front());
            q->items_.pop_front();
            return item;
        }
    };

    // Called by handler coroutines. If the write loop is suspended on Pop(),
    // resumes it immediately (within the same thread's call stack).
    void Push(std::vector<char> data) {
        items_.push_back(std::move(data));
        if (waiter_) {
            auto h = std::exchange(waiter_, {});
            h.resume();
        }
    }

    // Called when the read loop exits. Wakes up the write loop so it can drain
    // remaining items and then stop.
    void Close() {
        closed_ = true;
        if (waiter_) {
            auto h = std::exchange(waiter_, {});
            h.resume();
        }
    }

    PopAwaitable Pop() { return {this}; }

private:
    std::deque<std::vector<char>> items_;
    std::coroutine_handle<>       waiter_{};
    bool                          closed_ = false;
};
