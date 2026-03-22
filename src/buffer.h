#pragma once

#include <vector>
#include <cstddef>
#include <span>
#include <algorithm>

class Buffer {
public:
    Buffer(size_t initial_size = 4096) : data_(initial_size), write_idx_(0), read_idx_(0) {}

    char* WriteBegin() { return data_.data() + write_idx_; }
    size_t WritableBytes() const { return data_.size() - write_idx_; }
    void HasWritten(size_t len) { write_idx_ += len; }

    const char* ReadBegin() const { return data_.data() + read_idx_; }
    size_t ReadableBytes() const { return write_idx_ - read_idx_; }
    void HasRead(size_t len) { read_idx_ += len; }

    void EnsureWritable(size_t len) {
        if (WritableBytes() < len) {
            MakeSpace(len);
        }
    }

    std::span<std::byte> GetSpan() {
        return std::span<std::byte>(reinterpret_cast<std::byte*>(data_.data() + read_idx_), ReadableBytes());
    }

private:
    void MakeSpace(size_t len) {
        if (WritableBytes() + read_idx_ < len) {
            data_.resize(write_idx_ + len);
        } else {
            size_t readable = ReadableBytes();
            std::copy(data_.begin() + read_idx_,
                      data_.begin() + write_idx_,
                      data_.begin());
            read_idx_ = 0;
            write_idx_ = readable;
        }
    }

    std::vector<char> data_;
    size_t write_idx_;
    size_t read_idx_;
};
