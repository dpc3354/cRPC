#pragma once

#include <vector>
#include <cstddef>
#include <span>
#include <algorithm>
#include <cstring>

class Buffer {
public:
    Buffer() : external_data_(nullptr), capacity_(0), is_fixed_(false), write_idx_(0), read_idx_(0) {}
    
    // Fixed buffer mode
    Buffer(char* external_mem, size_t capacity) 
        : external_data_(external_mem), capacity_(capacity), is_fixed_(true), write_idx_(0), read_idx_(0) {}

    // Dynamic buffer mode
    Buffer(size_t initial_size) 
        : data_(initial_size), capacity_(initial_size), is_fixed_(false), write_idx_(0), read_idx_(0) {}

    char* WriteBegin() { return is_fixed_ ? external_data_ + write_idx_ : data_.data() + write_idx_; }
    size_t WritableBytes() const { return capacity_ - write_idx_; }
    void HasWritten(size_t len) { write_idx_ += len; }

    const char* ReadBegin() const { return is_fixed_ ? external_data_ + read_idx_ : data_.data() + read_idx_; }
    size_t ReadableBytes() const { return write_idx_ - read_idx_; }
    void HasRead(size_t len) { read_idx_ += len; }

    bool EnsureWritable(size_t len) {
        if (WritableBytes() >= len) return true;
        
        Compact();
        if (WritableBytes() >= len) return true;

        if (is_fixed_) {
            return false; // Can't dynamically resize registered memory!
        } else {
            capacity_ = write_idx_ + len;
            data_.resize(capacity_);
            return true;
        }
    }

    void Compact() {
        if (read_idx_ == 0) return;
        size_t readable = ReadableBytes();
        char* base = is_fixed_ ? external_data_ : data_.data();
        if (readable > 0) {
            std::memmove(base, base + read_idx_, readable);
        }
        read_idx_ = 0;
        write_idx_ = readable;
    }

    std::span<std::byte> GetSpan() {
        return std::span<std::byte>(reinterpret_cast<std::byte*>(is_fixed_ ? external_data_ + read_idx_ : data_.data() + read_idx_), ReadableBytes());
    }

private:
    std::vector<char> data_;
    char* external_data_;
    size_t capacity_;
    bool is_fixed_;
    size_t write_idx_;
    size_t read_idx_;
};
