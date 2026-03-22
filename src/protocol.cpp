#include "protocol.h"
#include "logger.h"
#include <cstring>
#include <arpa/inet.h>

std::optional<std::vector<char>> Protocol::ParseMessage(Buffer& buffer, RpcHeader& out_header) {
    if (buffer.ReadableBytes() < HEADER_SIZE) {
        return std::nullopt; // Not enough data for header
    }

    const char* ptr = buffer.ReadBegin();
    std::memcpy(&out_header, ptr, HEADER_SIZE);

    out_header.magic = ntohs(out_header.magic);
    out_header.seq_id = ntohl(out_header.seq_id);
    out_header.payload_len = ntohl(out_header.payload_len);

    if (out_header.magic != MAGIC_NUMBER) {
        LOG_ERROR("Invalid magic number: " << out_header.magic << " Expected: " << MAGIC_NUMBER);
        // For resilience, if the magic number is wrong, advance by one byte to find the next valid header
        buffer.HasRead(1);
        return std::nullopt;
    }

    if (buffer.ReadableBytes() < HEADER_SIZE + out_header.payload_len) {
        return std::nullopt; // Sticky packet / packet fragmentation: wait for full payload
    }

    // We have a full message
    buffer.HasRead(HEADER_SIZE);
    
    std::vector<char> payload(out_header.payload_len);
    if (out_header.payload_len > 0) {
        std::memcpy(payload.data(), buffer.ReadBegin(), out_header.payload_len);
        buffer.HasRead(out_header.payload_len);
    }

    return payload;
}

void Protocol::EncodeMessage(Buffer& buffer, const RpcHeader& header, const std::string& payload) {
    EncodeMessage(buffer, header, payload.data(), payload.size());
}

void Protocol::EncodeMessage(Buffer& buffer, const RpcHeader& header, const void* payload_data, size_t payload_len) {
    RpcHeader net_header = header;
    net_header.magic = htons(MAGIC_NUMBER);
    net_header.seq_id = htonl(header.seq_id);
    net_header.payload_len = htonl(payload_len);

    buffer.EnsureWritable(HEADER_SIZE + payload_len);
    std::memcpy(buffer.WriteBegin(), &net_header, HEADER_SIZE);
    buffer.HasWritten(HEADER_SIZE);

    if (payload_len > 0) {
        std::memcpy(buffer.WriteBegin(), payload_data, payload_len);
        buffer.HasWritten(payload_len);
    }
}
