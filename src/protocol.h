#pragma once
#include "buffer.h"
#include <cstdint>
#include <vector>
#include <optional>
#include <string>

struct RpcHeader {
    uint16_t magic;    // 'T' 'R' (0x5452)
    uint8_t version;
    uint8_t msg_type;  // 0: Request, 1: Response
    uint32_t seq_id;
    uint32_t payload_len;
} __attribute__((packed));

class Protocol {
public:
    static constexpr uint16_t MAGIC_NUMBER = 0x5452;
    static constexpr size_t HEADER_SIZE = sizeof(RpcHeader);

    // Parses the buffer. Returns true and extracts the payload if a full message is available.
    // Removes the header and payload from the buffer if successful.
    static std::optional<std::vector<char>> ParseMessage(Buffer& buffer, RpcHeader& out_header);

    // Encodes a message into the given buffer.
    static void EncodeMessage(Buffer& buffer, const RpcHeader& header, const std::string& payload);
    static void EncodeMessage(Buffer& buffer, const RpcHeader& header, const void* payload_data, size_t payload_len);
};
