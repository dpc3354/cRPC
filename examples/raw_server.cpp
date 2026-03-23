// Raw echo server — no protobuf, no RPC framing.
// Reflects every received byte back to the sender.
// Used with raw_bench to measure pure framework I/O latency.
//
// Usage: ./raw_server [port]   (default port: 8081)

#include "io_context.h"
#include "tcp_server.h"
#include <signal.h>
#include <cstdlib>

IoContext* g_io_ctx = nullptr;

void HandleSigInt(int) {
    if (g_io_ctx) g_io_ctx->Stop();
}

int main(int argc, char* argv[]) {
    signal(SIGINT, HandleSigInt);

    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 8081;

    try {
        IoContext io_ctx;
        g_io_ctx = &io_ctx;

        TcpServer server(&io_ctx, port);

        server.SetMessageCallback([](std::shared_ptr<Connection> conn, Buffer& buffer) {
            conn->Send(buffer.ReadBegin(), buffer.ReadableBytes());
            buffer.HasRead(buffer.ReadableBytes());
        });

        server.Start();
        LOG_INFO("Raw echo server listening on port " << port);
        io_ctx.Run();

    } catch (const std::exception& e) {
        LOG_ERROR("Exception: " << e.what());
        return 1;
    }

    return 0;
}
