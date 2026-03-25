#pragma once
#include "coro.h"
#include "coro_connection.h"
#include <functional>
#include <netinet/in.h>

// ---------------------------------------------------------------------------
// CoroTcpServer — accept loop implemented as a coroutine.
//
// Each accepted connection is handed to a user-supplied ConnectionHandler,
// which runs as an independent fire-and-forget Task coroutine.  The handler
// owns the CoroConnection lifetime: when the handler returns, the connection
// is destroyed.
//
// Example:
//
//   Task handleConn(std::shared_ptr<CoroConnection> conn) {
//       while (true) {
//           int n = co_await conn->Read();
//           if (n <= 0) break;
//           // ... process conn->GetReadBuffer() ...
//           co_await conn->Write(data, len);
//       }
//       conn->Close();
//   }
//
//   CoroTcpServer server(&io_ctx, 8080);
//   server.SetConnectionHandler(handleConn);
//   server.Start();
//   io_ctx.Run();
// ---------------------------------------------------------------------------
class CoroTcpServer {
public:
    using ConnectionHandler = std::function<Task(std::shared_ptr<CoroConnection>)>;

    CoroTcpServer(IoContext* io_ctx, uint16_t port);
    ~CoroTcpServer();

    void SetConnectionHandler(ConnectionHandler handler) {
        handler_ = std::move(handler);
    }

    void Start();
    void Stop();

private:
    Task AcceptLoop();

    IoContext* io_ctx_;
    int listen_fd_;
    uint16_t port_;
    bool running_;
    ConnectionHandler handler_;

    struct sockaddr_in client_addr_;
    socklen_t client_addr_len_;
};
