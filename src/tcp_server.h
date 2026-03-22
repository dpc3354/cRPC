#pragma once
#include "io_context.h"
#include "connection.h"
#include <string>
#include <netinet/in.h>
#include <unordered_map>

class TcpServer {
public:
    TcpServer(IoContext* io_ctx, uint16_t port);
    ~TcpServer();

    void Start();
    void Stop();

    void SetMessageCallback(Connection::MessageCallback cb) { message_cb_ = std::move(cb); }

private:
    void AsyncAccept();
    void OnAcceptEnd(int res);

    IoContext* io_ctx_;
    int listen_fd_;
    uint16_t port_;
    bool running_;

    struct sockaddr_in client_addr_;
    socklen_t client_addr_len_;

    std::unordered_map<int, std::shared_ptr<Connection>> connections_;

    Connection::MessageCallback message_cb_;
};
