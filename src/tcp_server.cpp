#include "tcp_server.h"
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>

TcpServer::TcpServer(IoContext* io_ctx, uint16_t port)
    : io_ctx_(io_ctx), port_(port), running_(false), listen_fd_(-1) {
    
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        LOG_ERROR("Failed to create listen socket");
        throw std::runtime_error("socket error");
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    std::memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port_);

    if (bind(listen_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        LOG_ERROR("Failed to bind to port " << port_);
        throw std::runtime_error("bind error");
    }

    if (listen(listen_fd_, SOMAXCONN) < 0) {
        LOG_ERROR("Failed to listen on socket");
        throw std::runtime_error("listen error");
    }
    
    LOG_INFO("TcpServer listening on port " << port_);
}

TcpServer::~TcpServer() {
    Stop();
}

void TcpServer::Start() {
    running_ = true;
    AsyncAccept();
}

void TcpServer::Stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        close(listen_fd_);
        listen_fd_ = -1;
    }
}

void TcpServer::AsyncAccept() {
    if (!running_) return;

    client_addr_len_ = sizeof(client_addr_);
    io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
    if (!sqe) {
        LOG_ERROR("Failed to get SQE from io_uring for accept.");
        return;
    }

    io_uring_prep_accept(sqe, listen_fd_, (struct sockaddr*)&client_addr_, &client_addr_len_, 0);

    io_ctx_->Submit(sqe, [this](int res) {
        OnAcceptEnd(res);
    });
}

void TcpServer::OnAcceptEnd(int res) {
    if (!running_) return;

    if (res < 0) {
        LOG_ERROR("Accept failed: " << strerror(-res));
    } else {
        int client_fd = res;
        LOG_INFO("New connection accepted: fd " << client_fd);
        auto conn = std::make_shared<Connection>(io_ctx_, client_fd);
        connections_[client_fd] = conn;

        conn->SetMessageCallback(message_cb_);
        conn->SetCloseCallback([this](std::shared_ptr<Connection> c) {
            connections_.erase(c->Fd());
        });

        conn->Start();
    }

    AsyncAccept();
}
