#include "coro_tcp_server.h"
#include "logger.h"
#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

CoroTcpServer::CoroTcpServer(IoContext* io_ctx, uint16_t port)
    : io_ctx_(io_ctx), port_(port), running_(false), listen_fd_(-1) {

    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) throw std::runtime_error("socket error");

    int opt = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(port_);

    if (::bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind error");

    if (::listen(listen_fd_, SOMAXCONN) < 0)
        throw std::runtime_error("listen error");

    LOG_INFO("CoroTcpServer listening on port " << port_);
}

CoroTcpServer::~CoroTcpServer() { Stop(); }

void CoroTcpServer::Start() {
    running_ = true;
    AcceptLoop();  // starts immediately (suspend_never), returns Task (discarded)
}

void CoroTcpServer::Stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

// AcceptLoop is the coroutine heart of the server.
// It runs inside the IoContext event loop: each co_await suspends here and
// resumes when io_uring delivers an accept completion.
Task CoroTcpServer::AcceptLoop() {
    while (running_) {
        client_addr_len_ = sizeof(client_addr_);

        io_uring_sqe* sqe = io_uring_get_sqe(io_ctx_->GetRing());
        if (!sqe) {
            LOG_ERROR("CoroTcpServer: ring full, cannot post accept");
            break;
        }
        io_uring_prep_accept(sqe, listen_fd_,
                             (struct sockaddr*)&client_addr_,
                             &client_addr_len_, 0);

        // Suspend here; resume when a new client connects (or listen_fd_ is closed)
        int client_fd = co_await IoAwaitable{io_ctx_, sqe};
        if (client_fd < 0) {
            if (running_ && -client_fd != ECANCELED)
                LOG_ERROR("CoroTcpServer: accept failed: " << strerror(-client_fd));
            break;
        }

        LOG_INFO("CoroTcpServer: new connection fd=" << client_fd);
        auto conn = std::make_shared<CoroConnection>(io_ctx_, client_fd);

        if (handler_)
            handler_(conn);  // spawns an independent Task coroutine
    }
}
