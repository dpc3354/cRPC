#include "io_context.h"
#include "coro_tcp_server.h"
#include "coro_connection.h"
#include "protocol.h"
#include "logger.h"
#include "rpc_message.pb.h"
#include <memory>
#include <sched.h>
#include <signal.h>
#include <vector>

static std::vector<IoContext*> g_all_ctxs;

void HandleSigInt(int) {
    for (auto* ctx : g_all_ctxs) ctx->Stop();
}

// ---------------------------------------------------------------------------
// 连接处理协程 — 每条连接独立运行，顺序代码，无嵌套回调。
//
// 流程：
//   co_await Read()   挂起等数据，不阻塞事件循环
//   解析协议 + 业务处理（此处为 protobuf EchoRequest → EchoResponse）
//   co_await Write()  挂起等发送完成
// ---------------------------------------------------------------------------
Task handleConnection(std::shared_ptr<CoroConnection> conn) {
    LOG_INFO("[coro] connection fd=" << conn->Fd());
    std::string resp_bytes;

    while (true) {
        // 等待数据到达
        int n = co_await conn->Read();
        if (n <= 0) break;

        // 循环解析 buffer 里可能积累的多条完整消息（处理 TCP 粘包）
        auto& buf = conn->GetReadBuffer();
        while (buf.ReadableBytes() >= Protocol::HEADER_SIZE) {
            RpcHeader header;
            auto payload_opt = Protocol::ParseMessage(buf, header);
            if (!payload_opt) break;  // 不足一条完整消息，等下次 Read()

            if (header.msg_type == 0) {  // Request
                crpc::EchoRequest req;
                if (!req.ParseFromArray(payload_opt->data(), payload_opt->size())) {
                    LOG_ERROR("[coro] failed to parse EchoRequest");
                    co_return;
                }

                crpc::EchoResponse resp;
                resp.set_message("Echo: " + req.message());
                resp.SerializeToString(&resp_bytes);

                RpcHeader resp_header = header;
                resp_header.msg_type  = 1;  // Response

                Buffer write_buf;
                Protocol::EncodeMessage(write_buf, resp_header, resp_bytes);

                int w = co_await conn->Write(
                    write_buf.ReadBegin(), write_buf.ReadableBytes());
                if (w <= 0) co_return;
            }
        }
    }
    // conn->Close() 不需要显式调用 — 协程结束时 shared_ptr 析构自动触发
}

int main() {
    signal(SIGINT, HandleSigInt);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {
        int nThreads = std::thread::hardware_concurrency();
        LOG_INFO("Starting coro_echo server with " << nThreads << " threads.");

        std::vector<std::thread> threads;
        std::vector<std::unique_ptr<IoContext>> ctxs(nThreads);

        for (int i = 0; i < nThreads; ++i) {
            ctxs[i] = std::make_unique<IoContext>();
            g_all_ctxs.push_back(ctxs[i].get());

            threads.emplace_back([ctx = ctxs[i].get(), i] {
                cpu_set_t cpus;
                CPU_ZERO(&cpus);
                CPU_SET(i, &cpus);
                pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);

                CoroTcpServer server(ctx, 8080);
                server.SetConnectionHandler(handleConnection);
                server.Start();
                ctx->Run(); });
        }
        for (auto& t : threads) t.join();
        
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: " << e.what());
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
