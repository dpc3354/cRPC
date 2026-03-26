#include "io_context.h"
#include "coro.h"
#include "coro_tcp_server.h"
#include "coro_connection.h"
#include "protocol.h"
#include "response_queue.h"
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
// Demonstrates true concurrent in-flight requests on a single connection.
//
// co_await AsyncSleep() is a real io_uring suspend point.  While this
// coroutine waits, the read loop keeps parsing and dispatching new requests,
// so N requests genuinely overlap in time instead of being processed serially.
//
// In a real service, replace AsyncSleep with:
//   co_await db.query(...)         — async database call
//   co_await rpc_client.call(...)  — downstream RPC
//   co_await file.read(...)        — async file IO
// ---------------------------------------------------------------------------
Task handleRequest(IoContext* ctx, RpcHeader header, std::vector<char> payload,
                   std::shared_ptr<ResponseQueue> queue) {
    crpc::EchoRequest req;
    if (!req.ParseFromArray(payload.data(), payload.size())) {
        LOG_ERROR("[async] failed to parse EchoRequest seq=" << header.seq_id);
        co_return;
    }

    LOG_INFO("[async] seq=" << header.seq_id << " start async work");

    // Suspend here — read loop continues dispatching other requests.
    co_await AsyncSleep(ctx, 10);  // simulate 10 ms async work

    LOG_INFO("[async] seq=" << header.seq_id << " async work done");

    crpc::EchoResponse resp;
    resp.set_message("Echo: " + req.message());
    std::string resp_bytes;
    resp.SerializeToString(&resp_bytes);

    RpcHeader resp_header  = header;
    resp_header.msg_type   = 1;

    Buffer write_buf;
    Protocol::EncodeMessage(write_buf, resp_header, resp_bytes);

    std::vector<char> data(write_buf.ReadBegin(),
                           write_buf.ReadBegin() + write_buf.ReadableBytes());
    queue->Push(std::move(data));
}

Task writeLoop(std::shared_ptr<CoroConnection> conn,
               std::shared_ptr<ResponseQueue> queue) {
    while (true) {
        auto item = co_await queue->Pop();
        if (!item) break;
        int w = co_await conn->Write(item->data(), item->size());
        if (w <= 0) break;
    }
}

Task handleConnection(std::shared_ptr<CoroConnection> conn) {
    LOG_INFO("[async] connection fd=" << conn->Fd());
    auto queue = std::make_shared<ResponseQueue>();
    writeLoop(conn, queue);

    while (true) {
        int n = co_await conn->Read();
        if (n <= 0) break;

        auto& buf = conn->GetReadBuffer();
        while (buf.ReadableBytes() >= Protocol::HEADER_SIZE) {
            RpcHeader header;
            auto payload_opt = Protocol::ParseMessage(buf, header);
            if (!payload_opt) break;

            if (header.msg_type == 0)
                handleRequest(conn->GetIoContext(), header, std::move(*payload_opt), queue);
        }
    }
    queue->Close();
}

int main() {
    signal(SIGINT, HandleSigInt);
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {
        int nThreads = std::thread::hardware_concurrency();
        LOG_INFO("Starting coro_echo_async server with " << nThreads << " threads.");

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
                ctx->Run();
            });
        }
        for (auto& t : threads) t.join();

    } catch (const std::exception& e) {
        LOG_ERROR("Exception: " << e.what());
    }

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
