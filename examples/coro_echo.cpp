#include "io_context.h"
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
// handleRequest — processes one RPC request and pushes the encoded response
// into the shared queue.
//
// Currently synchronous (no co_await), so it runs inline in the read loop.
// Adding an async step (e.g. co_await db_query()) here is the only change
// needed to get true concurrent in-flight requests: the read loop will keep
// parsing new requests while this handler is suspended.
// ---------------------------------------------------------------------------
Task handleRequest(RpcHeader header, std::vector<char> payload,
                   std::shared_ptr<ResponseQueue> queue) {
    crpc::EchoRequest req;
    if (!req.ParseFromArray(payload.data(), payload.size())) {
        LOG_ERROR("[coro] failed to parse EchoRequest seq=" << header.seq_id);
        co_return;
    }

    crpc::EchoResponse resp;
    resp.set_message("Echo: " + req.message());
    std::string resp_bytes;
    resp.SerializeToString(&resp_bytes);

    RpcHeader resp_header  = header;
    resp_header.msg_type   = 1;  // Response

    Buffer write_buf;
    Protocol::EncodeMessage(write_buf, resp_header, resp_bytes);

    std::vector<char> data(write_buf.ReadBegin(),
                           write_buf.ReadBegin() + write_buf.ReadableBytes());
    queue->Push(std::move(data));
}

// ---------------------------------------------------------------------------
// writeLoop — sole writer on this connection.
// Drains ResponseQueue one item at a time; exits when the queue is closed
// and empty (read loop ended) or when the connection drops.
// ---------------------------------------------------------------------------
Task writeLoop(std::shared_ptr<CoroConnection> conn,
               std::shared_ptr<ResponseQueue> queue) {
    while (true) {
        auto item = co_await queue->Pop();
        if (!item) break;  // queue closed and drained
        int w = co_await conn->Write(item->data(), item->size());
        if (w <= 0) break;
    }
}

// ---------------------------------------------------------------------------
// handleConnection — read loop.
//
// Spawns writeLoop as a sibling coroutine sharing the ResponseQueue, then
// dispatches one handleRequest Task per incoming RPC frame.
//
// Because handleRequest and writeLoop are separate coroutines:
// - Multiple requests can be in-flight simultaneously (seq_id demuxes them).
// - Writes are never interleaved — writeLoop is the only caller of conn->Write().
// ---------------------------------------------------------------------------
Task handleConnection(std::shared_ptr<CoroConnection> conn) {
    LOG_INFO("[coro] connection fd=" << conn->Fd());
    auto queue = std::make_shared<ResponseQueue>();
    writeLoop(conn, queue);  // fire-and-forget; suspends at first Pop()

    while (true) {
        int n = co_await conn->Read();
        if (n <= 0) break;

        auto& buf = conn->GetReadBuffer();
        while (buf.ReadableBytes() >= Protocol::HEADER_SIZE) {
            RpcHeader header;
            auto payload_opt = Protocol::ParseMessage(buf, header);
            if (!payload_opt) break;  // incomplete message, wait for more data

            if (header.msg_type == 0) {  // Request
                handleRequest(header, std::move(*payload_opt), queue);
            }
        }
    }
    queue->Close();  // drain remaining responses, then writeLoop exits
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
