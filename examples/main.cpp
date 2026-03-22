#include "io_context.h"
#include "tcp_server.h"
#include "protocol.h"
#include "rpc_message.pb.h"
#include <signal.h>

IoContext* g_io_ctx = nullptr;

void HandleSigInt(int) {
    if (g_io_ctx) {
        LOG_INFO("Stopping server...");
        g_io_ctx->Stop();
    }
}

int main() {
    signal(SIGINT, HandleSigInt);
    
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    try {
        IoContext io_ctx;
        g_io_ctx = &io_ctx;

        TcpServer server(&io_ctx, 8080);
        
        server.SetMessageCallback([](std::shared_ptr<Connection> conn, Buffer& buffer) {
            while (true) {
                RpcHeader header;
                auto payload_opt = Protocol::ParseMessage(buffer, header);
                if (!payload_opt) {
                    break;
                }

                auto payload = payload_opt.value();
                
                if (header.msg_type == 0) { // Request
                    crpc::EchoRequest req;
                    if (req.ParseFromArray(payload.data(), payload.size())) {
                        LOG_INFO("Received EchoRequest: " << req.message() << " Seq: " << header.seq_id);
                        
                        crpc::EchoResponse resp;
                        resp.set_message("Echo: " + req.message());
                        
                        std::string resp_str;
                        resp.SerializeToString(&resp_str);
                        
                        RpcHeader resp_header = header;
                        resp_header.msg_type = 1; // Response
                        
                        Buffer write_buf;
                        Protocol::EncodeMessage(write_buf, resp_header, resp_str);
                        
                        conn->Send(write_buf.ReadBegin(), write_buf.ReadableBytes());
                    } else {
                        LOG_ERROR("Failed to parse EchoRequest");
                    }
                }
            }
        });

        server.Start();
        io_ctx.Run();

    } catch (const std::exception& e) {
        LOG_ERROR("Exception: " << e.what());
    }
    
    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}
