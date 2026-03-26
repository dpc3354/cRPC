# Find Protobuf using PkgConfig for the examples
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
find_program(PROTOC_EXEC protoc REQUIRED)

# Generate C++ sources for Protocol Buffers
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.cc ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.h
    COMMAND ${PROTOC_EXEC} --cpp_out=${CMAKE_CURRENT_BINARY_DIR} -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/rpc_message.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/rpc_message.proto
)

set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.h)

# Coroutine echo server (C++20 coroutines + protobuf)
add_executable(coro_echo coro_echo.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(coro_echo PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(coro_echo PRIVATE crpc ${PROTOBUF_LIBRARIES})

# Async concurrent echo server (demonstrates true in-flight multiplexing)
add_executable(coro_echo_async coro_echo_async.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(coro_echo_async PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(coro_echo_async PRIVATE crpc ${PROTOBUF_LIBRARIES})

# Latency benchmark client
add_executable(latency_bench latency_bench.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(latency_bench PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(latency_bench PRIVATE crpc ${PROTOBUF_LIBRARIES})

# Multiplexed (pipelined) benchmark client
add_executable(multiplex_bench multiplex_bench.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(multiplex_bench PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(multiplex_bench PRIVATE crpc ${PROTOBUF_LIBRARIES} pthread)

# Multi-connection multiplexed benchmark
add_executable(mt_multiplex_bench mt_multiplex_bench.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(mt_multiplex_bench PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(mt_multiplex_bench PRIVATE crpc ${PROTOBUF_LIBRARIES} pthread)

# Multi-threaded latency benchmark client
add_executable(latency_bench_mt latency_bench_mt.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(latency_bench_mt PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(latency_bench_mt PRIVATE crpc ${PROTOBUF_LIBRARIES} pthread)
