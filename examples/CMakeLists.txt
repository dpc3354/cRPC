# Find Protobuf using PkgConfig for the examples
find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF REQUIRED protobuf)
find_program(PROTOC_EXEC protoc REQUIRED)

# Generate C++ sources for Protocol Buffers for the Echo Server Example
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.cc ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.h
    COMMAND ${PROTOC_EXEC} --cpp_out=${CMAKE_CURRENT_BINARY_DIR} -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/rpc_message.proto
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/rpc_message.proto
)

set(PROTO_SRCS ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.cc)
set(PROTO_HDRS ${CMAKE_CURRENT_BINARY_DIR}/rpc_message.pb.h)

# Example executable
add_executable(echo_server main.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(echo_server PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
)

target_link_libraries(echo_server PRIVATE
    crpc
    ${PROTOBUF_LIBRARIES}
)

# Latency benchmark client
add_executable(latency_bench latency_bench.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_include_directories(latency_bench PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROTOBUF_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(latency_bench PRIVATE
    crpc
    ${PROTOBUF_LIBRARIES}
)
