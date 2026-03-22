# Find liburing
find_path(URING_INCLUDE_DIR liburing.h)
find_library(URING_LIBRARY uring)

if(NOT URING_INCLUDE_DIR OR NOT URING_LIBRARY)
    message(FATAL_ERROR "liburing is required but not found")
endif()

file(GLOB SRCS *.cpp)

if(NOT ENABLE_ETCD)
    list(REMOVE_ITEM SRCS "${CMAKE_CURRENT_SOURCE_DIR}/registry.cpp")
endif()

# Core Library (Generic RPC Framework)
add_library(crpc STATIC ${SRCS})

target_include_directories(crpc PUBLIC 
    ${CMAKE_CURRENT_SOURCE_DIR}
)

if(ENABLE_ETCD)
    target_link_libraries(crpc PUBLIC etcd-cpp-api)
endif()

target_link_libraries(crpc PUBLIC 
    ${URING_LIBRARY} 
    pthread
)
