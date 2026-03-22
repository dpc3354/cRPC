#pragma once
#include <string>
#include <memory>

class Registry {
public:
    Registry(const std::string& etcd_url);
    ~Registry();

    // Register a service with a TTL. Automatically spawns a thread to keep the lease alive.
    void RegisterService(const std::string& service_name, const std::string& endpoint, int ttl_seconds);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
