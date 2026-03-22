#include "registry.h"
#include "logger.h"
#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <stdexcept>
#include <thread>

struct Registry::Impl {
    std::shared_ptr<etcd::Client> client;
    std::shared_ptr<etcd::KeepAlive> keep_alive;
    
    Impl(const std::string& url) {
        client = std::make_shared<etcd::Client>(url);
    }
};

Registry::Registry(const std::string& etcd_url) : impl_(std::make_unique<Impl>(etcd_url)) {
    LOG_INFO("Registry constructed with etcd URL: " << etcd_url);
}

Registry::~Registry() {
    if (impl_->keep_alive) {
        impl_->keep_alive->Cancel();
    }
}

void Registry::RegisterService(const std::string& service_name, const std::string& endpoint, int ttl_seconds) {
    auto res = impl_->client->leasegrant(ttl_seconds).get();
    if (!res.is_ok()) {
        LOG_ERROR("Failed to grant lease: " << res.error_message());
        throw std::runtime_error("lease grant failed");
    }
    
    int64_t lease_id = res.value().lease();
    std::string key = "/crpc/services/" + service_name + "/" + endpoint;
    
    auto put_res = impl_->client->put(key, endpoint, lease_id).get();
    if (!put_res.is_ok()) {
        LOG_ERROR("Failed to register service to etcd: " << put_res.error_message());
        throw std::runtime_error("service registration failed");
    }
    
    impl_->keep_alive = impl_->client->leasekeepalive(lease_id);
    
    LOG_INFO("Successfully registered service " << service_name << " at " << endpoint << " with TTL " << ttl_seconds << "s");
}
