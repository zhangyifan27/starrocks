#pragma once

#include <algorithm>

#include "block_cache/block_cache.h"
#include "common/status.h"
#include "gen_cpp/internal_service.pb.h"

namespace starrocks::io {
// Remote node cache reader - provides basic read_buffer functionality
class RemoteNodeCache {
public:
    static constexpr int32_t DEFAULT_BRPC_TIMEOUT_MS = 3000;
    static constexpr int32_t MAX_BRPC_TIMEOUT_MS = 30000;

    explicit RemoteNodeCache(const TNetworkAddress& node_address) : _node_address(node_address) {}

    ~RemoteNodeCache() = default;

    // Init the brpc stub
    Status init(RuntimeState* state);

    // Read data from cache
    Status read_buffer(const std::string& cache_key, const off_t offset, const size_t size, IOBuffer* buffer,
                       const ReadCacheOptions& options);

private:
    int32_t _calculate_timeout(size_t size) const {
        // Base timeout + estimated data transfer time
        return std::min(DEFAULT_BRPC_TIMEOUT_MS + static_cast<int32_t>(size / 1024), MAX_BRPC_TIMEOUT_MS);
    }

    TNetworkAddress _node_address;
    PInternalService_Stub* _brpc_stub = nullptr;
};

} // namespace starrocks::io