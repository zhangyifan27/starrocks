#include "io/remote_node_cache.h"

#include <fmt/format.h>

#include <algorithm>
#include <exception>

#include "block_cache/block_cache.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/statusor.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/ref_count_closure.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks::io {

/**
 * RAII wrapper for RefCountClosure to ensure proper cleanup and exception safety.
 * 
 * This guard ensures that:
 * 1. RefCountClosure is properly initialized with ref count = 1
 * 2. Memory is cleaned up even if exceptions occur before join()
 * 3. No memory leaks in error paths
 * 4. Follows RAII principles for resource management
 */
template <typename T>
class RefCountClosureGuard {
public:
    explicit RefCountClosureGuard() : _closure(new RefCountClosure<T>()) { _closure->ref(); }

    ~RefCountClosureGuard() {
        // RefCountClosure uses ref counting - when unref() returns true,
        // it means the ref count reached 0 and the object should be deleted
        if (_closure && _closure->unref()) {
            delete _closure;
        }
    }

    // Non-copyable
    RefCountClosureGuard(const RefCountClosureGuard&) = delete;
    RefCountClosureGuard& operator=(const RefCountClosureGuard&) = delete;

    // Movable
    RefCountClosureGuard(RefCountClosureGuard&& other) noexcept : _closure(other._closure) { other._closure = nullptr; }

    RefCountClosureGuard& operator=(RefCountClosureGuard&& other) noexcept {
        if (this != &other) {
            // Clean up current closure if any
            if (_closure && _closure->unref()) {
                delete _closure;
            }
            _closure = other._closure;
            other._closure = nullptr;
        }
        return *this;
    }

    RefCountClosure<T>* get() const { return _closure; }
    RefCountClosure<T>* operator->() const { return _closure; }
    RefCountClosure<T>& operator*() const { return *_closure; }

private:
    RefCountClosure<T>* _closure;
};

Status RemoteNodeCache::init(RuntimeState* state) {
    if (_node_address.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }
    _brpc_stub = state->exec_env()->brpc_stub_cache()->get_stub(_node_address);
    if (UNLIKELY(_brpc_stub == nullptr)) {
        auto msg = fmt::format("The brpc stub of {}:{} is null.", _node_address.hostname, _node_address.port);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

Status RemoteNodeCache::read_buffer(const std::string& cache_key, const off_t offset, const size_t size,
                                    IOBuffer* buffer, const ReadCacheOptions& options) {
    if (cache_key.empty()) {
        return Status::InvalidArgument("cache_key cannot be empty");
    }
    if (offset < 0) {
        return Status::InvalidArgument("offset cannot be negative");
    }
    if (buffer == nullptr) {
        return Status::InvalidArgument("buffer cannot be null");
    }
    if (!_brpc_stub) {
        return Status::InternalError("brpc stub not initialized");
    }
    if (size == 0) {
        return Status::OK();
    }

    PReadNodeCacheRequest request;
    request.set_cache_key(cache_key);
    request.set_offset(offset);
    request.set_size(size);
    request.mutable_options()->set_use_adaptor(options.use_adaptor);

    RefCountClosureGuard<PReadNodeCacheResult> closure_guard;
    auto brpc_closure = closure_guard.get();
    brpc_closure->ref(); // for the rpc
    int32_t timeout_ms = _calculate_timeout(size);
    brpc_closure->cntl.set_timeout_ms(timeout_ms);
    SET_IGNORE_OVERCROWDED(brpc_closure->cntl, load);

    _brpc_stub->read_node_cache(&brpc_closure->cntl, &request, &brpc_closure->result, brpc_closure);
    brpc_closure->join();
    if (brpc_closure->cntl.Failed()) {
        std::string error_msg = fmt::format("Failed to read node cache: key={}, offset={}, size={}, error={}",
                                            cache_key, offset, size, brpc_closure->cntl.ErrorText());
        LOG(WARNING) << error_msg;
        return Status::InternalError(error_msg);
    }
    auto status = Status(brpc_closure->result.status());
    if (!status.ok()) {
        DLOG(WARNING) << "read node cache from " << _node_address.hostname << ":" << _node_address.port
                     << " failed: " << status.message();
        return status;
    }

    DLOG(INFO) << "success read node cache with length: " << brpc_closure->result.data().length();
    auto length = (size_t)brpc_closure->result.data().length();
    buffer->raw_buf().append((void*)brpc_closure->result.data().c_str(), length);
    return status;
}

} // namespace starrocks::io