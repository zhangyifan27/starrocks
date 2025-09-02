// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <condition_variable>
#include <iostream>
#include <memory>
#include <queue>

#include "block_cache/block_cache.h"
#include "block_cache/cache_options.h"
#include "block_cache/kv_cache.h"
#include "gutil/strings/fastmem.h"
#include "io/cache_input_stream.h"
#include "io/seekable_input_stream.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "util/random.h"

namespace starrocks::io {

class MockThreadPool {
public:
    using Task = std::function<void()>;

    MockThreadPool() : _mock_tracker(new MemTracker()){};
    ~MockThreadPool() { shutdown(); }

    void start() {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mock_tracker.get());
        _worker = std::thread([this] {
            std::unique_lock<std::mutex> lock(_mtx);
            while (!_tasks.empty()) {
                auto t = _tasks.front();
                _tasks.pop();
                t();
            }
            _cv.notify_all();
        });
    }

    template <typename F>
    void submit(F&& f) {
        _tasks.emplace(std::forward<F>(f));
    }

    void shutdown() {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mock_tracker.get());
        {
            std::unique_lock<std::mutex> lock(_mtx);
            _cv.wait(lock, [this] { return _tasks.empty(); });
        }
        if (_worker.joinable()) {
            _worker.join();
        }
    }

private:
    std::mutex _mtx;
    std::condition_variable _cv;
    std::queue<Task> _tasks;
    std::thread _worker;
    std::shared_ptr<MemTracker> _mock_tracker;
};

class MockBlockCache {
public:
    MockBlockCache() : _kv_cache(new MockThreadPool()){};
    ~MockBlockCache() = default;

    static MockBlockCache* instance() {
        static MockBlockCache instance;
        if (!instance._kv_cache) {
            instance._kv_cache = std::make_unique<MockThreadPool>();
        }
        return &instance;
    }

    void write_buffer(const std::string& cache_key, off_t offset, size_t size, const char* data,
                      WriteCacheOptions* options) {
        WriteCacheOptions* copied_options = new WriteCacheOptions(*options);
        copied_options->callback = options->callback;
        auto f = [copied_options]() {
            copied_options->callback(1, "");
            delete copied_options;
        };
        _kv_cache->submit(f);
    }

    void start() { _kv_cache->start(); }

    void shutdown() {
        _kv_cache->shutdown();
        _kv_cache.reset();
    }

    std::unique_ptr<MockThreadPool> _kv_cache;
};

class MockSharedBufferedInputStream : public SharedBufferedInputStream {
public:
    MockSharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, std::string filename, size_t file_size)
            : SharedBufferedInputStream(std::move(stream), std::move(filename), file_size) {
        _mock_file_size = file_size;
    }

    struct MockSharedBuffer : public SharedBuffer {
        ~MockSharedBuffer() {
            auto tracker = CurrentThread::mem_tracker();
            if (!tracker) {
                return;
            }
            if (buffer.capacity() != 0) {
                tracker->release(buffer.capacity());
            }
        }
    };

    Status _sort_and_check_overlap(std::vector<IORange>& ranges) {
        std::sort(ranges.begin(), ranges.end(), [](const IORange& a, const IORange& b) {
            if (a.offset != b.offset) {
                return a.offset < b.offset;
            }
            return a.size < b.size;
        });
        return Status::OK();
    }

    void _mock_merge_small_ranges(const std::vector<IORange>& small_ranges) {
        if (small_ranges.size() > 0) {
            auto update_map = [&](size_t from, size_t to) {
                // merge from [unmerge, i-1]
                int64_t ref_count = (to - from + 1);
                int64_t end = (small_ranges[to].offset + small_ranges[to].size);
                int64_t raw_offset = small_ranges[from].offset;
                int64_t raw_size = end - small_ranges[from].offset;
                SharedBufferPtr sb;
                if (config::orc_shared_buffer_mem_tracker_enable && mem_tracker_ptr()) {
                    sb.reset(new MockSharedBuffer, SharedBufferMemTrackerDeleter(mem_tracker_ptr()));
                } else {
                    sb.reset(new MockSharedBuffer);
                }
                sb->raw_offset = raw_offset;
                sb->raw_size = raw_size;
                sb->ref_count = ref_count;

                sb->align(_mock_align_size, _mock_file_size);
                _mock_map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
            };

            size_t unmerge = 0;
            for (size_t i = 1; i < small_ranges.size(); i++) {
                const auto& prev = small_ranges[i - 1];
                const auto& now = small_ranges[i];
                size_t now_end = now.offset + now.size;
                size_t prev_end = prev.offset + prev.size;
                if (((now_end - small_ranges[unmerge].offset) <= 8 * 1024 * 1024) &&
                    (now.offset - prev_end) <= 1 * 1024 * 1024) {
                    continue;
                } else {
                    update_map(unmerge, i - 1);
                    unmerge = i;
                }
            }
            update_map(unmerge, small_ranges.size() - 1);
        }
    }

    Status _mock_set_io_ranges_all_columns(const std::vector<IORange>& ranges) {
        if (ranges.size() == 0) {
            return Status::OK();
        }

        std::vector<IORange> check(ranges);
        RETURN_IF_ERROR(_sort_and_check_overlap(check));

        std::vector<IORange> small_ranges;
        for (const IORange& r : check) {
            if (r.size > 8 * 1024 * 1024) {
                SharedBufferPtr sb;
                if (config::orc_shared_buffer_mem_tracker_enable && mem_tracker_ptr()) {
                    sb.reset(new MockSharedBuffer, SharedBufferMemTrackerDeleter(mem_tracker_ptr()));
                } else {
                    sb.reset(new MockSharedBuffer);
                }
                sb->raw_offset = r.offset;
                sb->raw_size = r.size;
                sb->ref_count = 1;

                sb->align(_mock_align_size, _mock_file_size);
                _mock_map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
            } else {
                small_ranges.emplace_back(r);
            }
        }

        _mock_merge_small_ranges(small_ranges);
        return Status::OK();
    }

    Status _mock_set_io_ranges_active_and_lazy_columns(const std::vector<IORange>& ranges) {
        if (ranges.size() == 0) {
            return Status::OK();
        }

        // specify compare function is important. suppose we have zero range like [351,351],[351,356].
        // If we don't specify compare function, we may have [351,356],[351,351] which is bad order.
        std::vector<IORange> check(ranges);
        RETURN_IF_ERROR(_sort_and_check_overlap(check));

        std::vector<IORange> small_active_ranges;
        std::vector<bool> small_lazy_flag(ranges.size());
        small_lazy_flag.assign(ranges.size(), false);
        for (auto index = 0; index < check.size(); ++index) {
            const IORange& r = check[index];
            if (r.size > 8 * 1024 * 1024) {
                SharedBufferPtr sb;
                if (config::orc_shared_buffer_mem_tracker_enable && mem_tracker_ptr()) {
                    sb.reset(new MockSharedBuffer, SharedBufferMemTrackerDeleter(mem_tracker_ptr()));
                } else {
                    sb.reset(new MockSharedBuffer);
                }
                sb->raw_offset = r.offset;
                sb->raw_size = r.size;
                sb->ref_count = 1;

                sb->align(_mock_align_size, _mock_file_size);
                _mock_map.insert(std::make_pair(sb->raw_offset + sb->raw_size, sb));
            } else {
                if (r.is_active) {
                    small_active_ranges.emplace_back(r);
                } else {
                    small_lazy_flag[index] = true;
                }
            }
        }

        if (small_active_ranges.size() > 0) {
            _mock_merge_small_ranges(small_active_ranges);
        }

        std::vector<IORange> small_lazy_batch_ranges;
        for (auto index = 0; index < small_lazy_flag.size(); ++index) {
            if (!small_lazy_flag[index]) {
                // active column or big column
                continue;
            } else {
                // 1. there may be lazy_column locate in the middle of two active_columns,
                // such as active_column, lazy_column, active_column,
                // that two active_columns have merged and the lazy_column had be contained.
                const IORange& r = check[index];
                auto iter = _mock_map.upper_bound(r.offset);
                if (iter != _mock_map.end()) {
                    SharedBufferPtr& sb = iter->second;
                    if (sb->offset <= r.offset && sb->offset + sb->size >= r.offset + r.size) {
                        sb->ref_count++;
                        continue;
                    }
                }
                small_lazy_batch_ranges.emplace_back(r);
                // 2. there also may be active_column locate in the middle of two lazy_columns,
                // in this case active_column may be contained in two shared_buffer，
                // we should prevent that
                if (index + 1 >= small_lazy_flag.size() || !small_lazy_flag[index + 1]) {
                    _mock_merge_small_ranges(small_lazy_batch_ranges);
                    small_lazy_batch_ranges.clear();
                }
            }
        }

        return Status::OK();
    }

    Status mock_set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column) {
        if (coalesce_lazy_column || !config::io_coalesce_adaptive_lazy_active) {
            return _mock_set_io_ranges_all_columns(ranges);
        } else {
            return _mock_set_io_ranges_active_and_lazy_columns(ranges);
        }
    }

    StatusOr<SharedBufferedInputStream::SharedBufferPtr> mock_find_shared_buffer(size_t offset, size_t count) {
        auto iter = _mock_map.upper_bound(offset);
        if (iter == _mock_map.end()) {
            return Status::RuntimeError("failed to find shared buffer based on offset");
        }
        const SharedBufferPtr& sb = iter->second;
        if ((sb->offset > offset) || (sb->offset + sb->size) < (offset + count)) {
            return Status::RuntimeError("bad construction of shared buffer");
        }
        return sb;
    }

    Status mock_get_bytes(const uint8_t** buffer, size_t offset, size_t count, SharedBufferPtr shared_buffer) {
        if (!shared_buffer) {
            ASSIGN_OR_RETURN(auto ret, mock_find_shared_buffer(offset, count));
            shared_buffer = ret;
        }

        SharedBuffer& sb = *shared_buffer;
        if (sb.buffer.capacity() == 0) {
            if (mem_tracker_ptr()) {
                mem_tracker()->consume(sb.size);
            } else if (CurrentThread::mem_tracker()) {
                CurrentThread::mem_tracker()->consume(sb.size);
            }
            sb.buffer.reserve(sb.size);
            RETURN_IF_ERROR(stream()->read_at_fully(sb.offset, sb.buffer.data(), sb.size));
        }
        *buffer = sb.buffer.data() + offset - sb.offset;
        return Status::OK();
    }

    void set_mock_align_size(int64_t size) { _mock_align_size = size; }

    std::map<int64_t, SharedBufferPtr> _mock_map;
    int64_t _mock_align_size = 0;
    int64_t _mock_file_size = 0;
};

class MockCacheInputStream : public CacheInputStream {
public:
    MockCacheInputStream(const std::shared_ptr<SharedBufferedInputStream>& stream, const std::string& filename,
                         size_t size, int64_t modification_time)
            : CacheInputStream(stream, filename, size, modification_time) {
        _mock_cache = MockBlockCache::instance();
        _block_size = 4 * 1024 * 1024;
        _enable_populate_cache = true;
        _buffer_size = 4 * 1024 * 1024;
    }

    MockBlockCache* mock_cache() { return _mock_cache; }

protected:
    Status _read_block_from_local(const int64_t offset, const int64_t size, char* out) override {
        int64_t block_id = offset / _block_size;
        int64_t block_offset = block_id * _block_size;
        int64_t load_size = std::min(_block_size, _size - block_offset);

        SharedBufferPtr sb = nullptr;
        {
            // try to find data from shared buffer
            auto mock_stream = std::dynamic_pointer_cast<MockSharedBufferedInputStream>(_sb_stream);
            auto ret = mock_stream->mock_find_shared_buffer(offset, size);
            if (ret.ok()) {
                sb = ret.value();
                if (sb->buffer.capacity() > 0) {
                    strings::memcpy_inlined(out, sb->buffer.data() + offset - sb->offset, size);
                    if (_enable_populate_cache) {
                        _mock_populate_to_cache((const char*)sb->buffer.data() + block_offset - sb->offset,
                                                block_offset, load_size, sb);
                    }
                    return Status::OK();
                }
            }
        }
        return Status::NotFound("sb not found");
    }

    Status _read_blocks_from_remote(const int64_t offset, const int64_t size, char* out) override {
        const int64_t start_block_id = offset / _block_size;
        const int64_t end_block_id = (offset + size - 1) / _block_size;

        // We will load range=[read_start_offset, read_end_offset) from remote
        const int64_t block_start_offset = start_block_id * _block_size;
        const int64_t block_end_offset = std::min(end_block_id * _block_size + _block_size, _size);

        // cursors for `out`
        int64_t out_offset_cursor = offset;
        int64_t out_remain_size = size;
        char* out_pointer_cursor = out;

        for (int64_t read_offset_cursor = block_start_offset; read_offset_cursor < block_end_offset;) {
            // Everytime read at most one buffer size
            const int64_t read_size = std::min(_buffer_size, block_end_offset - read_offset_cursor);
            char* src = nullptr;

            // check [read_offset_cursor, read_size) is already in SharedBuffer
            // If existed, we can use zero copy to avoid copy data from SharedBuffer to _buffer
            SharedBufferPtr sb = nullptr;
            int64_t read_remote_ns = 0;
            {
                SCOPED_RAW_TIMER(&read_remote_ns);
                auto mock_stream = std::dynamic_pointer_cast<MockSharedBufferedInputStream>(_sb_stream);
                auto ret = mock_stream->mock_find_shared_buffer(read_offset_cursor, read_size);
                if (ret.ok()) {
                    sb = ret.value();
                    const uint8_t* buffer = nullptr;
                    RETURN_IF_ERROR(mock_stream->mock_get_bytes(&buffer, read_offset_cursor, read_size, sb));
                    src = (char*)buffer;
                } else {
                    RETURN_IF_ERROR(mock_stream->read_at_fully(read_offset_cursor, _buffer.data(), read_size));
                    src = _buffer.data();
                }
            }

            // write _buffer's data into `out`
            const int64_t shift = out_offset_cursor - read_offset_cursor;
            const int64_t out_size = std::min(read_size - shift, out_remain_size);
            if (out_size > 0) {
                strings::memcpy_inlined(out_pointer_cursor, src + shift, out_size);

                // cursor for `out`
                out_offset_cursor += out_size;
                out_pointer_cursor += out_size;
                out_remain_size -= out_size;
            }

            if (_enable_populate_cache) {
                _mock_populate_to_cache(src, read_offset_cursor, read_size, sb);
            }

            read_offset_cursor += read_size;
        }
        DCHECK_EQ(0, out_remain_size);
        DCHECK_EQ(offset + size, out_offset_cursor);
        DCHECK_EQ(out + size, out_pointer_cursor);
        return Status::OK();
    }

    void _mock_populate_to_cache(const char* p, int64_t offset, int64_t count, const SharedBufferPtr& sb) {
        int64_t begin = offset / _block_size * _block_size;
        int64_t end = std::min((offset + count + _block_size - 1) / _block_size * _block_size, _size);
        p -= (offset - begin);
        auto f = [sb, this](const char* buf, size_t off, size_t size) {
            DCHECK(off % _block_size == 0);

            WriteCacheOptions options;
            options.async = true;
            options.evict_probability = _datacache_evict_probability;
            options.priority = _priority;
            options.ttl_seconds = _ttl_seconds;
            if (options.async && sb) {
                auto cb = [sb](int code, const std::string& msg) {
                    // We only need to keep the shared buffer pointer
                    LOG_IF(WARNING, code != 0 && code != EEXIST) << "write block cache failed, errmsg: " << msg;
                };
                options.callback = cb;
                options.allow_zero_copy = true;
            }
            _mock_cache->write_buffer(_cache_key, off, size, buf, &options);
        };

        while (begin < end) {
            size_t size = std::min(_block_size, end - begin);
            f(p, begin, size);
            begin += size;
            p += size;
        }
        return;
    }

    MockBlockCache* _mock_cache;
};

} // namespace starrocks::io
