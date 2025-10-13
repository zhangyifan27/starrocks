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

#include "fs/fs.h"
#include <mutex>
#include <map>
#include <string>
#include <vector>
#include <algorithm>

namespace starrocks {

struct HdfsReadMetricsKey {
    static constexpr const char* kTotalOpenFSTimeNs = "TotalOpenFSTimeNs";
    static constexpr const char* kTotalOpenFileTimeNs = "TotalOpenFileTimeNs";
    static constexpr const char* kTotalReadTimeNs = "TotalReadTimeNs";

    static constexpr const char* kTotalBytesRead = "TotalBytesRead";
    static constexpr const char* kTotalLocalBytesRead = "TotalLocalBytesRead";
    static constexpr const char* kTotalShortCircuitBytesRead = "TotalShortCircuitBytesRead";
    static constexpr const char* kTotalZeroCopyBytesRead = "TotalZeroCopyBytesRead";

    // metrics for hedged read
    static constexpr const char* kTotalHedgedReadOps = "TotalHedgedReadOps";
    static constexpr const char* kTotalHedgedReadOpsInCurThread = "TotalHedgedReadOpsInCurThread";
    static constexpr const char* kTotalHedgedReadOpsWin = "TotalHedgedReadOpsWin";
};

std::unique_ptr<FileSystem> new_fs_hdfs(const FSOptions& options);

class HDFSTableReadIOSizeCounter {
public:
    static HDFSTableReadIOSizeCounter* instance() {
        static HDFSTableReadIOSizeCounter inst;
        return &inst;
    }
    void add(const std::string& table, int64_t size) {
        std::lock_guard<std::mutex> l(_mutex);
        _table_read_io_size[table] += size;
    }
    std::vector<std::pair<std::string, int64_t>> get_top_n_and_clear(size_t n) {
        std::lock_guard<std::mutex> l(_mutex);
        std::vector<std::pair<std::string, int64_t>> vec(_table_read_io_size.begin(), _table_read_io_size.end());
        std::sort(vec.begin(), vec.end(), [](const auto& a, const auto& b) { return b.second < a.second; });
        if (vec.size() > n) vec.resize(n);
        _table_read_io_size.clear();
        return vec;
    }
private:
    std::mutex _mutex;
    std::map<std::string, int64_t> _table_read_io_size;
};

class HDFSReadSizeStats {
public:
    static HDFSReadSizeStats* instance() {
        static HDFSReadSizeStats inst;
        return &inst;
    }

    void addSize(int64_t size, bool is_real_size) {
        std::lock_guard<std::mutex> l(_mutex);
        if (_read_sizes[is_real_size].size() > 1000000) {
            return;
        }
        _read_sizes[is_real_size].push_back(size);
        _total_size[is_real_size] += size;
        _count[is_real_size]++;
    }

    struct Stats {
        int64_t avg_size[2];
        int64_t p50_size[2];
        int64_t p90_size[2];
    };

    Stats get_stats_and_clear() {
        std::lock_guard<std::mutex> l(_mutex);
        Stats stats{{-1, -1}, {-1, -1}, {-1, -1}};
        if (_count[0] == 0 && _count[1] == 0) {
            return stats;
        }

        for (int i = 0; i < 2; i++) {
            if (_count[i]) {
                stats.avg_size[i] = _total_size[i] / _count[i];
            }
            std::sort(_read_sizes[i].begin(), _read_sizes[i].end());
            stats.p50_size[i] = _read_sizes[i][_count[i] * 50 / 100];
            stats.p90_size[i] = _read_sizes[i][_count[i] * 90 / 100];
        }

        for (int i = 0; i < 2; i++) {
            _read_sizes[i].clear();
            _total_size[i] = 0;
            _count[i] = 0;
        }

        return stats;
    }

private:
    std::mutex _mutex;
    std::vector<int64_t> _read_sizes[2];
    int64_t _total_size[2] = {0, 0};
    size_t _count[2] = {0, 0};
};

} // namespace starrocks
