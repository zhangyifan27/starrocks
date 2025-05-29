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

} // namespace starrocks
