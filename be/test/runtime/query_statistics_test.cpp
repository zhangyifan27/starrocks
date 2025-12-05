// Copyright 2025-present StarRocks, Inc.
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

#include "runtime/query_statistics.h"

#include "gen_cpp/data.pb.h"
#include "gtest/gtest.h"

#include "testutil/assert.h"
#include "testutil/sync_point.h"

namespace starrocks {

TEST(QueryStatisticsTest, TestUpdateStatsAndToPB) {
    QueryStatistics qs;

    // Update scan statistics items.
    qs.update_stats_item(/*table_id=*/1l, /*scan_rows=*/10l, /*scan_bytes=*/100l);
    qs.update_stats_item(/*table_id=*/2l, /*scan_rows=*/20l, /*scan_bytes=*/200l);

    // Also update global counters so that they are visible in pb serialization.
    qs.add_scan_stats(30l /* rows */, 300l /* bytes */);

    // Update exec stats for 2 plan nodes.
    qs.update_exec_stats_item(/*node_id=*/3l, /*push=*/5l, /*pull=*/15l, /*pred=*/2l, /*index=*/0l, /*rf=*/1l);
    qs.update_exec_stats_item(/*node_id=*/4l, /*push=*/8l, /*pull=*/18l, /*pred=*/3l, /*index=*/1l, /*rf=*/0l);

    // Serialize to protobuf and validate.
    PQueryStatistics pb;
    qs.to_pb(&pb);

    // Verify global counters.
    ASSERT_EQ(30l, pb.scan_rows());
    ASSERT_EQ(300l, pb.scan_bytes());

    // Verify table scan stats map size.
    ASSERT_EQ(2, pb.stats_items_size());
    // Values are unordered; check via aggregation.
    int64_t total_rows = 0l;
    int64_t total_bytes = 0l;
    for (const auto& item : pb.stats_items()) {
        total_rows += item.scan_rows();
        total_bytes += item.scan_bytes();
    }
    EXPECT_EQ(30l, total_rows);
    EXPECT_EQ(300l, total_bytes);

    // Verify node exec stats.
    ASSERT_EQ(2, pb.node_exec_stats_items_size());
    // Build a map for easy lookup.
    std::unordered_map<uint32_t, const NodeExecStatsItemPB*> node_map;
    for (const auto& item : pb.node_exec_stats_items()) {
        node_map[item.node_id()] = &item;
    }
    // Node 3 expectations.
    ASSERT_TRUE(node_map.contains(3));
    EXPECT_EQ(5l, node_map[3]->push_rows());
    EXPECT_EQ(15l, node_map[3]->pull_rows());
    EXPECT_EQ(2l, node_map[3]->pred_filter_rows());
    EXPECT_EQ(0l, node_map[3]->index_filter_rows());
    EXPECT_EQ(1l, node_map[3]->rf_filter_rows());
    // Node 4 expectations.
    ASSERT_TRUE(node_map.contains(4));
    EXPECT_EQ(8l, node_map[4]->push_rows());
    EXPECT_EQ(18l, node_map[4]->pull_rows());
    EXPECT_EQ(3l, node_map[4]->pred_filter_rows());
    EXPECT_EQ(1l, node_map[4]->index_filter_rows());
    EXPECT_EQ(0l, node_map[4]->rf_filter_rows());
}

} // namespace starrocks
