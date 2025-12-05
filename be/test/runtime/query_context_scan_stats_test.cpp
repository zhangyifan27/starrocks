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

#include "exec/pipeline/query_context.h"

#include "gen_cpp/data.pb.h"
#include "gtest/gtest.h"

namespace starrocks::pipeline {

TEST(QueryContextScanStatsTest, TestUpdateScanStats) {
    // Build RuntimeState & QueryContext skeleton.
    ExecEnv* exec_env = ExecEnv::GetInstance();
    TExecPlanFragmentParams params;
    params.params.query_id.__set_hi(400);
    params.params.query_id.__set_lo(400);

    // Prepare QueryContext.
    auto* query_ctx = exec_env->query_context_mgr()->get_or_register(params.params.query_id);
    query_ctx->set_query_id(params.params.query_id);
    query_ctx->set_total_fragments(1);
    query_ctx->set_delivery_expire_seconds(60);
    query_ctx->set_query_expire_seconds(60);
    query_ctx->extend_delivery_lifetime();
    query_ctx->extend_query_lifetime();
    query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                GlobalEnv::GetInstance()->query_pool_mem_tracker());

    // Prepare RuntimeState.
    RuntimeState runtime_state(exec_env);
    runtime_state.set_query_ctx(query_ctx);

    const int32_t kNodeId = 999;
    query_ctx->init_node_exec_stats({kNodeId});

    // Simulate scan of two tables.
    query_ctx->update_scan_stats(/*table_id*/ 1l, /*rows*/ 50l, /*bytes*/ 500l);
    query_ctx->update_scan_stats(/*table_id*/ 2l, /*rows*/ 70l, /*bytes*/ 700l);
    query_ctx->update_scan_stats(/*table_id*/ 1l, /*rows*/ 30l, /*bytes*/ 300l);

    // Serialize.
    auto stats = query_ctx->intermediate_query_statistic();
    PQueryStatistics pb;
    stats->to_pb(&pb);

    ASSERT_EQ(2, pb.stats_items_size());
    int64_t total_rows = 0l;
    int64_t total_bytes = 0l;
    for (const auto& item : pb.stats_items()) {
        if (item.table_id() == 1l) {
            EXPECT_EQ(80l, item.scan_rows());
            EXPECT_EQ(800l, item.scan_bytes());
        } else if (item.table_id() == 2l) {
            EXPECT_EQ(70l, item.scan_rows());
            EXPECT_EQ(700l, item.scan_bytes());
        }
        total_rows += item.scan_rows();
        total_bytes += item.scan_bytes();
    }
    EXPECT_EQ(150l, total_rows);
    EXPECT_EQ(1500l, total_bytes);
}

} // namespace starrocks::pipeline
