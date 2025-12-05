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

TEST(QueryContextExecStatsTest, TestUpdateExecStatsAPIs) {
    // Build RuntimeState & QueryContext skeleton.
    ExecEnv* exec_env = ExecEnv::GetInstance();
    TExecPlanFragmentParams params;
    params.params.query_id.__set_hi(300);
    params.params.query_id.__set_lo(300);

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

    // Apply updates.
    query_ctx->update_push_rows_stats(kNodeId, 10l);
    query_ctx->update_pull_rows_stats(kNodeId, 20l);
    query_ctx->update_pred_filter_stats(kNodeId, 5l);
    query_ctx->update_index_filter_stats(kNodeId, 3l);
    query_ctx->update_rf_filter_stats(kNodeId, 2l);

    // Overwrite pull_rows value.
    query_ctx->force_set_pull_rows_stats(kNodeId, 100l);

    // Fetch stats via public serialization helper.
    auto stats = query_ctx->intermediate_query_statistic();
    PQueryStatistics pb;
    stats->to_pb(&pb);

    ASSERT_EQ(1, pb.node_exec_stats_items_size());
    const auto& item = pb.node_exec_stats_items(0);
    EXPECT_EQ(kNodeId, item.node_id());
    EXPECT_EQ(10l, item.push_rows());
    EXPECT_EQ(100l, item.pull_rows());
    EXPECT_EQ(5l, item.pred_filter_rows());
    EXPECT_EQ(3l, item.index_filter_rows());
    EXPECT_EQ(2l, item.rf_filter_rows());
}

} // namespace starrocks::pipeline
