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

#include "exec/pipeline/limit_operator.h"
#include <gtest/gtest.h>
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "gen_cpp/data.pb.h"

#include <atomic>
#include <gtest/gtest.h>
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "gen_cpp/data.pb.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"

namespace starrocks::pipeline {

// Minimal factory required by LimitOperator constructor.
class DummyFactoryLimit : public OperatorFactory {
public:
    DummyFactoryLimit() : OperatorFactory(0, "dummy_limit", 0) {}
    OperatorPtr create(int32_t, int32_t) override { return nullptr; }
};

TEST(LimitOperatorTest, UpdateExecStats) {
    ExecEnv* env = ExecEnv::GetInstance();
    TExecPlanFragmentParams params;
    params.params.query_id.__set_hi(200);
    params.params.query_id.__set_lo(200);

    // Build QueryContext / RuntimeState skeleton.
    auto* qctx = env->query_context_mgr()->get_or_register(params.params.query_id);
    qctx->set_query_id(params.params.query_id);
    qctx->set_total_fragments(1);
    qctx->set_delivery_expire_seconds(60);
    qctx->set_query_expire_seconds(60);
    qctx->extend_delivery_lifetime();
    qctx->extend_query_lifetime();
    qctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                           GlobalEnv::GetInstance()->query_pool_mem_tracker());

    const int32_t kNodeId = 123;
    qctx->init_node_exec_stats({kNodeId});

    RuntimeState state(env);
    state.set_query_ctx(qctx);

    // Create operator under test.
    DummyFactoryLimit fac;
    std::atomic<int64_t> limit_val(100);
    auto op = std::make_shared<LimitOperator>(&fac, 1, kNodeId, 0, limit_val);
    ASSERT_TRUE(op->prepare(&state).ok());

    // Execute statistics update.
    op->update_exec_stats(&state);

    PQueryStatistics pb;
    qctx->intermediate_query_statistic()->to_pb(&pb);
    ASSERT_EQ(1, pb.node_exec_stats_items_size());
    const auto& item = pb.node_exec_stats_items(0);
    EXPECT_EQ(kNodeId, item.node_id());
}
} // namespace starrocks::pipeline