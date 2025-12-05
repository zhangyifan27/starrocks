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

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "exec/pipeline/query_context.h"
#include "exec/pipeline/operator.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "gen_cpp/data.pb.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"

namespace starrocks::pipeline {

// A dummy OperatorFactory implementation used only for constructing the operator under test.
class DummyFactory final : public OperatorFactory {
public:
    DummyFactory() : OperatorFactory(0, "dummy", 0) {}
    OperatorPtr create(int32_t /*degree_of_parallelism*/, int32_t /*driver_sequence*/) override { return nullptr; }
};

// A testing subclass that overrides prepare() to avoid HashJoiner dependencies and sets up the counters we need.
class TestHashJoinProbeOperator : public HashJoinProbeOperator {
public:
    TestHashJoinProbeOperator(OperatorFactory* factory, int32_t id, const std::string& name, int32_t plan_node_id,
                              int32_t driver_sequence)
            : HashJoinProbeOperator(factory, id, name, plan_node_id, driver_sequence, nullptr, nullptr) {}

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(OperatorWithDependency::prepare(state));
        // Manually create the counters that update_exec_stats() relies on.
        _pull_row_num_counter = ADD_COUNTER(_common_metrics, "PullRowNum", TUnit::UNIT);
        _conjuncts_input_counter = ADD_COUNTER(_common_metrics, "ConjunctsInputRows", TUnit::UNIT);
        _conjuncts_output_counter = ADD_COUNTER(_common_metrics, "ConjunctsOutputRows", TUnit::UNIT);
        _bloom_filter_eval_context.join_runtime_filter_input_counter =
                ADD_COUNTER(_common_metrics, "JoinRuntimeFilterInputRows", TUnit::UNIT);
        _bloom_filter_eval_context.join_runtime_filter_output_counter =
                ADD_COUNTER(_common_metrics, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
        return Status::OK();
    }
};

TEST(HashJoinProbeOperatorTest, test_update_exec_stats) {
    // Build RuntimeState & QueryContext skeleton.
    ExecEnv* exec_env = ExecEnv::GetInstance();
    TExecPlanFragmentParams params;
    params.params.query_id.__set_hi(100);
    params.params.query_id.__set_lo(100);

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

    const int32_t kPlanNodeId = 123;
    query_ctx->init_node_exec_stats({kPlanNodeId});

    // Instantiate operator under test.
    DummyFactory factory;
    auto op = std::make_shared<TestHashJoinProbeOperator>(&factory, 1, "hash_join_probe_test", kPlanNodeId, 0);
    ASSERT_OK(op->prepare(&runtime_state));

    // Simulate some counter values.
    COUNTER_UPDATE(op->_pull_row_num_counter, 100);
    COUNTER_UPDATE(op->_conjuncts_input_counter, 80);
    COUNTER_UPDATE(op->_conjuncts_output_counter, 50);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_input_counter, 30);
    COUNTER_UPDATE(op->_bloom_filter_eval_context.join_runtime_filter_output_counter, 10);

    // Call the function under test.
    op->update_exec_stats(&runtime_state);

    // Fetch delta statistics; they should include the numbers we just updated.
    auto qs = query_ctx->intermediate_query_statistic();
    PQueryStatistics pb;
    qs->to_pb(&pb);

    ASSERT_EQ(1, pb.node_exec_stats_items_size());
    const auto& item = pb.node_exec_stats_items(0);
    EXPECT_EQ(kPlanNodeId, item.node_id());
    EXPECT_EQ(100, item.pull_rows());
    EXPECT_EQ(30, item.pred_filter_rows());
    EXPECT_EQ(20, item.rf_filter_rows());
}

} // namespace starrocks::pipeline
