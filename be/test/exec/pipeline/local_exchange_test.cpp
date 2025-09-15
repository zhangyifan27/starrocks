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

#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "testutil/exprs_test_helper.h"
#include "testutil/desc_tbl_helper.h"
#include "testutil/column_test_helper.h"

#include <gtest/gtest.h>

namespace starrocks::pipeline {

class LocalExchangerTest : public ::testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}

    std::shared_ptr<ChunkBufferMemoryManager> create_memory_mgr(size_t num_partitions, int buffer_size) {
        return std::make_shared<ChunkBufferMemoryManager>(num_partitions, buffer_size);
    }

    std::shared_ptr<LocalExchangeSourceOperatorFactory> create_lx_source(
            RuntimeState* state,
            const std::shared_ptr<ChunkBufferMemoryManager>& mem_mgr, 
            int32_t degree_of_parallelism,
            std::vector<OperatorPtr>& source_ops) {
        auto rs = std::make_shared<LocalExchangeSourceOperatorFactory>(
            _next_operator_id++, _next_plan_node_id++, mem_mgr);
        rs->set_runtime_state(state);
        for (auto i = 0; i < degree_of_parallelism; ++i) {
            source_ops.push_back(rs->create(degree_of_parallelism, i));
        }
        return rs;
    }

    std::shared_ptr<LocalExchangeSinkOperatorFactory> create_lx_sink(
            const std::shared_ptr<LocalExchanger>& exchanger, 
            int32_t degree_of_parallelism,
            std::vector<OperatorPtr>& sink_ops) {
        auto ptr = std::make_shared<LocalExchangeSinkOperatorFactory>(
                _next_operator_id++, _next_plan_node_id++, exchanger);
        for (auto i = 0; i < degree_of_parallelism; ++i) {
            sink_ops.push_back(ptr->create(degree_of_parallelism, i)); 
        }
        return ptr;
    }

    std::shared_ptr<RandomPassthroughExchanger> create_random_exchanger(
            const std::shared_ptr<ChunkBufferMemoryManager>& mem_mgr,
            const std::shared_ptr<LocalExchangeSourceOperatorFactory>& source_op) {
        return std::make_shared<RandomPassthroughExchanger>(mem_mgr, source_op.get());
    }

    std::shared_ptr<AdaptivePassthroughExchanger> create_adaptive_exchanger(
            const std::shared_ptr<ChunkBufferMemoryManager>& mem_mgr,
            const std::shared_ptr<LocalExchangeSourceOperatorFactory>& source_op) {
        return std::make_shared<AdaptivePassthroughExchanger>(mem_mgr, source_op.get());
    }

    std::shared_ptr<PartitionExchanger> create_partition_exchanger(
            const std::shared_ptr<ChunkBufferMemoryManager>& mem_mgr, 
            const std::shared_ptr<LocalExchangeSourceOperatorFactory>& source_op,
            const TPartitionType::type part_type,
            const std::vector<ExprContext*>& partition_expr_ctxs,
            bool enable_optimized,
            const std::optional<std::vector<uint32_t>>& bucket_to_partition = std::nullopt) {
        return std::make_shared<PartitionExchanger>(
            mem_mgr, source_op.get(), part_type, partition_expr_ctxs, enable_optimized, bucket_to_partition);
    }

    std::vector<ExprContext*> create_partition_expr(
        const SlotTypeInfoArray& slot_info,
        const std::vector<SlotId>& col_ids) {
      std::vector<ExprContext*> rs;
      for (auto& slot_id : col_ids) {
        const auto& info = slot_info[slot_id];
        auto type = ExprsTestHelper::create_scalar_type_desc(to_thrift(std::get<1>(info)));
        auto t_expr_node = ExprsTestHelper::create_slot_expr_node(0, slot_id, type, false);
        auto* col_ref = _pool.add(new ColumnRef(t_expr_node)); 
        rs.push_back(_pool.add(new ExprContext(col_ref)));
      }
      return rs;
    }

    template <typename ExchangerCreator>
    void test_exchange_multi_sink_multi_source(
            int32_t num_partitions,
            int sinker_degree_of_parallelism,
            const std::vector<ChunkPtr>& chunks,
            ExchangerCreator creator) {
        // create lx 
        RuntimeState dummy_runtime_state;
        dummy_runtime_state.set_chunk_size(4096);

        auto mem_mgr = create_memory_mgr(num_partitions, 1024* 1024* 10);
        std::vector<OperatorPtr> op_holder;
        auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
        auto exchanger = creator(mem_mgr, local_exchange_source);

        std::vector<OperatorPtr> sink_ops;
        auto lx_sink_factory = create_lx_sink(exchanger, sinker_degree_of_parallelism, sink_ops);
        EXPECT_EQ(sink_ops.size(), sinker_degree_of_parallelism);
        EXPECT_TRUE(lx_sink_factory->prepare(&dummy_runtime_state).ok());

        EXPECT_EQ(exchanger->get_memory_usage(), 0);

        for (auto i = 0; i < sink_ops.size(); ++i) {
            auto status = sink_ops[i]->prepare(&dummy_runtime_state);
            EXPECT_TRUE(status.ok());
            EXPECT_TRUE(sink_ops[i]->need_input());
        }

        for (auto i = 0; i < chunks.size(); ++i) {
            auto sink_ind = i % sinker_degree_of_parallelism;
            EXPECT_TRUE(sink_ops[sink_ind]->push_chunk(&dummy_runtime_state, chunks[i]).ok());
        }

        for (auto& op : sink_ops) {
            EXPECT_TRUE(op->set_finishing(&dummy_runtime_state).ok());
        }

        const auto& sources = local_exchange_source->get_sources();
        // check all rows
        size_t all_rows_number = 0;
        for (auto i = 0; i < sources.size(); ++i) {
            EXPECT_TRUE(sources[i]->has_output() || chunks.empty());
            while (sources[i]->has_output()) {
                auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
                EXPECT_TRUE(pull_status.status().ok());
                auto pulled_chunk = pull_status.value();
                EXPECT_TRUE(pulled_chunk);
                all_rows_number += pulled_chunk->num_rows();
            }
            EXPECT_TRUE(sources[i]->is_finished());
            EXPECT_TRUE(sources[i]->set_finishing(&dummy_runtime_state).ok());
            EXPECT_TRUE(sources[i]->set_finished(&dummy_runtime_state).ok());
        }

        size_t target_rows_number = 0;
        for (auto i = 0; i < chunks.size(); ++i) {
            target_rows_number += chunks[i]->num_rows();
        }
        EXPECT_EQ(all_rows_number, target_rows_number);
    }

    void test_rand_passthrough_exchanger_mutli_sink_multi_source(
            int32_t num_partitions,
            int sinker_degree_of_parallelism,
            const std::vector<ChunkPtr>& chunks) {
        test_exchange_multi_sink_multi_source(
                num_partitions,
                sinker_degree_of_parallelism,
                chunks,
                [this](auto mem_mgr, auto local_exchange_source) {
                    return create_random_exchanger(mem_mgr, local_exchange_source);
                });
    }

    void test_adaptive_passthrough_exchanger_multi_sink_multi_source(
            int32_t num_partitions,
            int sinker_degree_of_parallelism,
            const std::vector<ChunkPtr>& chunks) {
        test_exchange_multi_sink_multi_source(
                num_partitions,
                sinker_degree_of_parallelism,
                chunks,
                [this](auto mem_mgr, auto local_exchange_source) {
                    return create_adaptive_exchanger(mem_mgr, local_exchange_source);
                });
    }

    void test_partition_exchanger_multi_sink_multi_source(
            int32_t num_partitions,
            int sinker_degree_of_parallelism,
            const SlotTypeInfoArray& info,
            const std::vector<SlotId>& partition_key,
            const std::vector<ChunkPtr>& chunks,
            const TPartitionType::type type,
            const std::optional<std::vector<uint32_t>>& bucket_to_partition = std::nullopt) {
        
        test_exchange_multi_sink_multi_source(
                num_partitions,
                sinker_degree_of_parallelism,
                chunks,
                [&](auto mem_mgr, auto local_exchange_source) {
                    auto expr_ctxs = create_partition_expr(info, partition_key);
                    auto exchanger = create_partition_exchanger(
                            mem_mgr, local_exchange_source, type,
                            expr_ctxs, true, bucket_to_partition);
                    return exchanger;
                });
    }

    void test_partition_exchanger_one_sink_multi_source(
        int32_t num_partitions, 
        const SlotTypeInfoArray& info,
        const std::vector<SlotId>& partition_key,
        const std::vector<ChunkPtr>& chunks, 
        const TPartitionType::type type,
        const std::optional<std::vector<uint32_t>>& bucket_to_partition = std::nullopt) {
        test_partition_exchanger_multi_sink_multi_source(
                num_partitions,
                1,
                info,
                partition_key,
                chunks,
                type,
                bucket_to_partition); 
    }

protected:
    ObjectPool _pool;
    uint32_t _next_operator_id;
    int32_t _next_plan_node_id;
};

TEST_F(LocalExchangerTest, rand_passthrough_exchanger) {
    SlotTypeInfoArray info = {
        {"c1", TYPE_BIGINT, false},
        {"c2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 20; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map); 
        input_chunks.push_back(chunk);
    }

    test_rand_passthrough_exchanger_mutli_sink_multi_source(
            8,
            1,
            {});

    test_rand_passthrough_exchanger_mutli_sink_multi_source(
            8,
            8,
            input_chunks);

    test_rand_passthrough_exchanger_mutli_sink_multi_source(
            3,
            8,
            input_chunks);
}

TEST_F(LocalExchangerTest, adaptive_passthrough_exchanger) {
    SlotTypeInfoArray info = {
        {"c1", TYPE_BIGINT, false},
        {"c2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 50; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map); 
        input_chunks.push_back(chunk);
    }

    test_adaptive_passthrough_exchanger_multi_sink_multi_source(
            8,
            1,
            {});

    test_adaptive_passthrough_exchanger_multi_sink_multi_source(
            8,
            8,
            {input_chunks[0]});

    test_adaptive_passthrough_exchanger_multi_sink_multi_source(
            3,
            8,
            input_chunks);
}

TEST_F(LocalExchangerTest, partition_exchange_multi_sink_multi_source) {
    SlotTypeInfoArray info = {
        {"c1", TYPE_BIGINT, false},
        {"c2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 20; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map); 
        input_chunks.push_back(chunk);
    }

    test_partition_exchanger_multi_sink_multi_source(
            8,
            4,
            info,
            {0},
            input_chunks,
            TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED);

    test_partition_exchanger_multi_sink_multi_source(
            8,
            4,
            info,
            {0},
            input_chunks,
            TPartitionType::type::HASH_PARTITIONED);

    test_partition_exchanger_multi_sink_multi_source(
            2,
            4,
            info,
            {0},
            {},
            TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED);
}

TEST_F(LocalExchangerTest, partition_exchange_one_sink_multi_source) {
    SlotTypeInfoArray info = {
        {"c1", TYPE_BIGINT, false},
        {"c2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 20; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map); 
        input_chunks.push_back(chunk);
    }

    test_partition_exchanger_one_sink_multi_source(
            8,
            info,
            {0},
            input_chunks,
            TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED);

    test_partition_exchanger_one_sink_multi_source(
            8,
            info,
            {0, 1},
            input_chunks,
            TPartitionType::type::HASH_PARTITIONED);

    // test empty input chunk
    test_partition_exchanger_one_sink_multi_source(
            10,
            info,
            {0},
            {},
            TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED);
}

TEST_F(LocalExchangerTest, partition_exchange_const_column) {
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };
    
    std::vector<int64_t> values;
    for (auto i = 0; i < 32; ++i) {
          values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);

    auto data_column = Int64Column::create();
    data_column->append(1);

    auto const_col2 = ConstColumn::create(std::move(data_column), col1->size());
    Columns const_cols = {col1, const_col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    ChunkPtr const_input_chunk = std::make_shared<Chunk>(const_cols, map);
    Columns cols = {col1, col2};
    ChunkPtr input_chunk = std::make_shared<Chunk>(cols, map);

    int32_t num_partitions = 8;
    dummy_runtime_state.set_chunk_size(4096);

    auto expr_ctxs = create_partition_expr(info, {0, 1});
    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);
    std::vector<OperatorPtr> op_holder;
    auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
    auto exchanger = create_partition_exchanger(
        mem_mgr, local_exchange_source, TPartitionType::type::HASH_PARTITIONED, 
        expr_ctxs, true);

    // prepare local exchanger in sink factory
    auto status = exchanger->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // one sink operator
    exchanger->incr_sinker();

    EXPECT_EQ(exchanger->get_memory_usage(), 0);

    status = exchanger->accept(const_input_chunk, 0);
    EXPECT_TRUE(status.ok());

    status = exchanger->accept(input_chunk, 0);
    EXPECT_TRUE(status.ok());

    EXPECT_TRUE(!local_exchange_source->get_sources()[0]->has_output());

    // one sink finish;
    exchanger->finish(&dummy_runtime_state);
    
    auto sources = local_exchange_source->get_sources();
    EXPECT_EQ(sources.size(), num_partitions);

    std::unordered_set<int64_t> col2_ndv;
    size_t all_rows_number = 0;
    for (auto i = 0; i < sources.size(); ++i) {
        // each one only have one chunk
        EXPECT_TRUE(sources[i]->has_output());
        auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
        EXPECT_TRUE(pull_status.status().ok());
        auto chunk = pull_status.value();
        for (auto r = 0; r < chunk->num_rows(); ++r) {
            col2_ndv.insert(chunk->columns()[1]->get(r).get_int64());
        }
        all_rows_number += chunk->num_rows();
    }

    EXPECT_EQ(all_rows_number, input_chunk->num_rows() + const_input_chunk->num_rows());
    EXPECT_EQ(col2_ndv.size(), 32);
}

TEST_F(LocalExchangerTest, partition_exchange_basic) {
    /**
     * One sinker multi sources
     * data size < one chunk size
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };
    
    std::vector<int64_t> values;
    for (auto i = 0; i < 128; ++i) {
          values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    ChunkPtr input_chunk = std::make_shared<Chunk>(cols, map);
  
    int32_t num_partitions = 8;
    dummy_runtime_state.set_chunk_size(4096);

    auto expr_ctxs = create_partition_expr(info, {0, 1});
    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);
    std::vector<OperatorPtr> op_holder;
    auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
    auto exchanger = create_partition_exchanger(
        mem_mgr, local_exchange_source, TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED, 
        expr_ctxs, true);

    // prepare local exchanger in sink factory
    auto status = exchanger->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // one sink operator
    exchanger->incr_sinker();

    EXPECT_EQ(exchanger->get_memory_usage(), 0);

    status = exchanger->accept(input_chunk, 0);
    EXPECT_TRUE(status.ok());

    // no output
    EXPECT_TRUE(!local_exchange_source->get_sources()[0]->has_output());

    // one sink finish;
    exchanger->finish(&dummy_runtime_state);
    
    auto sources = local_exchange_source->get_sources();
    EXPECT_EQ(sources.size(), num_partitions);

    size_t all_rows_number = 0;
    for (auto i = 0; i < sources.size(); ++i) {
        // each one only have one chunk
        EXPECT_TRUE(sources[i]->has_output());
        auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
        EXPECT_TRUE(pull_status.status().ok());
        auto chunk = pull_status.value();
        all_rows_number += chunk->num_rows();
    }
    EXPECT_EQ(all_rows_number, input_chunk->num_rows());
}

TEST_F(LocalExchangerTest, multi_sink_finish_sequence) {
    /**
     * 测试多个sink的场景，确保只有最后一个sink finish后才会set_finish所有source
     * 验证数据不会因为部分sink提前finish而丢失
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    dummy_runtime_state.set_chunk_size(4096);

    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 20; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map);
        input_chunks.push_back(chunk);
    }

    int32_t num_partitions = 8;
    int32_t num_sinks = 4;

    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);
    std::vector<OperatorPtr> op_holder;
    auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);

    // 创建分区交换器
    auto expr_ctxs = create_partition_expr(info, {0});
    auto exchanger = create_partition_exchanger(
        mem_mgr, local_exchange_source, TPartitionType::type::HASH_PARTITIONED,
        expr_ctxs, true);

    // 准备交换器
    auto status = exchanger->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // 创建多个sink操作符
    std::vector<OperatorPtr> sink_ops;
    auto lx_sink_factory = create_lx_sink(exchanger, num_sinks, sink_ops);

    // 验证sink数量
    EXPECT_EQ(sink_ops.size(), num_sinks);

    // 准备所有sink操作符
    for (auto& op : sink_ops) {
        EXPECT_TRUE(op->prepare(&dummy_runtime_state).ok());
    }

    // 模拟数据分发到不同sink
    for (auto i = 0; i < input_chunks.size(); ++i) {
        auto sink_idx = i % num_sinks;
        EXPECT_TRUE(sink_ops[sink_idx]->push_chunk(&dummy_runtime_state, input_chunks[i]).ok());
    }

    // 验证在部分sink finish时source不会提前结束
    for (auto i = 0; i < num_sinks - 1; ++i) {
        EXPECT_TRUE(sink_ops[i]->set_finishing(&dummy_runtime_state).ok());

        // 验证source仍然可以拉取数据
        const auto& sources = local_exchange_source->get_sources();
        for (auto& source : sources) {
            EXPECT_FALSE(source->is_finished());
        }
    }

    // 最后一个sink finish
    EXPECT_TRUE(sink_ops.back()->set_finishing(&dummy_runtime_state).ok());

    // 验证所有数据都能被拉取
    const auto& sources = local_exchange_source->get_sources();
    size_t total_rows = 0;
    for (auto& source : sources) {
        while (source->has_output()) {
            auto pull_status = source->pull_chunk(&dummy_runtime_state);
            EXPECT_TRUE(pull_status.status().ok());
            total_rows += pull_status.value()->num_rows();
        }
        EXPECT_TRUE(source->is_finished());
    }

    // 验证所有数据都被正确处理
    size_t expected_rows = 0;
    for (auto& chunk : input_chunks) {
        expected_rows += chunk->num_rows();
    }
    EXPECT_EQ(total_rows, expected_rows);
}

TEST_F(LocalExchangerTest, memory_limit_multi_sink_multi_source) {
    /**
     * 测试多个sink和多个source场景下：
     * 1. 多个sink轮流push_chunk
     * 2. 达到memory mgr内存限制后，need_input变为false
     * 3. source pull数据后恢复sink的push能力
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };

    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    dummy_runtime_state.set_chunk_size(4096);

    // 创建大块数据以快速达到内存限制
    std::vector<ChunkPtr> input_chunks;
    for (auto i = 0; i < 200; ++i) {
        ChunkPtr chunk = std::make_shared<Chunk>(cols, map);
        input_chunks.push_back(chunk);
    }

    // 设置较小的内存限制和多个sink/source
    int32_t num_partitions = 8;  // source数量
    int32_t num_sinks = 4;       // sink数量
    int32_t memory_limit = 10 * 1024; // 10KB内存限制

    auto mem_mgr = create_memory_mgr(num_partitions, memory_limit);
    std::vector<OperatorPtr> op_holder;
    auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);

    // 创建分区交换器
    auto expr_ctxs = create_partition_expr(info, {0});
    auto exchanger = create_partition_exchanger(
        mem_mgr, local_exchange_source, TPartitionType::type::HASH_PARTITIONED,
        expr_ctxs, true);

    // 准备交换器
    auto status = exchanger->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // 创建多个sink操作符
    std::vector<OperatorPtr> sink_ops;
    auto lx_sink_factory = create_lx_sink(exchanger, num_sinks, sink_ops);

    // 验证sink数量
    EXPECT_EQ(sink_ops.size(), num_sinks);

    // 准备所有sink操作符
    for (auto& op : sink_ops) {
        EXPECT_TRUE(op->prepare(&dummy_runtime_state).ok());
    }

    // 初始状态下所有sink都可以接收输入
    for (auto& sink : sink_ops) {
        EXPECT_TRUE(sink->need_input());
    }

    const auto& sources = local_exchange_source->get_sources();
    // 持续push数据到不同sink直到内存达到限制
    size_t pushed_chunks = 0;
    bool memory_full = false;
    while (!memory_full && pushed_chunks < input_chunks.size()) {
        auto sink_idx = pushed_chunks % num_sinks;
        EXPECT_TRUE(sink_ops[sink_idx]->push_chunk(&dummy_runtime_state, input_chunks[pushed_chunks]).ok());
        pushed_chunks++;
        memory_full = mem_mgr->is_full();
    }

    // 验证内存达到限制后所有sink都不再接收输入
    for (auto& sink : sink_ops) {
        EXPECT_FALSE(sink->need_input());
    }
    EXPECT_GT(pushed_chunks, 0);

    // 从source拉取数据释放内存
    size_t pulled_rows = 0;
    for (auto& source : sources) {
        if (source->has_output()) {
            auto pull_status = source->pull_chunk(&dummy_runtime_state);
            EXPECT_TRUE(pull_status.status().ok());
            pulled_rows += pull_status.value()->num_rows();
            // pull one chunk
            break;
        }
    }

    // 验证释放内存后所有sink都可以继续接收输入
    for (auto& sink : sink_ops) {
        EXPECT_TRUE(sink->need_input());
    }

    EXPECT_TRUE(pushed_chunks < input_chunks.size());
    // 继续push剩余数据到不同sink
    while (pushed_chunks < input_chunks.size()) {
        auto sink_idx = pushed_chunks % num_sinks;
        EXPECT_TRUE(sink_ops[sink_idx]->push_chunk(&dummy_runtime_state, input_chunks[pushed_chunks]).ok());
        pushed_chunks++;
    }

    // 完成所有数据处理
    for (auto& sink : sink_ops) {
        EXPECT_TRUE(sink->set_finishing(&dummy_runtime_state).ok());
    }

    // 验证所有数据都被正确处理
    size_t total_rows = 0;
    for (auto& source : sources) {
        while (source->has_output()) {
            auto pull_status = source->pull_chunk(&dummy_runtime_state);
            EXPECT_TRUE(pull_status.status().ok());
            total_rows += pull_status.value()->num_rows();
        }
        EXPECT_TRUE(source->is_finished());
    }

    size_t expected_rows = 0;
    for (auto& chunk : input_chunks) {
        expected_rows += chunk->num_rows();
    }
    EXPECT_EQ(total_rows + pulled_rows, expected_rows);
}

TEST_F(LocalExchangerTest, push_chunk_failure_with_bucket_to_partition_and_wrong_type) {
    /**
     * 测试当提供bucket_to_partition但分区类型不是BUCKET_SHUFFLE_HASH_PARTITIONED时
     * 在exchange sink push_chunk阶段会报错
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };

    // 准备测试数据
    std::vector<int64_t> values;
    for (auto i = 0; i < 128; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    dummy_runtime_state.set_chunk_size(4096);
    ChunkPtr input_chunk = std::make_shared<Chunk>(cols, map);

    int32_t num_partitions = 8;

    // 准备bucket_to_partition参数
    std::vector<uint32_t> bucket_to_partition = {0, 1, 2, 3, 4, 5, 6, 7};

    auto expr_ctxs = create_partition_expr(info, {0, 1});
    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);
    std::vector<OperatorPtr> op_holder;
    auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);

    // 使用HASH_PARTITIONED类型(不是BUCKET_SHUFFLE_HASH_PARTITIONED)但提供bucket_to_partition
    auto exchanger = create_partition_exchanger(
        mem_mgr, local_exchange_source, TPartitionType::type::HASH_PARTITIONED,
        expr_ctxs, true, bucket_to_partition);

    // 准备交换器
    auto status = exchanger->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // 创建sink操作符
    std::vector<OperatorPtr> sink_ops;
    auto lx_sink_factory = create_lx_sink(exchanger, 1, sink_ops);
    EXPECT_EQ(sink_ops.size(), 1);

    // 准备sink操作符
    status = sink_ops[0]->prepare(&dummy_runtime_state);
    EXPECT_TRUE(status.ok());

    // 尝试push_chunk应该会失败
    status = sink_ops[0]->push_chunk(&dummy_runtime_state, input_chunk);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), TStatusCode::INTERNAL_ERROR);
    EXPECT_TRUE(status.message().find("Failed to create shuffler") != std::string::npos);
}

TEST_F(LocalExchangerTest, bucket_to_partition_data_distribution) {
    /**
     * 测试设置bucket_to_partition和不设置时数据分布的一致性
     * 当bucket_to_partition[i]=i时，两种情况下各分区的数据应该相同
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };

    // 准备测试数据
    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    dummy_runtime_state.set_chunk_size(4096);
    ChunkPtr input_chunk = std::make_shared<Chunk>(cols, map);

    int32_t num_partitions = 8;

    // 准备bucket_to_partition参数，设置bucket_to_partition[i]=i
    std::vector<uint32_t> bucket_to_partition(num_partitions);
    for (int i = 0; i < num_partitions; ++i) {
        bucket_to_partition[i] = i;
    }

    auto expr_ctxs = create_partition_expr(info, {0, 1});
    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);

    // 测试不设置bucket_to_partition的情况
    std::vector<size_t> rows_per_partition(num_partitions, 0);
    {
        std::vector<OperatorPtr> op_holder;
        auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
        auto exchanger = create_partition_exchanger(
            mem_mgr, local_exchange_source, TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED,
            expr_ctxs, true);

        auto status = exchanger->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        std::vector<OperatorPtr> sink_ops;
        auto lx_sink_factory = create_lx_sink(exchanger, 1, sink_ops);
        status = sink_ops[0]->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        status = sink_ops[0]->push_chunk(&dummy_runtime_state, input_chunk);
        EXPECT_TRUE(status.ok());
        status = sink_ops[0]->set_finishing(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        // 收集不设置bucket_to_partition时的数据分布
        const auto& sources = local_exchange_source->get_sources();
        for (size_t i = 0; i < sources.size(); ++i) {
            while (sources[i]->has_output()) {
                auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
                EXPECT_TRUE(pull_status.status().ok());
                rows_per_partition[i] += pull_status.value()->num_rows();
            }
        }
    }

    // 测试设置bucket_to_partition的情况
    {
        std::vector<OperatorPtr> op_holder;
        auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
        auto exchanger = create_partition_exchanger(
            mem_mgr, local_exchange_source, TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED,
            expr_ctxs, true, bucket_to_partition);

        auto status = exchanger->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        std::vector<OperatorPtr> sink_ops;
        auto lx_sink_factory = create_lx_sink(exchanger, 1, sink_ops);
        status = sink_ops[0]->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        status = sink_ops[0]->push_chunk(&dummy_runtime_state, input_chunk);
        EXPECT_TRUE(status.ok());
        status = sink_ops[0]->set_finishing(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        // 验证设置bucket_to_partition时的数据分布与不设置时相同
        const auto& sources = local_exchange_source->get_sources();
        for (size_t i = 0; i < sources.size(); ++i) {
            auto row_count = 0;
            while (sources[i]->has_output()) {
                auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
                EXPECT_TRUE(pull_status.status().ok());
                row_count += pull_status.value()->num_rows();
            }

            EXPECT_EQ(row_count, rows_per_partition[i]);
        }
    }
}

TEST_F(LocalExchangerTest, bucket_to_partition_with_1_and_2_partitions) {
    /**
     * 测试设置bucket_to_partition情况下：
     * 1. 8个bucket分别执行1个partition的case
     * 2. 8个bucket分别执行2个partition的case
     * 3. 验证两种情况下pull到的数据总量一致
     */
    SlotTypeInfoArray info = {
        {"col1", TYPE_BIGINT, false},
        {"col2", TYPE_BIGINT, false},
    };

    // 准备测试数据
    std::vector<int64_t> values;
    for (auto i = 0; i < 4096; ++i) {
        values.push_back(i);
    }

    ColumnPtr col1 = ColumnTestHelper::build_column(values);
    ColumnPtr col2 = ColumnTestHelper::build_column(values);
    Columns cols = {col1, col2};

    Chunk::SlotHashMap map;
    for (auto i = 0; i < 2; ++i) {
        map[i] = i;
    }

    RuntimeState dummy_runtime_state;
    dummy_runtime_state.set_chunk_size(4096);
    ChunkPtr input_chunk = std::make_shared<Chunk>(cols, map);

    int32_t num_partitions = 8;
    auto expr_ctxs = create_partition_expr(info, {0, 1});
    auto mem_mgr = create_memory_mgr(num_partitions, 1024 * 1024 * 10);

    // Case 1: 每个bucket对应1个partition (bucket_to_partition[i] = i % 8)
    size_t case1_total_rows = 0;
    {
        std::vector<uint32_t> bucket_to_partition(num_partitions);
        for (int i = 0; i < num_partitions; ++i) {
            bucket_to_partition[i] = 0;
        }

        std::vector<OperatorPtr> op_holder;
        auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
        auto exchanger = create_partition_exchanger(
            mem_mgr, local_exchange_source, TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED,
            expr_ctxs, true, bucket_to_partition);

        auto status = exchanger->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        std::vector<OperatorPtr> sink_ops;
        auto lx_sink_factory = create_lx_sink(exchanger, 1, sink_ops);
        status = sink_ops[0]->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        status = sink_ops[0]->push_chunk(&dummy_runtime_state, input_chunk);
        EXPECT_TRUE(status.ok());
        status = sink_ops[0]->set_finishing(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        // 收集数据总量
        const auto& sources = local_exchange_source->get_sources();
        for (size_t i = 0; i < sources.size(); ++i) {
            while (sources[i]->has_output()) {
                auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
                EXPECT_TRUE(pull_status.status().ok());
                case1_total_rows += pull_status.value()->num_rows();
            }
        }
    }

    // Case 2: 每个bucket对应2个partition (bucket_to_partition[i] = i % 4)
    size_t case2_total_rows = 0;
    {
        std::vector<uint32_t> bucket_to_partition(num_partitions);
        for (int i = 0; i < num_partitions; ++i) {
            bucket_to_partition[i] = i > num_partitions/2 ? 0 : 1; // 每个partition处理2个bucket
        }

        std::vector<OperatorPtr> op_holder;
        auto local_exchange_source = create_lx_source(&dummy_runtime_state, mem_mgr, num_partitions, op_holder);
        auto exchanger = create_partition_exchanger(
            mem_mgr, local_exchange_source, TPartitionType::type::BUCKET_SHUFFLE_HASH_PARTITIONED,
            expr_ctxs, true, bucket_to_partition);

        auto status = exchanger->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        std::vector<OperatorPtr> sink_ops;
        auto lx_sink_factory = create_lx_sink(exchanger, 1, sink_ops);
        status = sink_ops[0]->prepare(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        status = sink_ops[0]->push_chunk(&dummy_runtime_state, input_chunk);
        EXPECT_TRUE(status.ok());
        status = sink_ops[0]->set_finishing(&dummy_runtime_state);
        EXPECT_TRUE(status.ok());

        // 收集数据总量
        const auto& sources = local_exchange_source->get_sources();
        for (size_t i = 0; i < sources.size(); ++i) {
            while (sources[i]->has_output()) {
                auto pull_status = sources[i]->pull_chunk(&dummy_runtime_state);
                EXPECT_TRUE(pull_status.status().ok());
                case2_total_rows += pull_status.value()->num_rows();
            }
        }
    }

    // 验证两种情况下pull到的数据总量一致
    EXPECT_EQ(case1_total_rows, input_chunk->num_rows());
    EXPECT_EQ(case2_total_rows, input_chunk->num_rows());
}


} // ending namespace starrocks::pipeline
