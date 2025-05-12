#pragma once

#include <utility>

#include "gen_cpp/DataSinks_types.h"
#include "runtime/jdbc_table_sink.h"

namespace starrocks::pipeline {
class JDBCTableSinkIOBuffer;

class JDBCTableSinkOperator final : public Operator {
public:
    JDBCTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          std::shared_ptr<JDBCTableSinkIOBuffer> sink_io_buffer)
            : Operator(factory, id, "jdbc_table_sink", plan_node_id, false, driver_sequence),
              _sink_io_buffer(std::move(sink_io_buffer)) {}

    ~JDBCTableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    std::shared_ptr<JDBCTableSinkIOBuffer> _sink_io_buffer;
};

class JDBCTableSinkOperatorFactory final : public OperatorFactory {
public:
    JDBCTableSinkOperatorFactory(int32_t id, const TJDBCTableSink& jdbc_sink, std::vector<TExpr> t_output_expr,
                                 int32_t num_sinkers, FragmentContext* fragment_ctx)
            : OperatorFactory(id, "jdbc_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
              _t_output_expr(std::move(t_output_expr)),
              _t_jdbc_table_sink(jdbc_sink),
              _num_sinkers(num_sinkers),
              _fragment_ctx(fragment_ctx) {}

    ~JDBCTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t dop, int32_t driver_sequence) override {
        return std::make_shared<JDBCTableSinkOperator>(this, _id, _plan_node_id, driver_sequence, _sink_io_buffer);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    TJDBCTableSink _t_jdbc_table_sink;
    int32_t _num_sinkers;

    std::shared_ptr<JDBCTableSinkIOBuffer> _sink_io_buffer;
    FragmentContext* _fragment_ctx = nullptr;
};
} // namespace starrocks::pipeline