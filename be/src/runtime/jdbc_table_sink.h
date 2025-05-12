#pragma once

#include "exec/data_sink.h"
#include "exec/jdbc_scanner.h"

namespace starrocks {
class JDBCTableSink : public DataSink {
public:
    JDBCTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs);
    ~JDBCTableSink() override = default;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

    std::vector<TExpr> get_output_expr() const { return _t_output_expr; }

private:
    ObjectPool* _pool;
    const std::vector<TExpr>& _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    JDBCScanContext _jdbc_ctx;
    std::string _jdbc_table_name;

    std::unique_ptr<JDBCExecutor> _writer;
    RuntimeProfile* _profile = nullptr;
};
}