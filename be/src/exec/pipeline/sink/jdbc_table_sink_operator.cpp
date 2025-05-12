#include "exec/pipeline/sink/jdbc_table_sink_operator.h"

#include "exec/jdbc_scanner.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/sink/sink_io_buffer.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/jdbc_driver_manager.h"

namespace starrocks::pipeline {

class JDBCTableSinkIOBuffer : public SinkIOBuffer {
public:
    JDBCTableSinkIOBuffer(const TJDBCTableSink& t_jdbc_table_sink, std::vector<ExprContext*>& output_expr_ctxs,
                          std::string sql_str, int32_t num_sinkers, FragmentContext* fragment_ctx)
            : SinkIOBuffer(num_sinkers),
              _t_jdbc_table_sink(t_jdbc_table_sink),
              _output_expr_ctxs(output_expr_ctxs),
              _fragment_ctx(fragment_ctx),
              _sql_str(std::move(sql_str)) {
        std::stringstream title;
        title << "JDBCTableSinkIOBuffer (frag_id=" << fragment_ctx->fragment_instance_id() << ")";
        _profile = std::make_unique<RuntimeProfile>(title.str());
    }
    ~JDBCTableSinkIOBuffer() override = default;

    void close(RuntimeState* state) override;

private:
    void _add_chunk(const ChunkPtr& chunk) override;

    Status _open_writer();

    TJDBCTableSink _t_jdbc_table_sink;
    const std::vector<ExprContext*> _output_expr_ctxs;
    FragmentContext* _fragment_ctx;
    std::string _sql_str;

    std::unique_ptr<JDBCExecutor> _writer = nullptr;
    std::unique_ptr<RuntimeProfile> _profile;
};

void JDBCTableSinkIOBuffer::close(RuntimeState* state) {
    if (_writer != nullptr) {
        WARN_IF_ERROR(_writer->close(state), "close jdbc writer failed");
        _writer.reset();
    }
    SinkIOBuffer::close(state);
}

void JDBCTableSinkIOBuffer::_add_chunk(const ChunkPtr& chunk) {
    if (_writer == nullptr) {
        if (Status s = _open_writer(); !s.ok()) {
            LOG(WARNING) << "open jdbc writer failed, error:" << s.to_string();
            _fragment_ctx->cancel(s);
            return;
        }
    }

    if (Status s = _writer->write(chunk.get(), _output_expr_ctxs); !s.ok()) {
        LOG(WARNING) << "add chunk to JDBCExecutor failed, error:" << s.to_string();
        _fragment_ctx->cancel(s);
        return;
    }
}

Status JDBCTableSinkIOBuffer::_open_writer() {
    const TJDBCTable& jdbc_table = _t_jdbc_table_sink.jdbc_table;
    auto jdbc_ctx_st = JDBCScanContext::convert_jdbc_table_to_context(jdbc_table);
    if (!jdbc_ctx_st.ok()) {
        return jdbc_ctx_st.status();
    }

    jdbc_ctx_st.value().sql = _sql_str;
    _writer = std::make_unique<JDBCExecutor>(jdbc_ctx_st.value(), _profile.get());
    return _writer->open(_state);
}

Status JDBCTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return _sink_io_buffer->prepare(state, _unique_metrics.get());
}

void JDBCTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool JDBCTableSinkOperator::need_input() const {
    return _sink_io_buffer->need_input();
}

bool JDBCTableSinkOperator::is_finished() const {
    return _sink_io_buffer->is_finished();
}

Status JDBCTableSinkOperator::set_finishing(RuntimeState* state) {
    return _sink_io_buffer->set_finishing();
}

bool JDBCTableSinkOperator::pending_finish() const {
    return !_sink_io_buffer->is_finished();
}

Status JDBCTableSinkOperator::set_cancelled(RuntimeState* state) {
    _sink_io_buffer->cancel_one_sinker();
    return Status::OK();
}

StatusOr<ChunkPtr> JDBCTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from sink operator");
}

Status JDBCTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _sink_io_buffer->append_chunk(state, chunk);
}

Status JDBCTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    std::stringstream ss;
    ss << "INSERT INTO " << _t_jdbc_table_sink.jdbc_table.jdbc_table;
    const auto* output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_t_jdbc_table_sink.tuple_id);
    if (output_tuple_desc != nullptr) {
        if (output_tuple_desc->slots().size() != _output_expr_ctxs.size()) {
            return Status::InternalError("Mismatch between tuple slots and output expressions");
        }
        ss << " (";
        for (int i = 0; i < _output_expr_ctxs.size(); i++) {
            const std::string& column_name = output_tuple_desc->slots()[i]->col_name();
            ss << (i == 0 ? "" : ", ") << column_name;
        }

        ss << ")";
    } else {
        LOG(INFO) << "no columns specified in jdbc table sink, using all columns";
    }

    ss << " VALUES (";
    for (int i = 0; i < _output_expr_ctxs.size(); i++) {
        ss << (i == 0 ? "" : ",") << "?";
    }
    ss << ")";
    LOG(INFO) << "generate insert sql " << ss.str();

    _sink_io_buffer = std::make_shared<JDBCTableSinkIOBuffer>(_t_jdbc_table_sink, _output_expr_ctxs, ss.str(),
                                                              _num_sinkers, _fragment_ctx);

    return Status::OK();
}

void JDBCTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline