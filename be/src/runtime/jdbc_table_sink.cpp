#include "runtime/jdbc_table_sink.h"

#include "runtime/jdbc_driver_manager.h"

namespace starrocks {
JDBCTableSink::JDBCTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

Status JDBCTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    const TJDBCTableSink jdbc_sink = t_sink.jdbc_table_sink;
    const TJDBCTable jdbc_table = jdbc_sink.jdbc_table;

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs, state));

    std::string driver_location;
    Status s = JDBCDriverManager::getInstance()->get_driver_location(jdbc_table.jdbc_driver_name, jdbc_table.jdbc_driver_url,
                                                                     jdbc_table.jdbc_driver_checksum, &driver_location);
    if (!s.ok()) {
        LOG(ERROR) << "get JDBC driver location failed. " << s.to_string();
        return s;
    }

    _jdbc_ctx.driver_path = driver_location;
    _jdbc_ctx.driver_class_name = jdbc_table.jdbc_driver_name;
    _jdbc_ctx.jdbc_url = jdbc_table.jdbc_url;
    _jdbc_ctx.user = jdbc_table.jdbc_user;
    _jdbc_ctx.passwd = jdbc_table.jdbc_passwd;
    _jdbc_ctx.sql = "INSERT";
    _jdbc_table_name = jdbc_table.jdbc_table;

    return Status::OK();
}

Status JDBCTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "JDBCTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));

    LOG(INFO) << "prepare JDBCTableSink " << title.str();
    return Status::OK();
}

Status JDBCTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    _writer = std::make_unique<JDBCExecutor>(_jdbc_ctx, _profile);
    RETURN_IF_ERROR(_writer->open(state));

    LOG(INFO) << "open JDBCTableSink successfully";
    return Status::OK();
}

Status JDBCTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return _writer->write(chunk, _output_expr_ctxs);
}

Status JDBCTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}
} // namespace starrocks