package com.starrocks.planner;

import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TJDBCTableSink;
import com.starrocks.thrift.TTableDescriptor;

public class JDBCTableSink extends DataSink {
    private final JDBCTable table;
    private final TupleDescriptor tupleDescriptor;

    public JDBCTableSink(JDBCTable jdbcTable, TupleDescriptor tupleDescriptor) {
        this.table = jdbcTable;
        this.tupleDescriptor = tupleDescriptor;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        return prefix + "JDBC TABLE SINK\n" +
                prefix + "  " + DataPartition.UNPARTITIONED.getExplainString(explainLevel);
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.JDBC_TABLE_SINK);
        TTableDescriptor tableDesc = table.toThrift(null);
        TJDBCTableSink jdbcTableSink = new TJDBCTableSink(tableDesc.getJdbcTable());
        jdbcTableSink.setTuple_id(tupleDescriptor.getId().asInt());
        tDataSink.setJdbc_table_sink(jdbcTableSink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return null;
    }
}
