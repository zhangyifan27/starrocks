// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.task;

import com.google.api.client.util.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.Status;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TJDBCDeleteReq;
import com.starrocks.thrift.TResourceInfo;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class JDBCDeleteTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(JDBCDeleteTask.class);

    private final List<Predicate> conditions;
    private final JDBCTable jdbcTable;
    private long transactionId;
    private CountDownLatch latch;
    private Status st = Status.OK;

    public JDBCDeleteTask(TResourceInfo resourceInfo, long backendId, long dbId, long tableId, long jobId,
                          long transactionId, JDBCTable table, List<Predicate> conditions) {
        super(resourceInfo, backendId, TTaskType.JDBC_DELETE, dbId, tableId, -1, -1, -1, jobId);
        this.conditions = conditions;
        this.jdbcTable = table;
        this.transactionId = transactionId;
    }

    public TJDBCDeleteReq toThrift() {
        TJDBCDeleteReq request = new TJDBCDeleteReq(jdbcTable.toThrift(null).getJdbcTable());
        List<String> filters = new ArrayList<>();
        List<SlotRef> slotRefs = Lists.newArrayList();
        Expr.collectList(conditions, SlotRef.class, slotRefs);
        ExprSubstitutionMap sMap = new ExprSubstitutionMap(false);
        String identifier = getIdentifierSymbol();
        for (SlotRef slotRef : slotRefs) {
            SlotRef tmpRef = (SlotRef) slotRef.clone();
            tmpRef.setTblName(null);
            tmpRef.setLabel(identifier + tmpRef.getLabel() + identifier);
            sMap.put(slotRef, tmpRef);
        }

        ArrayList<Predicate> jdbcConditions = Expr.cloneList(conditions, sMap);
        for (Predicate condition : jdbcConditions) {
            filters.add(condition.toJDBCSQL());
        }

        request.setDelete_conditions(filters);
        return request;
    }

    public void setCountDownLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void countDownLatch() {
        if (this.latch != null) {
            latch.countDown();
        }
    }

    public void setStatus(Status status) {
        if (!status.ok()) {
            st = status;
        }
    }

    public Status getStatus() {
        return st;
    }

    public long getTransactionId() {
        return transactionId;
    }

    private String getIdentifierSymbol() {
        return isMysql() ? "`" : "";
    }

    private boolean isMysql() {
        JDBCResource resource = (JDBCResource) GlobalStateMgr.getCurrentState().getResourceMgr()
                .getResource(jdbcTable.getResourceName());
        // Compatible with jdbc catalog
        String jdbcURI = resource != null ? resource.getProperty(JDBCResource.URI) : jdbcTable.getProperty(JDBCResource.URI);
        return jdbcURI.startsWith("jdbc:mysql");
    }
}
