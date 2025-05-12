// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.load;

import autovalue.shaded.com.google.common.common.base.Preconditions;
import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.QueryStateException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.task.AgentBatchTask;
import com.starrocks.task.AgentTaskExecutor;
import com.starrocks.task.AgentTaskQueue;
import com.starrocks.task.JDBCDeleteTask;
import com.starrocks.transaction.TabletCommitInfo;
import com.starrocks.transaction.TabletFailInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class JDBCDeleteJob extends DeleteJob {
    private static final Logger LOG = LogManager.getLogger(JDBCDeleteJob.class);

    private static final int CHECK_INTERVAL = 1000;

    private JDBCDeleteTask task;

    public JDBCDeleteJob(long id, String label) {
        super(id, -1, label, null);
        task = null;
    }

    @Override
    public void run(DeleteStmt stmt, Database db, Table table, List<Partition> partitions)
            throws DdlException, QueryStateException {
        Preconditions.checkState(table.isJDBCTable());
        JDBCTable jdbcTable = (JDBCTable) table;
        CountDownLatch latch = new CountDownLatch(1);
        List<Predicate> deleteConditions = getDeleteConditions();

        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            AgentBatchTask batchTask = new AgentBatchTask();
            List<Long> backendIds = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendIds(true);
            Collections.shuffle(backendIds);
            Long backendId = backendIds.get(0);
            JDBCDeleteTask task = new JDBCDeleteTask(null, backendId, db.getId(), table.getId(),
                    id, -1, jdbcTable, deleteConditions);
            task.setCountDownLatch(latch);

            if (AgentTaskQueue.addTask(task)) {
                batchTask.addTask(task);
                this.task = task;
            } else {
                throw new DdlException("submit task failed");
            }

            AgentTaskExecutor.submit(batchTask);
        } catch (Throwable t) {
            LOG.warn("error occurred during delete process", t);
            throw new DdlException(t.getMessage(), t);
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        LOG.info("waiting JDBC delete job finish. timeout: {}", getTimeoutMs());
        boolean ok = false;
        try {
            long countDownTime = getTimeoutMs();
            while (countDownTime > 0) {
                if (countDownTime > CHECK_INTERVAL) {
                    countDownTime -= CHECK_INTERVAL;
                    ok = latch.await(CHECK_INTERVAL, TimeUnit.MILLISECONDS);
                    if (ok) {
                        break;
                    }
                } else {
                    ok = latch.await(countDownTime, TimeUnit.MILLISECONDS);
                    break;
                }
            }
        } catch (InterruptedException e) {
            LOG.warn("InterruptedException: ", e);
        }
        LOG.info("JDBC delete job finish");
        Status st = this.task.getStatus();
        if (!st.ok()) {
            LOG.warn(st.toString());
            throw new DdlException("failed to execute JDBC delete. JDBCTable: " + jdbcTable.getJdbcTable()
                    + ", " + st);
        }
    }

    @Override
    public long getTimeoutMs() {
        return 3600 * 1000;
    }

    @Override
    public boolean cancel(DeleteMgr.CancelType cancelType, String reason) {
        return false;
    }

    @Override
    public void clear() {
    }

    @Override
    public boolean commitImpl(Database db, long timeoutMs) throws UserException {
        return true;
    }

    @Override
    protected List<TabletCommitInfo> getTabletCommitInfos() {
        return Collections.emptyList();
    }

    @Override
    protected List<TabletFailInfo> getTabletFailInfos() {
        return Collections.emptyList();
    }
}
