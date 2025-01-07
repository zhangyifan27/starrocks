// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RoutineLoadDataSourceProperties;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.io.Text;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.common.util.SmallFileMgr;
import com.starrocks.common.util.SmallFileMgr.SmallFile;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergApiConverter;
import com.starrocks.connector.iceberg.IcebergUtil;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.load.streamload.StreamLoadInfo;
import com.starrocks.load.streamload.StreamLoadMgr;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.IcebergStreamLoadPlanner;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.planner.StreamLoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.CreateRoutineLoadStmt;
import com.starrocks.sql.ast.ImportColumnDesc;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * IcebergRoutineLoadJob is a kind of RoutineLoadJob which fetch data from iceberg.
 */
public class IcebergRoutineLoadJob extends RoutineLoadJob implements GsonPreProcessable {
    private static final Logger LOG = LogManager.getLogger(IcebergRoutineLoadJob.class);

    public static final String ICEBERG_FILE_CATALOG = "iceberg";
    private static final long DEFAULT_SPLIT_SIZE = 2L * 1024 * 1024 * 1024; // 2 GB
    // Different to kafka routine load which can stop consuming when reaches the deadline and commit,
    // an iceberg routine task can only commit when all files are read or task is failed.
    // Iceberg routine task's rangeDesc.file_type is FILE_BROKER,
    // so that the underline implement on be is file_connector, it can't just read some files and skip the left.
    // Thus, if there is a small timeout set, a task may be always timeout and can never succeed.
    private static final int DEFAULT_TIMEOUT = 900;

    @SerializedName("ict")
    private String icebergCatalogType;
    @SerializedName("icm")
    private String icebergCatalogHiveMetastoreUris;
    @SerializedName("ir")
    private String icebergResourceName;
    @SerializedName("ic")
    private String icebergCatalogName;
    @SerializedName("id")
    private String icebergDatabase;
    @SerializedName("it")
    private String icebergTable = null;
    @SerializedName("icp")
    private String icebergConsumePosition = null;
    @SerializedName("bkd")
    private BrokerDesc brokerDesc;
    // iceberg properties, property prefix will be mapped to iceberg custom parameters, which can be extended in the
    // future
    @SerializedName("cp")
    private Map<String, String> customProperties = Maps.newHashMap();
    private Map<String, String> convertedCustomProperties = Maps.newHashMap();
    private org.apache.iceberg.Table iceTbl; // actual iceberg table

    private IcebergSplitDiscover discover;
    private Expr icebergWhereExpr;

    public IcebergRoutineLoadJob() {
        // for serialization, id is dummy
        super(-1, LoadDataSourceType.ICEBERG);
    }

    public IcebergRoutineLoadJob(Long id, String name, long dbId, long tableId,
                                 IcebergCreateRoutineLoadStmtConfig config, BrokerDesc brokerDesc) {
        super(id, name, dbId, tableId, LoadDataSourceType.ICEBERG);
        this.icebergCatalogType = config.getIcebergCatalogType();
        this.icebergCatalogHiveMetastoreUris = config.getIcebergCatalogHiveMetastoreUris();
        this.icebergResourceName = config.getIcebergResourceName();
        this.icebergCatalogName = config.getIcebergCatalogName();
        this.icebergDatabase = config.getIcebergDatabase();
        this.icebergTable = config.getIcebergTable();
        this.icebergConsumePosition = config.getIcebergConsumePosition();
        this.icebergWhereExpr = config.getIcebergWhereExpr();
        this.brokerDesc = brokerDesc;
        this.progress = new IcebergProgress();
    }

    @Override
    public void prepare() throws UserException {
        super.prepare();
        // should reset converted properties each time the job being prepared.
        // because the file info can be changed anytime.
        convertCustomProperties(true);
    }

    public synchronized void convertCustomProperties(boolean rebuild) throws DdlException {
        if (customProperties.isEmpty()) {
            return;
        }

        if (!rebuild && !convertedCustomProperties.isEmpty()) {
            return;
        }

        if (rebuild) {
            convertedCustomProperties.clear();
        }

        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                // convert FILE:file_name -> FILE:file_id:md5
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                SmallFile smallFile = smallFileMgr.getSmallFile(dbId, ICEBERG_FILE_CATALOG, file, true);
                convertedCustomProperties.put(entry.getKey(), "FILE:" + smallFile.id + ":" + smallFile.md5);
            } else {
                convertedCustomProperties.put(entry.getKey(), entry.getValue());
            }
        }
    }

    org.apache.iceberg.Table getIceTbl() throws UserException {
        if (iceTbl != null) {
            return iceTbl;
        }
        try {
            if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
                iceTbl = IcebergUtil.getTableFromHiveMetastore(icebergCatalogHiveMetastoreUris, icebergDatabase,
                        icebergTable);
            } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
                iceTbl = IcebergUtil.getTableFromResource(icebergResourceName, icebergDatabase, icebergTable);
            } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
                iceTbl = IcebergUtil.getTableFromCatalog(icebergCatalogName, icebergDatabase, icebergTable);
            }
            return iceTbl;
        } catch (StarRocksConnectorException | AnalysisException e) {
            throw new UserException(e);
        }
    }

    private Expression getIcebergPredicates(org.apache.iceberg.Table iceTbl, Expr whereExpr) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        UUID uuid = UUID.randomUUID();
        TUniqueId loadId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(this.tableId);
            if (table == null) {
                throw new MetaNotFoundException("table " + this.tableId + " does not exist");
            }
            Map<String, Column> dummyColumns = new HashMap<>();
            for (ImportColumnDesc importColumnDesc : getColumnDescs()) {
                dummyColumns.put(importColumnDesc.getColumnName(),
                        new Column(importColumnDesc.getColumnName(), Type.UNKNOWN_TYPE));
            }
            for (Column column : IcebergApiConverter.toFullSchemas(iceTbl.schema())) {
                dummyColumns.put(column.getName(), column);
            }
            OlapTable icebergPretendToOlapTable =
                    new OlapTable(1, name + "_dummy", new ArrayList<>(dummyColumns.values()), KeysType.DUP_KEYS,
                            ((OlapTable) table).getPartitionInfo(), new RandomDistributionInfo(3));
            for (Partition partition : table.getPartitions()) {
                icebergPretendToOlapTable.addPartition(partition);
            }
            final AtomicReference<List<Expr>> conjuncts = new AtomicReference<>();
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromRoutineLoadJob(this);
            streamLoadInfo.setWhereExpr(whereExpr);
            StreamLoadPlanner planner =
                    new StreamLoadPlanner(db, icebergPretendToOlapTable, streamLoadInfo) {
                        @Override
                        protected StreamLoadScanNode createScanNode(TUniqueId loadId, TupleDescriptor tupleDesc)
                                throws UserException {
                            StreamLoadScanNode scanNode =
                                    new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, destTable,
                                            streamLoadInfo) {
                                        @Override
                                        protected Expr analyzeAndCastFold(Expr whereExpr) {
                                            return whereExpr;
                                        }

                                        protected Expr castToSlot(SlotDescriptor slotDesc, Expr expr)
                                                throws UserException {
                                            if (slotDesc.getType() == Type.UNKNOWN_TYPE) {
                                                slotDesc.setColumn(
                                                        new Column(slotDesc.getColumn().getName(), expr.getType(),
                                                                true));
                                                return expr;
                                            } else if (!slotDesc.getType().matchesType(expr.getType())) {
                                                return expr.castTo(slotDesc.getType());
                                            } else {
                                                return expr;
                                            }
                                        }
                                    };
                            scanNode.setUseVectorizedLoad(true);
                            scanNode.init(analyzer);
                            scanNode.finalizeStats(analyzer);
                            conjuncts.set(scanNode.getConjuncts());
                            return scanNode;
                        }
                    };
            planner.plan(loadId);
            List<Expression> icebergPredicates = IcebergScanNode.preProcessConjuncts(iceTbl, conjuncts.get());
            Expression icebergWhereExpression = Expressions.alwaysTrue();
            if (!icebergPredicates.isEmpty()) {
                icebergWhereExpression = icebergPredicates.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
            }
            return icebergWhereExpression;
        } catch (Exception e) {
            LOG.warn("job " + name + " failed to parse whereExpr to iceberg predicate.", e);
            return null;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    public int getTimeoutSecond() {
        return DEFAULT_TIMEOUT;
    }

    @Override
    public void divideRoutineLoadJob(int currentConcurrentTaskNum) throws UserException {
        getIceTbl();
        IcebergProgress icebergProgress = (IcebergProgress) progress;
        Long readIcebergSnapshotsAfterTimestamp =
                IcebergCreateRoutineLoadStmtConfig.getReadIcebergSnapshotsAfterTimestamp(customProperties);
        Expression icebergWherePredicates = null;
        if (icebergWhereExpr != null || whereExpr != null) {
            icebergWherePredicates =
                    getIcebergPredicates(iceTbl, icebergWhereExpr != null ? icebergWhereExpr : whereExpr);
            String icebergPredicatesSql = icebergWhereExpr != null ? icebergWhereExpr.toSql() : whereExpr.toSql();
            jobProperties.put("icebergWherePredicates", icebergWherePredicates != null ? icebergPredicatesSql : "");
        }
        if (discover == null) {
            boolean isPrimaryTable;
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = db.getTable(tableId);
                isPrimaryTable = ((OlapTable) table).getKeysType() == KeysType.PRIMARY_KEYS;
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
            discover = new IcebergSplitDiscover(isPrimaryTable, name, iceTbl, icebergProgress, icebergConsumePosition,
                    readIcebergSnapshotsAfterTimestamp);
        }
        Long icebergPlanSplitSize =
                IcebergCreateRoutineLoadStmtConfig.getIcebergPlanSplitSize(customProperties);
        discover.setWhereExpr(icebergWherePredicates);
        discover.setCurrentConcurrentTaskNum(currentConcurrentTaskNum);
        discover.setSplitSize(icebergPlanSplitSize != null ? icebergPlanSplitSize : DEFAULT_SPLIT_SIZE);
        discover.setPaused(IcebergCreateRoutineLoadStmtConfig.getIcebergPauseTaskGen(customProperties));
        discover.setSkipOverwrite(IcebergCreateRoutineLoadStmtConfig.getIcebergSkipOverwrite(customProperties));
        discover.setMergeOnReadOverwrite(
                IcebergCreateRoutineLoadStmtConfig.getIcebergIncludeSnapshotMergeOnReadOverwrite(customProperties));
        discover.setReplacePartitions(
                IcebergCreateRoutineLoadStmtConfig.getIcebergIncludeSnapshotReplacePartitions(customProperties));

        List<RoutineLoadTaskInfo> result = new ArrayList<>();
        writeLock();
        try {
            if (state == JobState.NEED_SCHEDULE) {
                discover.start();
                // divide splits into tasks
                for (int i = 0; i < currentConcurrentTaskNum; i++) {
                    long timeToExecuteMs = System.currentTimeMillis() + taskSchedIntervalS * 1000;
                    IcebergTaskInfo icebergTaskInfo = new IcebergTaskInfo(UUID.randomUUID(), this,
                            taskSchedIntervalS * 1000, timeToExecuteMs, getTimeoutSecond() * 1000,
                            icebergConsumePosition, discover, icebergProgress);
                    LOG.debug("iceberg routine load task created: " + icebergTaskInfo);
                    routineLoadTaskInfoList.add(icebergTaskInfo);
                    result.add(icebergTaskInfo);
                }
                // change job state to running
                if (!result.isEmpty()) {
                    unprotectUpdateState(JobState.RUNNING, null, false);
                }
            } else {
                LOG.debug("Ignore to divide routine load job while job state {}", state);
            }
            // save task into queue of needScheduleTasks
            GlobalStateMgr.getCurrentState().getRoutineLoadTaskScheduler().addTasksInQueue(result);
        } finally {
            writeUnlock();
        }
    }

    @Override
    public int calculateCurrentConcurrentTaskNum() {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        int aliveBeNum = systemInfoService.getAliveBackendNumber();
        if (desireTaskConcurrentNum == 0) {
            desireTaskConcurrentNum = Config.max_routine_load_task_concurrent_num;
        }

        LOG.debug("current concurrent task number is min"
                        + "(desire task concurrent num: {}, alive be num * per job per be: {}, config: {})",
                desireTaskConcurrentNum, aliveBeNum * Config.max_iceberg_routine_load_task_num_per_be_per_job,
                Config.max_iceberg_routine_load_task_concurrent_num);
        currentTaskConcurrentNum = Math.min(
                Math.min(desireTaskConcurrentNum, aliveBeNum * Config.max_iceberg_routine_load_task_num_per_be_per_job),
                Config.max_iceberg_routine_load_task_concurrent_num);
        return currentTaskConcurrentNum;
    }

    // Through the transaction status and attachment information, to determine whether the progress needs to be updated.
    @Override
    protected boolean checkCommitInfo(RLTaskTxnCommitAttachment rlTaskTxnCommitAttachment,
                                      TransactionState txnState,
                                      TransactionState.TxnStatusChangeReason txnStatusChangeReason) {
        if (txnState.getTransactionStatus() == TransactionStatus.COMMITTED) {
            // For committed txn, update the progress.
            return true;
        }

        // For compatible reason, the default behavior of empty load is still returning "all partitions have no load data" and abort transaction.
        // In this situation, we also need update commit info.
        if (txnStatusChangeReason != null &&
                txnStatusChangeReason == TransactionState.TxnStatusChangeReason.NO_PARTITIONS) {
            // Because the max_filter_ratio of routine load task is always 1.
            // Therefore, under normal circumstances, routine load task will not return the error "too many filtered rows".
            // If no data is imported, the error "all partitions have no load data" may only be returned.
            // In this case, the status of the transaction is ABORTED,
            // but we still need to update the position to skip these error lines.
            Preconditions.checkState(txnState.getTransactionStatus() == TransactionStatus.ABORTED,
                    txnState.getTransactionStatus());
            return true;
        }

        // Running here, the status of the transaction should be ABORTED,
        // and it is caused by other errors. In this case, we should not update the position.
        LOG.debug("no need to update the progress of iceberg routine load. txn status: {}, " +
                        "txnStatusChangeReason: {}, task: {}, job: {}",
                txnState.getTransactionStatus(), txnStatusChangeReason,
                DebugUtil.printId(rlTaskTxnCommitAttachment.getTaskId()), id);
        return false;
    }

    @Override
    protected void updateProgress(RLTaskTxnCommitAttachment attachment) throws UserException {
        super.updateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected void clearTasks() {
        super.clearTasks();
        if (discover != null) {
            discover.stop();
        }
        if (state.isFinalState()) {
            discover = null;
            ((IcebergProgress) progress).clear();
        }
    }

    public int pendingAndRunningTasks() {
        int tasks = discover == null ? 0 : discover.pendingSplitsSize();
        for (RoutineLoadTaskInfo taskInfo : routineLoadTaskInfoList) {
            IcebergTaskInfo task = ((IcebergTaskInfo) taskInfo);
            if (task.hasSplits()) {
                tasks++;
            }
        }
        return tasks;
    }

    @Override
    protected void replayUpdateProgress(RLTaskTxnCommitAttachment attachment) {
        super.replayUpdateProgress(attachment);
        this.progress.update(attachment.getProgress());
    }

    @Override
    protected long getAbortedTaskNextScheduleTime(RoutineLoadTaskInfo taskInfo, String txnStatusChangeReasonStr) {
        return System.currentTimeMillis() + taskSchedIntervalS * 1000 *
                Math.min(Config.max_iceberg_routine_load_renew_task_schedule_delay_round,
                        ((IcebergTaskInfo) taskInfo).getRenewCount());
    }

    @Override
    protected RoutineLoadTaskInfo unprotectRenewTask(long timeToExecuteMs, RoutineLoadTaskInfo routineLoadTaskInfo) {
        IcebergTaskInfo oldIcebergTaskInfo = (IcebergTaskInfo) routineLoadTaskInfo;
        // add new task
        IcebergTaskInfo icebergTaskInfo =
                new IcebergTaskInfo(timeToExecuteMs, getTimeoutSecond() * 1000, oldIcebergTaskInfo);
        // remove old task
        routineLoadTaskInfoList.remove(routineLoadTaskInfo);
        // add new task
        routineLoadTaskInfoList.add(icebergTaskInfo);
        LOG.debug("iceberg routine load task renewed: " + icebergTaskInfo);
        return icebergTaskInfo;
    }

    @Override
    protected boolean unprotectNeedReschedule() throws UserException {
        if (this.state == JobState.PAUSED) {
            boolean autoSchedule = ScheduleRule.isNeedAutoSchedule(this);
            if (autoSchedule) {
                LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, name)
                        .add("current_state", this.state)
                        .add("msg", "should be rescheduled")
                        .build());
            }
            return autoSchedule;
        } else {
            return false;
        }
    }

    public TExecPlanFragmentParams plan(TUniqueId loadId, long txnId, long beId, int timeout,
                                        List<IcebergSplit> splits, String label) throws UserException {
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        if (db == null) {
            throw new MetaNotFoundException("db " + dbId + " does not exist");
        }
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(this.tableId);
            if (table == null) {
                throw new MetaNotFoundException("table " + this.tableId + " does not exist");
            }
            StreamLoadInfo streamLoadInfo = StreamLoadInfo.fromRoutineLoadJob(this);
            streamLoadInfo.setTimeout(timeout);
            StreamLoadPlanner planner =
                    new IcebergStreamLoadPlanner(db, (OlapTable) table, streamLoadInfo, brokerDesc, beId, splits);
            TExecPlanFragmentParams planParams = planner.plan(loadId);

            planParams.query_options.setLoad_job_type(TLoadJobType.ROUTINE_LOAD);
            if (planParams.query_options.enable_profile) {
                planParams.query_options.setEnable_profile(true);
                StreamLoadMgr streamLoadManager = GlobalStateMgr.getCurrentState().getStreamLoadMgr();

                StreamLoadTask streamLoadTask = streamLoadManager.createLoadTask(db, table.getName(), label,
                        "", "", taskTimeoutSecond, true, warehouseId);
                streamLoadTask.setTxnId(txnId);
                streamLoadTask.setLabel(label);
                streamLoadTask.setTUniqueId(loadId);
                streamLoadManager.addLoadTask(streamLoadTask);

                Coordinator coord =
                        getCoordinatorFactory().createSyncStreamLoadScheduler(planner, planParams.getCoord());
                streamLoadTask.setCoordinator(coord);

                QeProcessorImpl.INSTANCE.registerQuery(loadId, coord);

                LOG.info(new LogBuilder("routine load task create stream load task success").
                        add("transactionId", txnId).
                        add("label", label).
                        add("streamLoadTaskId", streamLoadTask.getId()));

            }

            // add table indexes to transaction state
            TransactionState txnState =
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getTransactionState(db.getId(), txnId);
            if (txnState == null) {
                throw new MetaNotFoundException("txn does not exist: " + txnId);
            }
            txnState.addTableIndexes(planner.getDestTable());

            return planParams;
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Override
    protected String getStatistic() {
        Map<String, Object> summary = Maps.newHashMap();
        summary.put("totalRows", Long.valueOf(totalRows));
        summary.put("loadedRows", Long.valueOf(totalRows - errorRows - unselectedRows));
        summary.put("errorRows", Long.valueOf(errorRows));
        summary.put("unselectedRows", Long.valueOf(unselectedRows));
        summary.put("receivedBytes", Long.valueOf(receivedBytes));
        summary.put("taskExecuteTimeMs", Long.valueOf(totalTaskExcutionTimeMs));
        summary.put("receivedBytesRate", Long.valueOf(receivedBytes / totalTaskExcutionTimeMs * 1000));
        summary.put("loadRowsRate",
                Long.valueOf((totalRows - errorRows - unselectedRows) / totalTaskExcutionTimeMs * 1000));
        summary.put("committedTaskNum", Long.valueOf(committedTaskNum));
        summary.put("abortedTaskNum", Long.valueOf(abortedTaskNum));
        summary.put("pendingAndRunningTasks", pendingAndRunningTasks());
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(summary);
    }

    @Override
    public String dataSourcePropertiesToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("(\n");
        sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_CATALOG_TYPE).append("\"=\"");
        sb.append(icebergCatalogType).append("\",\n");

        if (StringUtils.isNotEmpty(icebergCatalogHiveMetastoreUris)) {
            sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_CATALOG_HIVE_METASTORE_URIS)
                    .append("\"=\"");
            sb.append(icebergCatalogHiveMetastoreUris).append("\",\n");
        }

        if (StringUtils.isNotEmpty(icebergResourceName)) {
            sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_RESOURCE_NAME).append("\"=\"");
            sb.append(icebergResourceName).append("\",\n");
        }

        if (StringUtils.isNotEmpty(icebergCatalogHiveMetastoreUris)) {
            sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_CATALOG_NAME).append("\"=\"");
            sb.append(icebergCatalogName).append("\",\n");
        }

        sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_DATABASE).append("\"=\"");
        sb.append(icebergDatabase).append("\",\n");

        sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_TABLE).append("\"=\"");
        sb.append(icebergTable).append("\",\n");

        sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_CONSUME_POSITION).append("\"=\"");
        sb.append(icebergConsumePosition).append("\",\n");

        if (icebergWhereExpr != null) {
            sb.append("\"").append(IcebergCreateRoutineLoadStmtConfig.ICEBERG_CATALOG_NAME).append("\"=\"");
            sb.append(AstToSQLBuilder.toSQL(icebergWhereExpr)).append("\",\n");
        }

        if (brokerDesc != null) {
            sb.append("\"").append("brokerDesc").append("\"=\"");
            sb.append(brokerDesc.toString()).append("\",\n");
        }

        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            sb.append("\"").append("property.").append(entry.getKey()).append("\"=\"");
            sb.append(entry.getValue()).append("\",\n");
        }

        if (sb.length() >= 2) {
            sb.delete(sb.length() - 2, sb.length());
            sb.append("\n");
        }
        sb.append(")");
        return sb.toString();
    }

    public static IcebergRoutineLoadJob fromCreateStmt(CreateRoutineLoadStmt stmt) throws UserException {
        // check db and table
        Database db = GlobalStateMgr.getCurrentState().getDb(stmt.getDBName());
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, stmt.getDBName());
        }

        long tableId = -1L;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            unprotectedCheckMeta(db, stmt.getTableName(), stmt.getRoutineLoadDesc());
            Table table = db.getTable(stmt.getTableName());
            tableId = table.getId();
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }

        // init iceberg routine load job
        long id = GlobalStateMgr.getCurrentState().getNextId();
        IcebergCreateRoutineLoadStmtConfig config = stmt.getCreateIcebergRoutineLoadStmtConfig();
        IcebergRoutineLoadJob icebergRoutineLoadJob = new IcebergRoutineLoadJob(id, stmt.getName(),
                db.getId(), tableId, config, config.getBrokerDesc());
        icebergRoutineLoadJob.setOptional(stmt);
        icebergRoutineLoadJob.checkCustomProperties();
        icebergRoutineLoadJob.checkSchema(icebergRoutineLoadJob.columnDescs);
        icebergRoutineLoadJob.checkIcebergV2();
        icebergRoutineLoadJob.checkIcebergWhereExpr();

        return icebergRoutineLoadJob;
    }

    private void checkIcebergWhereExpr() throws DdlException {
        if (whereExpr == null && icebergWhereExpr == null) {
            return;
        }
        try {
            Expression expression =
                    getIcebergPredicates(iceTbl, icebergWhereExpr != null ? icebergWhereExpr : whereExpr);
            // ignore error when parsing whereExpr to icebergPredicates
            // do not ignore error when parsing icebergWhereExpr to icebergPredicates
            if (expression == null && icebergWhereExpr != null) {
                String msg = "job " + name + " failed to parse icebergWhereExpr to iceberg predicate.";
                throw new DdlException(msg);
            }
        } catch (UserException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }

    private void checkCustomProperties() throws DdlException {
        SmallFileMgr smallFileMgr = GlobalStateMgr.getCurrentState().getSmallFileMgr();
        for (Map.Entry<String, String> entry : customProperties.entrySet()) {
            if (entry.getValue().startsWith("FILE:")) {
                String file = entry.getValue().substring(entry.getValue().indexOf(":") + 1);
                // check file
                if (!smallFileMgr.containsFile(dbId, ICEBERG_FILE_CATALOG, file)) {
                    throw new DdlException("File " + file + " does not exist in db "
                            + dbId + " with globalStateMgr: " + ICEBERG_FILE_CATALOG);
                }
            }
        }
    }

    @Override
    protected void setOptional(CreateRoutineLoadStmt stmt) throws UserException {
        super.setOptional(stmt);

        if (!stmt.getCreateIcebergRoutineLoadStmtConfig().getCustomIcebergProperties().isEmpty()) {
            setCustomIcebergProperties(stmt.getCreateIcebergRoutineLoadStmtConfig().getCustomIcebergProperties());
        }
    }

    @Override
    protected String getSourceProgressString() {
        Map<String, String> sourceProgress = Maps.newHashMap();
        if (((IcebergProgress) progress).getLastSplitMeta() == null) {
            return "";
        }
        Snapshot snapshot = iceTbl.snapshot(((IcebergProgress) progress).getLastSplitMeta().getEndSnapshotId());
        if (snapshot == null) {
            return "";
        }
        sourceProgress.put("snapshot-id", String.valueOf(snapshot.snapshotId()));
        sourceProgress.put("timestamp-ms", String.valueOf(snapshot.timestampMillis()));
        sourceProgress.put("operation", snapshot.operation());
        sourceProgress.put("manifest-list", String.valueOf(snapshot.manifestListLocation()));
        sourceProgress.put("schema-id", String.valueOf(snapshot.schemaId()));
        sourceProgress.putAll(snapshot.summary());
        sourceProgress.putAll(iceTbl.properties());
        // make sure write.watermark is not empty
        String lastWatermark = sourceProgress.get("write.watermark");
        if (!StringUtils.isEmpty(lastWatermark)) {
            ((IcebergProgress) progress).setLastWatermark(lastWatermark);
        } else {
            sourceProgress.put("write.watermark", ((IcebergProgress) progress).getLastWatermark());
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(sourceProgress);
    }

    private void setCustomIcebergProperties(Map<String, String> icebergProperties) {
        this.customProperties = icebergProperties;
    }

    private void checkSchema(List<ImportColumnDesc> columnDescs) throws DdlException {
        Set<String> columnsInIceTbl = new HashSet<>();
        try {
            org.apache.iceberg.Table iceTbl = getIceTbl();
            iceTbl.refresh();
            for (Types.NestedField field : iceTbl.schema().columns()) {
                columnsInIceTbl.add(field.name());
            }
        } catch (UserException e) {
            throw new DdlException("failed to get iceberg table", e);
        }
        if (columnDescs == null || columnDescs.isEmpty()) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                Table table = db.getTable(tableId);
                for (Column column : table.getBaseSchema()) {
                    if (!columnsInIceTbl.contains(column.getName())) {
                        throw new DdlException(
                                "import column " + column.getName() + " does not exist in iceberg table");
                    }
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
            return;
        }
        for (ImportColumnDesc columnDesc : columnDescs) {
            if (columnDesc.isColumn() && !columnsInIceTbl.contains(columnDesc.getColumnName())) {
                throw new DdlException(
                        "import column " + columnDesc.getColumnName() + " does not exist in iceberg table");
            }
        }
    }

    private void checkIcebergV2() throws DdlException {
        if (!ConnectContext.get().getSessionVariable().enableIcebergRoutineLoadV2Check()) {
            LOG.info("enableIcebergRoutineLoadV2Check is set false, skip the iceberg v2 check.");
            return;
        }
        boolean isMorTable;
        try {
            org.apache.iceberg.Table iceTbl = getIceTbl();
            isMorTable = IcebergUtil.isMorTable((BaseTable) iceTbl);
            if (isMorTable) {
                throw new DdlException("Iceberg-V2:Merge-On-Read table is not support!");
            }
        } catch (UserException e) {
            throw new DdlException("failed to get iceberg table", e);
        }
        Database db = GlobalStateMgr.getCurrentState().getDb(dbId);
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            Table table = db.getTable(tableId);
            if (((OlapTable) table).getKeysType() != KeysType.PRIMARY_KEYS &&
                    ((OlapTable) table).getKeysType() != KeysType.AGG_KEYS) {
                throw new DdlException("Primary key tables fully support Iceberg v2 copy-on-write tables," +
                        " while aggregation tables have partial support. " +
                        "Besides the primary key model, consumption with overwrite mode may cause data duplication issues. " +
                        "If you are certain that the upstream COW (Copy-on-Write) table will only import data " +
                        "in append mode and are aware of the potential data duplication risks, " +
                        "you can skip this check by `set enable_iceberg_routine_load_v2_check = false`");
            }
            // TODO: Enhance the Validity Checks for the icebergRoutineLoad Import Method for Aggregation Models
            // Enhance the Validity Checks for the icebergRoutineLoad Import Method for Aggregation Models, for example:
            // 1. Allow aggregation columns to be of Bitmap or HLL types.
            // 2. Allow aggregation functions MAX, MIN, and REPLACE for INT, BIGINT, FLOAT, DOUBLE, and DECIMAL types.
            // 3. Allow the REPLACE aggregation function for CHAR, VARCHAR, and STRING types.
            // 4. Allow aggregation functions MAX, MIN, and REPLACE for DATE and DATETIME types.
            // 5. Ensure that aggregation functions for JSON, ARRAY, MAP, and STRUCT types follow valid aggregation logic.
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Override
    protected String dataSourcePropertiesJsonToString() {
        Map<String, String> dataSourceProperties = Maps.newHashMap();
        dataSourceProperties.put("icebergCatalogType", icebergCatalogType);
        dataSourceProperties.put("icebergCatalogHiveMetastoreUris", icebergCatalogHiveMetastoreUris);
        dataSourceProperties.put("icebergResourceName", icebergResourceName);
        dataSourceProperties.put("icebergCatalogName", icebergCatalogName);
        dataSourceProperties.put("icebergDatabase", icebergDatabase);
        dataSourceProperties.put("icebergTable", icebergTable);
        dataSourceProperties.put("icebergConsumePosition", icebergConsumePosition);
        dataSourceProperties.put("icebergWhereExpr", icebergWhereExpr != null ? icebergWhereExpr.toSql() : "");
        if (brokerDesc != null) {
            dataSourceProperties.put("brokerDesc", brokerDesc.toString());
        }
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(dataSourceProperties);
    }

    @Override
    protected String customPropertiesJsonToString() {
        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        return gson.toJson(customProperties);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        try {
            ((IcebergProgress) progress).cleanExpiredSplitRecords(getIceTbl());
        } catch (Exception e) {
            LOG.error("job " + name + " failed to cleanExpiredSplitRecords while writing to out, ignore", e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            ((IcebergProgress) progress).cleanExpiredSplitRecords(getIceTbl());
        } catch (Exception e) {
            LOG.error("job " + name + " failed to cleanExpiredSplitRecords while writing to out, ignore", e);
        }
        super.write(out);
        Text.writeString(out, icebergCatalogType);
        if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergCatalogHiveMetastoreUris);
        } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergResourceName);
        } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
            Text.writeString(out, icebergCatalogName);
        }
        Text.writeString(out, icebergDatabase);
        Text.writeString(out, icebergTable);
        Text.writeString(out, icebergConsumePosition);
        out.writeBoolean(brokerDesc != null);
        if (brokerDesc != null) {
            brokerDesc.write(out);
        }

        out.writeInt(customProperties.size());
        for (Map.Entry<String, String> property : customProperties.entrySet()) {
            Text.writeString(out, "property." + property.getKey());
            Text.writeString(out, property.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        icebergCatalogType = Text.readString(in);
        if (IcebergCreateRoutineLoadStmtConfig.isHiveCatalogType(icebergCatalogType)) {
            icebergCatalogHiveMetastoreUris = Text.readString(in);
        } else if (IcebergCreateRoutineLoadStmtConfig.isResourceCatalogType(icebergCatalogType)) {
            icebergResourceName = Text.readString(in);
        } else if (IcebergCreateRoutineLoadStmtConfig.isExternalCatalogType(icebergCatalogType)) {
            icebergCatalogName = Text.readString(in);
        }
        icebergDatabase = Text.readString(in);
        icebergTable = Text.readString(in);
        icebergConsumePosition = Text.readString(in);
        if (in.readBoolean()) {
            brokerDesc = BrokerDesc.read(in);
        }

        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String propertyKey = Text.readString(in);
            String propertyValue = Text.readString(in);
            if (propertyKey.startsWith("property.")) {
                this.customProperties.put(propertyKey.substring(propertyKey.indexOf(".") + 1), propertyValue);
            }
        }
        icebergWhereExpr =
                IcebergCreateRoutineLoadStmtConfig.getIcebergWhereExprFromCustomIcebergProperties(customProperties);
    }

    @Override
    public void modifyJob(RoutineLoadDesc routineLoadDesc, Map<String, String> jobProperties,
                          RoutineLoadDataSourceProperties dataSourceProperties, OriginStatement originStatement,
                          boolean isReplay) throws DdlException {
        if (!isReplay) {
            if (routineLoadDesc != null && routineLoadDesc.getColumnsInfo() != null) {
                checkSchema(routineLoadDesc.getColumnsInfo().getColumns());
            }
            // modification to whereExpr is only allowed when all current splits are finished.
            // otherwise the discover.planTasks() return different result to that before pause,
            // which may cause incorrect progress when resume from recovery if any iceberg routine load job's task
            // is still running before resume, which cause duplicate data consumed
            boolean shouldCheckModify = routineLoadDesc != null && routineLoadDesc.getWherePredicate() != null;
            if (shouldCheckModify && !((IcebergProgress) progress).allDone()) {
                throw new DdlException(
                        "Modification to whereExpr when current tasks are unfinished" +
                                " may cause duplicate data consumed, please resume and wait all tasks finished," +
                                " then pause and do alter again." +
                                " Execute 'SHOW ROUTINE LOAD FOR " + name + ";' before pause" +
                                " and check pendingAndRunningTasks to see whether all tasks are finished.");
            }
        }
        super.modifyJob(routineLoadDesc, jobProperties, dataSourceProperties, originStatement, isReplay);
        if (!isReplay) {
            checkIcebergWhereExpr();
        }
    }

    @Override
    public void modifyDataSourceProperties(RoutineLoadDataSourceProperties dataSourceProperties) throws DdlException {
        this.customProperties.putAll(dataSourceProperties.getCustomIcebergProperties());
        LOG.info("modify the data source properties of iceberg routine load job: {}, datasource properties: {}",
                this.id, dataSourceProperties);
    }

    @Override
    public void setOtherMsg(String otherMsg) {
        super.setOtherMsg(otherMsg);
        if (discover.pendingSplitsSize() > discover.getCurrentConcurrentTaskNum() * 50) {
            this.otherMsg += String.format(
                    " And iceberg routine load job [job name %s] too many tasks[%s > %s] are running, " +
                            "delay consume the new data ",
                    discover.getJobName(),
                    discover.pendingSplitsSize(), discover.getCurrentConcurrentTaskNum() * 50);
        }
    }
}
