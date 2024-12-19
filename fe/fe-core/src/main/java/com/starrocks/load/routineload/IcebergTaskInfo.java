// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TIcebergLoadInfo;
import com.starrocks.thrift.TIcebergSplit;
import com.starrocks.thrift.TLoadSourceType;
import com.starrocks.thrift.TPlanFragment;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class IcebergTaskInfo extends RoutineLoadTaskInfo {
    private static final Logger LOG = LogManager.getLogger(IcebergTaskInfo.class);
    private RoutineLoadMgr routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadMgr();

    private final IcebergSplitDiscover discover;
    private final String consumePosition;
    private final IcebergProgress progress;
    private List<IcebergSplit> splits;
    private int renewCount;

    public IcebergTaskInfo(UUID id, RoutineLoadJob job, long taskScheduleIntervalMs, long timeToExecuteMs, long timeoutMs,
                           String consumePosition, IcebergSplitDiscover discover,
                           IcebergProgress progress) {
        super(id, job, taskScheduleIntervalMs, timeToExecuteMs, timeoutMs);
        this.discover = discover;
        this.consumePosition = consumePosition;
        this.progress = progress;
    }

    public IcebergTaskInfo(long timeToExecuteMs, long timeoutMs, IcebergTaskInfo icebergTaskInfo) {
        super(UUID.randomUUID(), icebergTaskInfo.getJob(), icebergTaskInfo.getTaskScheduleIntervalMs(),
                resetTimeToExecuteMs(icebergTaskInfo, timeToExecuteMs), icebergTaskInfo.getBeId(), timeoutMs,
                icebergTaskInfo.getStatistics());
        this.discover = icebergTaskInfo.discover;
        this.consumePosition = icebergTaskInfo.consumePosition;
        this.progress = icebergTaskInfo.progress;
        this.splits = icebergTaskInfo.splits.stream()
                .filter((Predicate<IcebergSplit>) split -> !progress.isDone(split))
                .collect(Collectors.toList());
        this.renewCount = icebergTaskInfo.renewCount + 1;
    }

    public int getRenewCount() {
        return renewCount;
    }

    private static long resetTimeToExecuteMs(IcebergTaskInfo icebergTaskInfo, long timeToExecuteMs) {
        if (icebergTaskInfo.splits.stream()
                .anyMatch((Predicate<IcebergSplit>) split -> !icebergTaskInfo.progress.isDone(split))) {
            LOG.info("Job {} task delay {} ms to execute", icebergTaskInfo.getJobId(),
                    (timeToExecuteMs - System.currentTimeMillis()));
            return timeToExecuteMs;
        }
        return icebergTaskInfo.discover.noPendingSplit()
                ? timeToExecuteMs
                // has splits to consume, schedule this task immediately
                : System.currentTimeMillis();
    }

    public boolean hasSplits() {
        return splits != null && !splits.isEmpty();
    }

    private void getNewSplits() throws RoutineLoadPauseException {
        while (true) {
            Pair<IcebergSplitMeta, CombinedScanTask> p = discover.pollPendingSplit();
            if (p == null) {
                // no new splits
                return;
            }
            List<IcebergSplit> splits = new ArrayList<>(p.second.files().size());
            for (FileScanTask scanTask : p.second.files()) {
                IcebergSplit split = new IcebergSplit(p.first, scanTask);
                Boolean last = progress.add(split);
                // ignore split that is marked as done
                if (last == null || !last) {
                    splits.add(split);
                }
            }
            if (!splits.isEmpty()) {
                this.splits = splits;
                return;
            }
        }
    }

    @Override
    public boolean readyToExecute() throws RoutineLoadPauseException {
        IcebergRoutineLoadJob icebergRoutineLoadJob = (IcebergRoutineLoadJob) job;
        if (icebergRoutineLoadJob == null) {
            return false;
        }

        if (hasSplits()) {
            return true;
        }

        getNewSplits();
        return hasSplits();
    }

    @Override
    public boolean isProgressKeepUp(RoutineLoadProgress progress, Map<String, Long> consumeLagsRowNum) {
        return discover.noPendingSplit();
    }

    @Override
    public TRoutineLoadTask createRoutineLoadTask() throws UserException {
        IcebergRoutineLoadJob routineLoadJob = (IcebergRoutineLoadJob) job;

        // init tRoutineLoadTask and create plan fragment
        TRoutineLoadTask tRoutineLoadTask = new TRoutineLoadTask();
        TUniqueId queryId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        tRoutineLoadTask.setId(queryId);
        tRoutineLoadTask.setJob_id(routineLoadJob.getId());
        tRoutineLoadTask.setTxn_id(txnId);
        Database database = GlobalStateMgr.getCurrentState().getDb(routineLoadJob.getDbId());
        if (database == null) {
            throw new MetaNotFoundException("database " + routineLoadJob.getDbId() + " does not exist");
        }
        tRoutineLoadTask.setDb(database.getFullName());
        Table tbl = database.getTable(routineLoadJob.getTableId());
        if (tbl == null) {
            throw new MetaNotFoundException("table " + routineLoadJob.getTableId() + " does not exist");
        }
        tRoutineLoadTask.setTbl(tbl.getName());
        // label = job_name+job_id+task_id+txn_id
        String label =
                Joiner.on("-").join(routineLoadJob.getName(), routineLoadJob.getId(), DebugUtil.printId(id), txnId);
        tRoutineLoadTask.setLabel(label);
        tRoutineLoadTask.setAuth_code(routineLoadJob.getAuthCode());
        TIcebergLoadInfo tIcebergLoadInfo = new TIcebergLoadInfo();
        List<TIcebergSplit> tSplits = new ArrayList<>(splits.size());
        for (IcebergSplit split : splits) {
            tSplits.add(
                    new TIcebergSplit(
                            split.getSplitMeta().getStartSnapshotId(),
                            split.getSplitMeta().getEndSnapshotId(),
                            split.getSplitMeta().getEndSnapshotTimestamp(),
                            split.getSplitMeta().getTotalSplits(),
                            split.getOffset(),
                            split.getLength(),
                            split.getFileSize(),
                            split.getPath()
                    )
            );
        }
        tIcebergLoadInfo.setSplits(tSplits);
        tRoutineLoadTask.setIceberg_load_info(tIcebergLoadInfo);
        tRoutineLoadTask.setType(TLoadSourceType.ICEBERG);
        tRoutineLoadTask.setParams(plan(routineLoadJob, splits));
        tRoutineLoadTask.setMax_interval_s(routineLoadJob.getTaskConsumeSecond());
        tRoutineLoadTask.setMax_batch_rows(routineLoadJob.getMaxBatchRows());
        tRoutineLoadTask.setMax_batch_size(Config.max_routine_load_batch_size);
        tRoutineLoadTask.setFormat(splits.get(0).getFormatType());
        return tRoutineLoadTask;
    }

    @Override
    protected String getTaskDataSourceProperties() {
        if (consumePosition != null) {
            return "consumePosition: " + consumePosition;
        } else {
            return "";
        }
    }

    @Override
    String dataSourceType() {
        return "iceberg";
    }

    @Override
    public String toString() {
        return "Task id: " + getId() + ", split size: " + (splits != null ? splits.size() : 0);
    }

    private TExecPlanFragmentParams plan(IcebergRoutineLoadJob routineLoadJob, List<IcebergSplit> splits)
            throws UserException {
        TUniqueId loadId = new TUniqueId(id.getMostSignificantBits(), id.getLeastSignificantBits());
        // plan for each task, in case table has change(rollup or schema change)
        TExecPlanFragmentParams tExecPlanFragmentParams =
                routineLoadJob.plan(loadId, txnId, beId, routineLoadJob.getTimeoutSecond(), splits, label);
        if (tExecPlanFragmentParams.query_options.enable_profile) {
            StreamLoadTask streamLoadTask = GlobalStateMgr.getCurrentState().
                    getStreamLoadMgr().getTaskByLabel(label);
            setStreamLoadTask(streamLoadTask);
        }
        TPlanFragment tPlanFragment = tExecPlanFragmentParams.getFragment();
        tPlanFragment.getOutput_sink().getOlap_table_sink().setTxn_id(txnId);
        return tExecPlanFragmentParams;
    }
}
