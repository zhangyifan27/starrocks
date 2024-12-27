// Licensed to the Apache Software Foundation (ASF) under one

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.IcebergUtil;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
//import org.apache.iceberg.TableScanOptions;
import org.apache.iceberg.TableScanOptions;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

public class IcebergSplitDiscover {
    private static final Logger LOG = LogManager.getLogger(IcebergSplitDiscover.class);
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("iceberg-routine-load-split-scheduler").build());
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final boolean isPrimaryTable;
    private final String jobName;
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final AtomicLong scheduleSeqId = new AtomicLong();
    private final org.apache.iceberg.Table iceTbl; // actual iceberg table
    private final IcebergProgress icebergProgress;
    private final String icebergConsumePosition;
    private final Long readIcebergSnapshotsAfterTimestamp;
    private final Queue<Pair<IcebergSplitMeta, CombinedScanTask>> splitsQueue;
    private final Queue<Pair<IcebergSplitMeta, CombinedScanTask>> v2Queue;
    private Expression whereExpr;
    private boolean skipOverwrite = false;
    private boolean mergeOnReadOverwrite = true;
    private boolean replacePartitions = true;
    private int currentConcurrentTaskNum;
    private long splitSize;

    public IcebergSplitDiscover(boolean isPrimaryTable, String jobName, Table iceTbl,
                                IcebergProgress icebergProgress, String icebergConsumePosition,
                                Long readIcebergSnapshotsAfterTimestamp) {
        this.isPrimaryTable = isPrimaryTable;
        this.jobName = jobName;
        this.iceTbl = iceTbl;
        this.icebergProgress = icebergProgress;
        this.icebergConsumePosition = icebergConsumePosition;
        this.readIcebergSnapshotsAfterTimestamp = readIcebergSnapshotsAfterTimestamp;
        this.splitsQueue = new LinkedBlockingQueue<>();
        this.v2Queue = new LinkedBlockingQueue<>();
    }

    public void setWhereExpr(Expression whereExpr) {
        this.whereExpr = whereExpr;
    }

    public void setSkipOverwrite(boolean skipOverwrite) {
        this.skipOverwrite = skipOverwrite;
    }

    public void setMergeOnReadOverwrite(boolean mergeOnReadOverwrite) {
        this.mergeOnReadOverwrite = mergeOnReadOverwrite;
    }

    public void setReplacePartitions(boolean replacePartitions) {
        this.replacePartitions = replacePartitions;
    }

    public void setCurrentConcurrentTaskNum(int currentConcurrentTaskNum) {
        this.currentConcurrentTaskNum = currentConcurrentTaskNum;
    }

    public void setSplitSize(long splitSize) {
        this.splitSize = splitSize;
    }

    public int getCurrentConcurrentTaskNum() {
        return currentConcurrentTaskNum;
    }
    public String getJobName() {
        return jobName;
    }

    private void scheduleCheckAndAddSplits(long expectedScheduleSeqId) {
        lock.writeLock().lock();
        try {
            if (stopped.get() || expectedScheduleSeqId != scheduleSeqId.get()) {
                LOG.info("job {} abort checking splits", jobName);
                return;
            }
            if (paused.get()) {
                LOG.info("job {} is paused to check splits", jobName);
            } else {
                checkAndAddSplits();
            }
            if (stopped.get() || expectedScheduleSeqId != scheduleSeqId.get()) {
                LOG.info("job {} abort checking splits", jobName);
                return;
            }
            SCHEDULED_EXECUTOR_SERVICE.schedule(() -> scheduleCheckAndAddSplits(expectedScheduleSeqId),
                    Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
        } catch (Exception e) {
            LOG.error("job " + jobName + " failed to checkAndAddSplits", e);
            SCHEDULED_EXECUTOR_SERVICE.schedule(() -> scheduleCheckAndAddSplits(expectedScheduleSeqId),
                    Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean noPendingSplit() {
        return splitsQueue.isEmpty() && v2Queue.isEmpty();
    }

    public int pendingSplitsSize() {
        return splitsQueue.size() + v2Queue.size();
    }

    public Pair<IcebergSplitMeta, CombinedScanTask> pollPendingSplit() {
        addToQueueFromV2Queue();
        return splitsQueue.poll();
    }

    private CloseableIterable<CombinedScanTask> planTasks(TableScan scan) {
        // the difference between this planTasks() and scan.planTasks() is
        // 1. the sorting of scanFiles, so that the result of the planTasks() is idempotent
        // 2. file is not split, otherwise changing to splitSize may cause idempotent
        return planTasks(scan, scan.planFiles());
    }

    private CloseableIterable<CombinedScanTask> planTasks(TableScan scan, CloseableIterable<FileScanTask> scanFiles) {
        List<FileScanTask> sortedScanFiles = Lists.newArrayList(scanFiles);
        sortedScanFiles.sort(Comparator.comparing(o -> o.file().path().toString()));
        CloseableIterable<FileScanTask> sortedFileScanTasks =
                CloseableIterable.combine(sortedScanFiles, scanFiles);
        LOG.debug("IcebergRoutineLoadJob[{}] watch sortedFileScanTasks:[{}]", jobName, sortedFileScanTasks);
        return TableScanUtil.planTasks(
                sortedFileScanTasks, scan.targetSplitSize(), scan.splitLookback(), scan.splitOpenFileCost());
    }

    private IcebergSplitMeta addToQueue(TableScan scan, long startSnapshotId, long endSnapshotId,
                                        long endSnapshotTimestamp) {
        BaseTable baseTable = (BaseTable) scan.table();
        if (IcebergUtil.isMorTable(baseTable)) {
            CloseableIterable<FileScanTask> scanFiles = scan.planFiles();
            int totalSplits = 0;
            for (FileScanTask ignored : scanFiles) {
                totalSplits++;
            }
            LOG.debug("IcebergRoutineLoadJob[{}] handle mor table, addToQueue files.size:{}", jobName, totalSplits);
            if (totalSplits == 0) {
                LOG.error("IcebergRoutineLoadJob[{}] planFiles return empty! scan:{}", jobName, scan);
            }
            IcebergSplitMeta splitMeta =
                    new IcebergSplitMeta(startSnapshotId, endSnapshotId, endSnapshotTimestamp, totalSplits);
            addToV2Queue(scan, scanFiles, splitMeta);
            addToQueueFromV2Queue();
            return splitMeta;
        } else {
            try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(scan)) {
                int totalSplits = 0;
                for (CombinedScanTask combinedScanTask : tasksIterable) {
                    totalSplits += combinedScanTask.files().size();
                }
                LOG.debug("IcebergRoutineLoadJob[{}] added to queue size: {}", jobName, totalSplits);
                if (totalSplits == 0) {
                    LOG.error("IcebergRoutineLoadJob[{}] planTasks return empty! scan:{}", jobName, scan);
                }
                IcebergSplitMeta splitMeta =
                        new IcebergSplitMeta(startSnapshotId, endSnapshotId, endSnapshotTimestamp, totalSplits);
                tasksIterable.forEach(combinedScanTask -> splitsQueue.offer(Pair.create(splitMeta, combinedScanTask)));
                return splitMeta;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close table scan: " + scan, e);
            }
        }
    }

    private void addToQueue(TableScan scan, IcebergSplitMeta splitMeta) {
        BaseTable baseTable = (BaseTable) scan.table();
        if (IcebergUtil.isCowTable(baseTable)) {
            addToV2Queue(scan, scan.planFiles(), splitMeta);
            addToQueueFromV2Queue();
        } else {
            try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(scan)) {
                tasksIterable.forEach(combinedScanTask -> splitsQueue.offer(Pair.create(splitMeta, combinedScanTask)));
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close table scan: " + scan, e);
            }
        }
    }

    private void addToV2Queue(TableScan scan, CloseableIterable<FileScanTask> scanFiles, IcebergSplitMeta splitMeta) {
        // should handle one sequence number at one time and tasks with smaller sequence number first
        Map<Long, List<FileScanTask>> dataFilesGroupedBySequenceNumber =
                new TreeMap<>(StreamSupport.stream(scanFiles.spliterator(), false)
                        // some splits are already done when recovery
                        .filter(fileScanTask -> {
                            try {
                                return !icebergProgress.isDone(new IcebergSplit(splitMeta, fileScanTask));
                            } catch (RoutineLoadPauseException e) {
                                return false;
                            }
                        })
                        .collect(Collectors.groupingBy(fileScanTask -> fileScanTask.file().fileSequenceNumber())));
        for (List<FileScanTask> fileScanTasks : dataFilesGroupedBySequenceNumber.values()) {
            if (fileScanTasks.isEmpty()) {
                continue;
            }
            CloseableIterable<CombinedScanTask> tasksIterable = planTasks(scan,
                    CloseableIterable.withNoopClose(fileScanTasks));
            tasksIterable.forEach(combinedScanTask -> v2Queue.offer(Pair.create(splitMeta, combinedScanTask)));
        }
    }

    private synchronized void addToQueueFromV2Queue() {
        if (!splitsQueue.isEmpty() || icebergProgress.hasUnfinishedSplit()) {
            return;
        }
        Pair<IcebergSplitMeta, CombinedScanTask> pair = v2Queue.poll();
        if (pair == null) {
            return;
        }
        splitsQueue.offer(pair);
    }

    @Nullable
    private Triple<TableScan, Long, Long> appendsBetween(TableScan scan, Long startSnapshotId, Long endSnapshotId) {
        // FIXME: snapshot created by "insert overwrite" is not supported here
        // one can judge "insert overwrite" using the below condition against every snapshot between startSnapshot and endSnapshot
        // snapshot.operation().equals(DataOperations.OVERWRITE) && snapshot.removedDataFiles(iceTbl.io()).iterator().hasNext();
        List<Long> snapshotIds = SnapshotUtil.currentAncestorIds(scan.table());
        // 根据endSnapshotId得到祖先快照列表，如果列表中找不到startSnapshotId，则拉取当前快照的全部增量
        int startIndex = snapshotIds.lastIndexOf(startSnapshotId);
        if (startIndex > 0) {
            snapshotIds = snapshotIds.subList(0, startIndex + 1);
        } else {
            LOG.warn("IcebergRoutineLoadJob[{}] The startSnapshotId[{}] could not be found in any of the ancestral " +
                            "snapshots of the current Iceberg table[{}]. It might have been deleted. The incremental " +
                            "data between the current snapshot and its oldest ancestor will be retrieved.",
                    jobName, startSnapshotId, endSnapshotId);
        }

        // current IcebergTable not found the endSnapshotId
        int endIndex = snapshotIds.indexOf(endSnapshotId);
        if (endIndex < 0) {
            LOG.warn("IcebergRoutineLoadJob[{}] current iceberg table not found the endSnapshotId: {}", jobName, endSnapshotId);
            return null;
        }

        if (!snapshotIds.isEmpty()) {
            startSnapshotId = snapshotIds.get(snapshotIds.size() - 1);
            endSnapshotId = snapshotIds.get(endIndex);
            scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
            return Triple.of(scan, startSnapshotId, endSnapshotId);
        }

        LOG.warn("IcebergRoutineLoadJob[{}] unknown error. startSnapshotId:{}, endSnapshotId: {}",
                jobName, startSnapshotId, endSnapshotId);
        return null;
    }

    private void firstScan(Snapshot snapshot) {
        if (IcebergCreateRoutineLoadStmtConfig.isFromEarliest(icebergConsumePosition)) {
            long snapshotId = snapshot.snapshotId();
            if (readIcebergSnapshotsAfterTimestamp == null) {
                LOG.info("IcebergRoutineLoadJob[{}] readIcebergSnapshotsAfterTimestamp is not set, " +
                        "first scan whit snapshotId[{}]", jobName, snapshotId);
                TableScan scan = newTableScan();
                scan = scan.useSnapshot(snapshotId);
                icebergProgress.setLastSplitMeta(addToQueue(scan, -1, snapshotId, snapshot.timestampMillis()));
            } else {
                // If the user-set readIcebergSnapshotsAfterTimestamp is greater than the current snapshot timestamp,
                // then there are no splits to add at the moment.
                if (readIcebergSnapshotsAfterTimestamp >= snapshot.timestampMillis()) {
                    LOG.warn("Iceberg routine load job [job name {}] started with [{}], but the current " +
                                    "snapshot's timestamp [{}] is earlier than readIcebergSnapshotsAfterTimestamp [{}]",
                            jobName, icebergConsumePosition, snapshot.timestampMillis(),
                            readIcebergSnapshotsAfterTimestamp);
                    return;
                }
                scanBetween(snapshot, readIcebergSnapshotsAfterTimestamp);
            }
        } else {
            scanBetween(snapshot, null);
        }
    }

    private void scanBetween(Snapshot endSnapshot, Long readIcebergSnapshotsAfterTimestamp) {
        long endSnapshotId = endSnapshot.snapshotId();
        Snapshot startSnapshot = findStartSnapshot(readIcebergSnapshotsAfterTimestamp);
        if (startSnapshot == null) {
            LOG.info("IcebergRoutineLoadJob[{}] cannot find the startSnapshot by readIcebergSnapshotsAfterTimestamp, " +
                    "then useSnapshot:{}", jobName, endSnapshotId);
            // If the current snapshot has no ancestor snapshots, it is essentially a full scan.
            TableScan scan = newTableScan();
            scan = scan.useSnapshot(endSnapshotId);
            icebergProgress.setLastSplitMeta(addToQueue(scan, -1, endSnapshotId, endSnapshot.timestampMillis()));
            return;
        }
        LOG.debug("IcebergRoutineLoadJob[{}] find the startSnapshot[{}] by " +
                "readIcebergSnapshotsAfterTimestamp, and the endSnapshot[{}]", jobName, startSnapshot, endSnapshot);
        TableScan scan = newTableScan();
        Triple<TableScan, Long, Long> triple = appendsBetween(scan, startSnapshot.snapshotId(), endSnapshotId);
        if (triple != null) {
            icebergProgress.setLastSplitMeta(
                    addToQueue(triple.getLeft(), triple.getMiddle(), triple.getRight(),
                            endSnapshot.timestampMillis()));
        }
    }

    private Snapshot findStartSnapshot(Long readIcebergSnapshotsAfterTimestamp) {
        // sorted from most recently to earliest, current snapshot is included
        Iterator<Snapshot> snapshotsAncestors = SnapshotUtil.currentAncestors(iceTbl).iterator();
        // current snapshot
        snapshotsAncestors.next();
        Snapshot startSnapshot = null;
        while (snapshotsAncestors.hasNext()) {
            Snapshot s = snapshotsAncestors.next();
            if (readIcebergSnapshotsAfterTimestamp == null) {
                return s;
            }
            startSnapshot = s;
            if (s.timestampMillis() <= readIcebergSnapshotsAfterTimestamp) {
                break;
            }
        }
        return startSnapshot;
    }

    private TableScan newTableScan() {
        TableScan scan = iceTbl.newScan()
                .includeColumnStats()
                .option(TableProperties.SPLIT_SIZE, "" + splitSize)
                .option(TableScanOptions.SKIP_OVERWRITE, Boolean.toString(skipOverwrite))
                .option(TableProperties.INCLUDE_SNAPSHOT_MERGE_ON_READ_OVERWRITE,
                        Boolean.toString(mergeOnReadOverwrite))
                .option(TableProperties.INCLUDE_SNAPSHOT_REPLACE_PARTITIONS, Boolean.toString(replacePartitions));
        if (whereExpr != null) {
            scan = scan.filter(whereExpr);
        }
        return scan;
    }

    private void checkAndAddSplits() {
        if (pendingSplitsSize() > currentConcurrentTaskNum * 50) {
            LOG.warn("iceberg routine load job [job name {}] too many tasks[{} > {}] are running, delay check", jobName,
                    pendingSplitsSize(), currentConcurrentTaskNum * 50);
            return;
        }
        IcebergMetadata.refreshTable(iceTbl);
        Snapshot snapshot = iceTbl.currentSnapshot();
        if (snapshot == null) {
            // no data
            return;
        }
        IcebergSplitMeta lastSplitMeta = icebergProgress.getLastSplitMeta();
        // first time
        if (lastSplitMeta == null) {
            firstScan(snapshot);
            return;
        }
        long snapshotId = snapshot.snapshotId();
        if (lastSplitMeta.getEndSnapshotId() == snapshotId) {
            // no new data
            return;
        }
        TableScan scan = newTableScan();
        Triple<TableScan, Long, Long> triple = appendsBetween(scan, lastSplitMeta.getEndSnapshotId(), snapshotId);
        if (triple != null) {
            icebergProgress.setLastSplitMeta(addToQueue(triple.getLeft(), triple.getMiddle(), triple.getRight(),
                    snapshot.timestampMillis()));
        }
    }

    private void addSplitsFromRecovery(List<IcebergSplitMeta> splitMetas, Snapshot snapshot) {
        if (snapshot == null) {
            // no data
            LOG.warn("IcebergRoutineLoadJob[{}] snapshot is null, the iceberg tablet has no data.", jobName);
            return;
        }

        // first time
        if (splitMetas == null) {
            LOG.info("IcebergRoutineLoadJob[{}] splitMetas is null, begin the first scan with snapshot[{}]", jobName, snapshot);
            firstScan(snapshot);
            return;
        }
        IcebergSplitMeta lastSplitMeta = null;
        for (IcebergSplitMeta splitMeta : splitMetas) {
            if (lastSplitMeta != null && splitMeta.getStartSnapshotId() != lastSplitMeta.getEndSnapshotId()) {
                // usually, the splitMetas are continuous, like[{-1,3},{3,6}]. if not, the gap should be filled
                try {
                    TableScan scan = newTableScan();
                    Triple<TableScan, Long, Long> triple = appendsBetween(scan, lastSplitMeta.getEndSnapshotId(),
                            splitMeta.getStartSnapshotId());
                    if (triple != null) {
                        addToQueue(triple.getLeft(), triple.getMiddle(), triple.getRight(),
                                scan.table().snapshot(triple.getRight()).timestampMillis());
                    }
                } catch (IllegalArgumentException e) {
                    // range [lastSplitMeta.getEndSnapshotId(), splitMeta.getStartSnapshotId()] is illegal
                    LOG.warn("IcebergRoutineLoadJob[{}] ignore this range gap: {}", jobName, e.getMessage(), e);
                }
            }
            lastSplitMeta = splitMeta;
            if (splitMeta.isAllDone()) {
                continue;
            }
            if (splitMeta.getStartSnapshotId() == -1) {
                TableScan scan = newTableScan();
                scan = scan.useSnapshot(splitMeta.getEndSnapshotId());
                addToQueue(scan, splitMeta);
            } else {
                try {
                    TableScan scan = newTableScan();
                    Triple<TableScan, Long, Long> triple =
                            appendsBetween(scan, splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId());
                    if (triple != null && triple.getMiddle() == splitMeta.getStartSnapshotId() &&
                            triple.getRight() == splitMeta.getEndSnapshotId()) {
                        addToQueue(triple.getLeft(), splitMeta);
                    } else {
                        // It might throw an IllegalArgumentException
                        scan = scan.appendsBetween(splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId());
                        addToQueue(scan, splitMeta);
                    }
                } catch (IllegalArgumentException e) {
                    // range [splitMeta.getStartSnapshotId(), splitMeta.getEndSnapshotId()] is illegal
                    LOG.warn("IcebergRoutineLoadJob[{}] ignore this range: {}" + e.getMessage(), jobName, splitMeta, e);
                    icebergProgress.removeSplitMeta(splitMeta);
                }
            }
            LOG.info("job {} recover splitMeta end: {}", jobName, splitMeta);
        }
    }

    private void recover(long expectedScheduleSeqId) {
        iceTbl.refresh();
        Snapshot snapshot = iceTbl.currentSnapshot();
        lock.writeLock().lock();
        try {
            LOG.info("job {} recover begin", jobName);
            List<IcebergSplitMeta> splitMetas = icebergProgress.recoverLastSnapshots(iceTbl);
            v2Queue.clear();
            splitsQueue.clear();
            addSplitsFromRecovery(splitMetas, snapshot);
            LOG.info("job {} recover end", jobName);
        } catch (Exception e) {
            LOG.error("job {} recover failed: ", jobName, e);
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
        SCHEDULED_EXECUTOR_SERVICE.schedule(() -> scheduleCheckAndAddSplits(expectedScheduleSeqId),
                Config.routine_load_iceberg_split_check_interval_second, TimeUnit.SECONDS);
    }

    public void start() {
        stopped.set(false);
        // use new id, so that the old scheduled check can aborted automatically to avoid duplicate check
        long nextId = scheduleSeqId.incrementAndGet();
        // The scheduleCheckAndAddSplits depends on obtaining the correct lastSplitMeta，
        // and needs to wait for the recovery to complete before scheduling.
        SCHEDULED_EXECUTOR_SERVICE.schedule(() -> recover(nextId), 0, TimeUnit.SECONDS);
    }

    public void stop() {
        stopped.set(true);
        v2Queue.clear();
        splitsQueue.clear();
    }

    public void setPaused(boolean paused) {
        this.paused.set(paused);
    }
}
