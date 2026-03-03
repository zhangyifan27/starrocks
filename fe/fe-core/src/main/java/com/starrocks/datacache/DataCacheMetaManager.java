// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.datacache;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Striped;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.Table;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.scheduler.SubmitResult;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DataCacheMetaManager extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(DataCacheMetaManager.class);

    // Logical database and table names for data cache metadata.
    public static final String CACHE_DB_NAME = "datacache_meta_db";
    public static final String FILE_CACHE_META = "datacache_file_meta";
    public static final String CACHE_STATUS_ACTIVE = "ACTIVE";

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final DataCacheMetaCache metaCache = new DataCacheMetaCache();

    // Partition-level mutexes to serialize cache select/delete on the same partition.
    // Use fixed-size striped locks to cap memory usage.
    private final Striped<Lock> partitionLocks = Striped.lock(1024);

    private final Queue<Long> candidateGcPartitions = new ConcurrentLinkedQueue<>(); // store partitionUid
    private final Set<Long> candidateGcTracker = new CopyOnWriteArraySet<>();

    private final long expiredFilesThreshold;
    private final long expiredBytesThreshold;

    private volatile boolean initialized;

    private final DataCacheMetaStore metaStore = new DataCacheMetaStore();
    private final DataCacheFileMetaStore fileMetaStore =
            new DataCacheFileMetaStore(CACHE_DB_NAME, FILE_CACHE_META);

    // ---------------------------------------- Construction & lifecycle ----------------------------------------

    public DataCacheMetaManager() {
        this(60_000L, 1L, 1L);
    }

    public DataCacheMetaManager(long intervalMs, long expiredFilesThreshold, long expiredBytesThreshold) {
        super("data-cache-meta-manager", intervalMs);
        this.expiredFilesThreshold = expiredFilesThreshold;
        this.expiredBytesThreshold = expiredBytesThreshold;
    }

    @Override
    protected void runAfterCatalogReady() {
        // To make UT pass, some UT will create database and table
        trySleep(Config.datacache_manager_sleep_time_sec * 1000);

        initializeIfNeeded();
        cleanExpiredPartitions();
        runGc();
    }

    private void trySleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    public void initializeIfNeeded() {
        if (initialized) {
            return;
        }
        lock.writeLock().lock();
        try {
            if (initialized) {
                return;
            }
            LOG.info("Initializing DataCacheMetaManager metadata structures for database {}", CACHE_DB_NAME);
            fileMetaStore.ensureTable();
            metaStore.initialize(metaCache);
            initialized = true;
        } catch (UserException e) {
            LOG.warn("Failed to initialize DataCacheMetaManager metadata structures", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---------------------------------------- ID computation & factory helpers ----------------------------------------

    private static long hash64(String value) {
        return Hashing.murmur3_128().hashString(Strings.nullToEmpty(value), StandardCharsets.UTF_8).asLong();
    }

    private long computeTableId(TableName table) {
        return hash64(table.toString());
    }

    private long computePartitionId(TableName table, String partitionKey) {
        return hash64(table.toString() + partitionKey.toString());
    }

    /**
     * Generate a stable signature for a hash ring configuration so we can detect topology/algorithm changes.
     *
     * @param hashAlgorithm   backend selector hash algorithm
     * @param filePathOnly    whether file-path-only hashing is enabled
     * @param virtualNodeNum  virtual node count used by consistent hash
     * @param workerIds       collection of worker ids participating in the ring
     * @return hex string signature
     */
    public String buildHashRingSignature(String hashAlgorithm, boolean filePathOnly,
                                         int virtualNodeNum, Collection<Long> workerIds) {
        List<Long> sortedIds = new ArrayList<>(workerIds);
        Collections.sort(sortedIds);
        String payload = String.format("%s|%s|%s|%s", hashAlgorithm, filePathOnly, virtualNodeNum,
                sortedIds.toString());
        return Hashing.murmur3_128().hashString(payload, StandardCharsets.UTF_8).toString();
    }

    // Acquire a partition-scoped lock for the given table/partition. Callers should use try-with-resources.
    public CloseableLock lockPartition(TableName tableName, String partitionKey) {
        long partitionId = computePartitionId(tableName, partitionKey);
        Lock partitionLock = partitionLocks.get(partitionId);
        return CloseableLock.lock(partitionLock);
    }

    // Compute a file id deterministically from its path.
    private long computeFileId(String filePath) {
        return hash64(filePath);
    }

    // Factory method to create a FileEntry from logical identifiers instead of numeric ids.
    // - fileId is computed internally from filePath
    // - tableId/partitionUid are derived from tableName and partitionKey
    public DataCacheFileMeta buildFileMeta(TableName tableName,
                                           String partitionKey,
                                           long backendId,
                                           String filePath,
                                           long fileSizeBytes,
                                           long offset,
                                           long length,
                                           long partitionVersion,
                                           long modificationTime,
                                           String fileType,
                                           boolean isRelativePath,
                                           String hashRingSignature) {
        long tableId = computeTableId(tableName);
        long partitionUid = computePartitionId(tableName, partitionKey);
        long fileId = computeFileId(filePath);
        return new DataCacheFileMeta(partitionUid, fileId, backendId, filePath, fileSizeBytes,
                offset, length, tableId, partitionVersion, modificationTime, fileType, isRelativePath, hashRingSignature);
    }

    // ---------------------------------------- Data cache meta APIs ----------------------------------------
    /**
     * Update table/partition/file cache metadata and cache data size. (assume execution success)
     */
    public void updateDataCacheMeta(DataCacheSelectStatement stmt, ExecPlan execPlan,
                                    ExecutionDAG executionDAG, DataCacheSelectMetrics metrics) {
        updateDataCacheFileMeta(stmt, execPlan, executionDAG);
        updateDataCachePartitionMeta(stmt, execPlan, metrics);
    }

    public void updateDataCachePartitionMeta(DataCacheSelectStatement stmt, ExecPlan execPlan,
                                            DataCacheSelectMetrics metrics) {
        TableName tableName = stmt.getTableName();
        String partition = stmt.getPartition();
        if (stmt.isCacheSelect()) {
            ScanNode targetScanNode = locateScanNode(execPlan, tableName);
            if (targetScanNode == null) {
                LOG.warn("Data cache metadata collection could not find matching scan node for table {}",
                        tableName);
                return;
            }

            List<RemoteFileInfo> remoteFiles = targetScanNode.getRemoteFiles();
            if (remoteFiles == null || remoteFiles.isEmpty()) {
                LOG.warn("Data cache metadata collection found no remote files for table {}",
                        tableName);
                return;
            }

            String partitionPath = remoteFiles.get(0).getFullPath();
            long partitionVersion = stmt.getPartitionVersion();
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime ttlExpireAt = now.plusSeconds(stmt.getTTLSeconds());
            String partitionField = stmt.getPartitionField();
            String partitionFieldType = stmt.getPartitionFieldType().toString();
            String partitionUnit = stmt.getPartitionUnit();
            String partitionFormat = stmt.getPartitionFieldFormat();

            long cacheDataSize = 0L;
            if (metrics != null && metrics.getBeMetrics() != null) {
                for (Map.Entry<Long, LoadDataCacheMetrics> metric : metrics.getBeMetrics().entrySet()) {
                    cacheDataSize += metric.getValue().getWriteBytes().getBytes();
                    cacheDataSize += metric.getValue().getReadBytes().getBytes();
                }
            }

            upsertPartitionMeta(tableName, partition, partitionVersion, CACHE_STATUS_ACTIVE, now, ttlExpireAt,
                    partitionField, partitionFieldType, cacheDataSize, partitionPath,
                    partitionUnit, partitionFormat, stmt.getHashRingSignature());
            upsertTableMeta(tableName);
        } else if (stmt.isCacheDelete()) {
            if (stmt.getDeleteMode() == CacheDeleteMode.NORMAL) {
                removePartitionMeta(tableName, partition);
            } else { // GC mode
                resetExpiredStatisticsForPartitionMeta(tableName, partition);
            }
            // Note: gc模式的cache delete 不要更新partition meta的cache data size,
        }
    }

    private void cleanExpiredPartitions() {
        List<Long> expired = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();

        lock.readLock().lock();
        try {
            for (DataCachePartitionMeta meta : metaCache.getPartitionMetaByUid().values()) {
                LocalDateTime ttl = meta.getTtlExpireAt();
                if (ttl != null && !ttl.isAfter(now)) {
                    LOG.info("Cleaning expired data cache partition {}.", meta.getPartitionKey());
                    expired.add(meta.getPartitionUid());
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        for (long partitionUid : expired) {
            fileMetaStore.deleteForPartition(partitionUid);
            removePartitionMeta(partitionUid);
        }
    }

    /**
     * Collect file meta and partition version for a cache select.
     */
    public void updateDataCacheFileMeta(DataCacheSelectStatement stmt, ExecPlan execPlan,
                                        ExecutionDAG executionDAG) {
        if (stmt.isCacheDesc()) {
            return;
        }

        if (stmt.isCacheDelete()) {
            removeCacheFileMetaAfterCacheDelete(stmt.getTableName(), stmt.getPartition(), stmt.getDeleteMode());
            return;
        }

        TableName tableName = stmt.getTableName();
        ScanNode targetScanNode = locateScanNode(execPlan, tableName);
        if (targetScanNode == null) {
            LOG.warn("Data cache metadata collection could not find matching scan node for table {}",
                    tableName);
            return;
        }

        List<RemoteFileInfo> remoteFiles = targetScanNode.getRemoteFiles();
        if (remoteFiles == null || remoteFiles.isEmpty()) {
            LOG.warn("Data cache metadata collection found no remote files for table {}",
                    tableName);
            return;
        }

        String partition = stmt.getPartition();
        String partitionName = stmt.getPartitionName();
        long remotePartitionVersion = computePartitionVersion(targetScanNode.getTable(), tableName, partitionName,
                remoteFiles);
        stmt.setPartitionVersion(remotePartitionVersion);
        long currentPartitionVersion = getPartitionVersion(tableName, partition);

        ExecutionFragment executionFragment = executionDAG.getFragment(targetScanNode.getFragmentId());
        if (executionFragment == null) {
            LOG.warn("Data cache metadata collection could not find execution fragment for scan node {}",
                    targetScanNode.getId());
            return;
        }

        // Compute current hash ring signature for debug/decision making.
        ConnectContext ctx = ConnectContext.get();
        SessionVariable sv = ctx.getSessionVariable();
        String hashAlgorithm = sv.getHdfsBackendSelectorHashAlgorithm();
        boolean filePathOnlyFlag = sv.getHdfsScanRangeHashFilePathOnly();
        int virtualNodeNum = sv.getConsistentHashVirtualNodeNum();
        Set<Long> workerIds = executionFragment.getInstances().stream()
                .map(FragmentInstance::getWorkerId).collect(Collectors.toSet());
        String ringSignature = buildHashRingSignature(hashAlgorithm, filePathOnlyFlag, virtualNodeNum, workerIds);
        stmt.setHashRingSignature(ringSignature);
        LOG.info("Data cache hash ring signature for table {} partition {}: algo={}, filePathOnly={}, vnodes={}, "
                        + "workers={}, signature={}",
                tableName, partition, hashAlgorithm, filePathOnlyFlag, virtualNodeNum, workerIds, ringSignature);

        String currentRingSignature = getPartitionHashRingSignature(tableName, partition);

        if (remotePartitionVersion == currentPartitionVersion && ringSignature.equals(currentRingSignature)) {
            LOG.info("Data cache metadata collection skipped for table {} partition {} as version and hash ring" +
                            "signature unchanged",
                    tableName, partition);
            return;
        }
        collectFileCacheMeta(executionFragment, targetScanNode,
                tableName, partition, remotePartitionVersion, ringSignature);
    }

    public List<List<String>> getPartitionsDataCacheSize(TableName tableName) {
        List<List<String>> rows = new ArrayList<>();
        long tableId = computeTableId(tableName);
        LOG.info("Data Cache - show data cache for table: TableName {} id {}", tableName, tableId);

        lock.readLock().lock();
        try {
            List<String> allPartitionMetaLogs = metaCache.getPartitionMetaByUid().values().stream()
                    .map(meta -> String.format(
                            "tableId=%s,partitionUid=%s,partitionKey=%s,version=%s,status=%s,created=%s,"
                                    + "lastRefresh=%s,expiredFiles=%s,expiredBytes=%s,ttl=%s,field=%s,fieldType=%s,"
                                    + "cacheSize=%s,path=%s,unit=%s,format=%s",
                            meta.getTableId(), meta.getPartitionUid(), meta.getPartitionKey(), meta.getVersion(),
                            meta.getCacheStatus(), meta.getCreatedTime(), meta.getLastRefreshTime(),
                            meta.getExpiredFiles(), meta.getExpiredBytes(), meta.getTtlExpireAt(),
                            meta.getPartitionField(), meta.getPartitionFieldType(), meta.getCacheDataSize(),
                            meta.getPartitionAbsPrefixPath(), meta.getPartitionUnit(), meta.getPartitionFieldFormat()))
                    .collect(Collectors.toList());
            LOG.info("Data Cache: all partition meta: {}", allPartitionMetaLogs);
            for (DataCachePartitionMeta meta : metaCache.getPartitionMetaByUid().values()) {
                if (meta.getTableId() != tableId) {
                    continue;
                }
                ByteSizeValue value = new ByteSizeValue(meta.getCacheDataSize());
                String ttl = meta.getTtlExpireAt() == null ? "" : meta.getTtlExpireAt().toString();
                rows.add(Lists.newArrayList(
                        String.valueOf(meta.getPartitionUid()),
                        String.valueOf(meta.getTableId()),
                        meta.getPartitionKey(),
                        String.valueOf(meta.getVersion()),
                        meta.getCacheStatus(),
                        meta.getCreatedTime() == null ? "" : meta.getCreatedTime().toString(),
                        meta.getLastRefreshTime() == null ? "" : meta.getLastRefreshTime().toString(),
                        String.valueOf(meta.getExpiredFiles()),
                        String.valueOf(meta.getExpiredBytes()),
                        ttl,
                        meta.getPartitionField(),
                        meta.getPartitionFieldType(),
                        value.toString(),
                        meta.getPartitionAbsPrefixPath(),
                        meta.getPartitionUnit(),
                        meta.getPartitionFieldFormat(),
                        meta.getHashRingSignature()));
            }
        } finally {
            lock.readLock().unlock();
        }
        LOG.info("Data cache partition sizes for {}: {}", tableName, rows);
        return rows;
    }

    public List<List<String>> getTablesDataCacheSize(String catalogName, String db) {
        LOG.info("Data Cache: getTableDataCacheSize({}, {})", catalogName, db);
        Map<Long, DataCacheTableMeta> all = metaCache.getTableMetaById();
        List<String> logs = all.entrySet().stream()
                .map(e -> String.format("tableId=%s,catalog=%s,db=%s,table=%s,type=%s,cacheSize=%s",
                        e.getKey(), e.getValue().getCatalogName(), e.getValue().getDbName(),
                        e.getValue().getTableName(), e.getValue().getTableType(), e.getValue().getCacheSize()))
                .collect(Collectors.toList());
        LOG.info("Data Cache: all table meta: {}", logs);

        List<List<String>> rows = new ArrayList<>();
        lock.readLock().lock();
        try {
            for (DataCacheTableMeta meta : metaCache.getTableMetaById().values()) {
                // Filter by catalog name (required)
                if (!meta.getCatalogName().equals(catalogName)) {
                    continue;
                }
                // Filter by db name (optional - if null, show all databases)
                if (db != null && !meta.getDbName().equals(db)) {
                    continue;
                }
                refreshTableCacheSizeLocked(meta.getTableId());
                ByteSizeValue cacheSizeValue = new ByteSizeValue(meta.getCacheSize());
                rows.add(Lists.newArrayList(
                        meta.getCatalogName(),
                        meta.getDbName(),
                        meta.getTableName(),
                        String.valueOf(meta.getTableId()),
                        meta.getTableType(),
                        meta.getCreatedTime() == null ? "" : meta.getCreatedTime().toString(),
                        meta.getUpdatedTime() == null ? "" : meta.getUpdatedTime().toString(),
                        cacheSizeValue.toString(),
                        meta.getScheduleTaskName()
                    ));
            }
        } finally {
            lock.readLock().unlock();
        }
        LOG.info("Data cache table sizes for catalog {} db {}: {}", catalogName, db, rows);
        return rows;
    }

    private long computePartitionVersion(Table table, TableName tableName, String partitionKey,
                                         List<RemoteFileInfo> remoteFiles) {
        if (table != null && table.isIcebergTable() && !Strings.isNullOrEmpty(partitionKey)) {
            Long icebergPartitionVersion = fetchIcebergPartitionVersion(table, tableName, partitionKey);
            if (icebergPartitionVersion != null) {
                return icebergPartitionVersion;
            }
            LOG.error("fail to get version for iceberg table {} partition {}", tableName, partitionKey);
            return 0;
        }

        return computeFileBasedPartitionVersion(remoteFiles);
    }

    private Long fetchIcebergPartitionVersion(Table table, TableName tableName, String partitionKey) {
        if (tableName == null || Strings.isNullOrEmpty(tableName.getCatalog())) {
            return null;
        }
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        if (metadataMgr == null) {
            return null;
        }
        try {
            List<PartitionInfo> partitionInfos = metadataMgr.getPartitions(tableName.getCatalog(), table,
                    Collections.singletonList(partitionKey));
            if (partitionInfos == null || partitionInfos.isEmpty()) {
                LOG.warn("No partition metadata found for iceberg table {} partition {}", tableName, partitionKey);
                return null;
            }
            PartitionInfo partitionInfo = partitionInfos.get(0);
            if (partitionInfo == null) {
                LOG.warn("Partition {} missing metadata entry for iceberg table {}", partitionKey, tableName);
                return null;
            }
            long micros = TimeUnit.MICROSECONDS.convert(partitionInfo.getModifiedTime(),
                    partitionInfo.getModifiedTimeUnit());
            return micros;
        } catch (Exception e) {
            LOG.warn("Failed to fetch iceberg partition metadata for table {} partition {}", tableName, partitionKey, e);
            return null;
        }
    }

    private long computeFileBasedPartitionVersion(List<RemoteFileInfo> remoteFiles) {
        long maxMtime = 0L;
        if (remoteFiles == null || remoteFiles.isEmpty()) {
            return maxMtime;
        }

        for (RemoteFileInfo remoteFile : remoteFiles) {
            if (remoteFile == null) {
                continue;
            }
            List<RemoteFileDesc> files = remoteFile.getFiles();
            if (files == null || files.isEmpty()) {
                continue;
            }
            for (RemoteFileDesc desc : files) {
                if (desc == null) {
                    continue;
                }

                long length = desc.getLength();
                if (length <= 0) {
                    continue;
                }
                maxMtime = Math.max(maxMtime, desc.getModificationTime());
            }
        }

        return maxMtime;
    }

    private ScanNode locateScanNode(ExecPlan execPlan, TableName tableName) {
        for (ScanNode scanNode : execPlan.getScanNodes()) {
            Table t = scanNode.getTable();
            if (t != null && t.getName().equals(tableName.getTbl())) {
                return scanNode;
            }
        }
        return null;
    }

    private void collectFileCacheMeta(ExecutionFragment executionFragment,
                                      ScanNode scanNode,
                                      TableName tableName,
                                      String partition,
                                      long partitionVersion,
                                      String ringSignature) {
        int scanNodeId = scanNode.getId().asInt();
        for (FragmentInstance instance : executionFragment.getInstances()) {
            long workerId = instance.getWorkerId();

            Map<Integer, List<TScanRangeParams>> node2ScanRanges = instance.getNode2ScanRanges();
            if (node2ScanRanges == null) {
                continue;
            }
            collectRanges(node2ScanRanges.get(scanNodeId), workerId,
                    tableName, partition, partitionVersion, ringSignature);
        }
    }

    // iceberg full path: table location(table base) + relative path(/data/<partition>/<file name>)
    // hive 直接存relative path，让be自己拼 full path
    private void collectRanges(List<TScanRangeParams> ranges,
                               long workerId,
                               TableName tableName,
                               String partition,
                               long partitionVersion,
                               String ringSignature) {
        if (ranges == null) {
            return;
        }
        for (TScanRangeParams params : ranges) {
            if (params == null || !params.isSetScan_range()) {
                continue;
            }
            TScanRange scanRange = params.getScan_range();
            if (scanRange == null || !scanRange.isSetHdfs_scan_range()) {
                continue;
            }
            THdfsScanRange hdfsRange = scanRange.getHdfs_scan_range();
            String path = hdfsRange.isSetFull_path() ? hdfsRange.getFull_path() : hdfsRange.getRelative_path();

            if (Strings.isNullOrEmpty(path)) {
                continue;
            }

            long fileLength = hdfsRange.getFile_length();
            long offset = hdfsRange.isSetOffset() ? hdfsRange.getOffset() : 0;
            long length = hdfsRange.isSetLength() ? hdfsRange.getLength() : fileLength;
            long modificationTime = hdfsRange.getModification_time();
            DataCacheFileMeta entry = buildFileMeta(tableName, partition, workerId, path, fileLength,
                    offset, length, partitionVersion, modificationTime,
                    hdfsRange.getFile_format().toString(), hdfsRange.isSetRelative_path(), ringSignature);
            upsertFile(entry);
        }
    }

    // ---------------------------------------- Legacy load hook (no-op) ----------------------------------------

    public void loadCacheRecords(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        // DataCacheRecord persistence is deprecated; consume and ignore legacy blocks if present.
        reader.readJson(Object.class);
    }

    // ---------------------------------------- Table metadata APIs ----------------------------------------

    public void upsertTableMeta(TableName tableName) {
        upsertTableMeta(tableName, null);
    }

    public void upsertTableMeta(TableName tableName, String scheduleTaskName) {
        long tableId = computeTableId(tableName);
        String catalog = tableName.getCatalog();
        String db = tableName.getDb();
        String table = tableName.getTbl();
        lock.writeLock().lock();
        try {
            DataCacheTableMeta row = metaCache.getTableMeta(tableId);
            if (row == null) {
                row = new DataCacheTableMeta(tableId, catalog, db, table, "",
                        LocalDateTime.now(), LocalDateTime.now(), 0L, scheduleTaskName);
                metaCache.putTableMeta(row);
            } else {
                row.setUpdatedTime(LocalDateTime.now());
                row.setScheduleTaskName(scheduleTaskName);
            }
            metaStore.persistTableMeta(row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<DataCacheTableMeta> getTableMeta(TableName tableName) {
        long tableId = computeTableId(tableName);
        lock.readLock().lock();
        try {
            return Optional.ofNullable(metaCache.getTableMeta(tableId)).map(DataCacheTableMeta::copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean existsTable(TableName tableName) {
        long tableId = computeTableId(tableName);
        lock.readLock().lock();
        try {
            return metaCache.containsTable(tableId);
        } finally {
            lock.readLock().unlock();
        }
    }

    // Recompute and update table-level cache size by summing partition cache sizes.
    // Caller must hold write lock. Returns the new cache size.
    private long refreshTableCacheSizeLocked(long tableId) {
        DataCacheTableMeta tableMeta = metaCache.getTableMeta(tableId);
        if (tableMeta == null) {
            return 0L;
        }
        long total = 0L;
        for (DataCachePartitionMeta partitionMeta : metaCache.getPartitionMetaByUid().values()) {
            if (partitionMeta.getTableId() == tableId) {
                total += partitionMeta.getCacheDataSize();
            }
        }
        tableMeta.setCacheSize(total);
        return total;
    }

    // ---------------------------------------- Partition metadata APIs ----------------------------------------

    // used when cache delete gc mode
    public void resetExpiredStatisticsForPartitionMeta(TableName tableName, String partitionKey) {
        long partitionId = computePartitionId(tableName, partitionKey);
        lock.writeLock().lock();
        try {
            DataCachePartitionMeta row = metaCache.getPartitionMeta(partitionId);
            if (row == null) {
                throw new RuntimeException("upsertPartitionMeta for cache delete gc: cache partition meta not found");
            } else {
                row.setExpiredFiles(0);
                row.setExpiredBytes(0);
            }
            metaCache.putPartitionMeta(row);
            metaStore.persistPartitionMeta(row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // used when cache select
    public void upsertPartitionMeta(TableName tableName, String partitionKey,
                                    long version, String cacheStatus,
                                    LocalDateTime lastRefreshTime, LocalDateTime ttlExpireAt,
                                    String partitionField, String partitionFieldType, long cacheDataSize,
                                    String partitionPath, String partitionUnit, String partitionFormat,
                                    String hashRingSignature) {
        long tableId = computeTableId(tableName);
        long partitionId = computePartitionId(tableName, partitionKey);
        lock.writeLock().lock();
        try {
            DataCachePartitionMeta row = metaCache.getPartitionMeta(partitionId);
            if (row == null) {
                row = new DataCachePartitionMeta(tableId, partitionKey, partitionId, version, cacheStatus,
                        LocalDateTime.now(), lastRefreshTime, 0, 0, ttlExpireAt,
                        partitionField, partitionFieldType, cacheDataSize, partitionPath, partitionUnit,
                        partitionFormat, hashRingSignature);
            } else {
                if (row.getVersion() != version || !row.getHashRingSignature().equals(hashRingSignature)) {
                    // update expired files/bytes
                    long[] stats = fileMetaStore.queryExpiredStats(tableId, partitionId, version, hashRingSignature);
                    long expiredFiles = stats[0];
                    long expiredBytes = stats[1];
                    row.setExpiredFiles(expiredFiles);
                    row.setExpiredBytes(expiredBytes);
                    if (expiredBytes > expiredFilesThreshold || expiredFiles > expiredBytesThreshold) {
                        LOG.info("upsertPartitionMeta: addCandidateGcPartition [{}.{}] expiredFiles[{}], expiredBytes[{}]",
                                tableName, partitionKey, expiredFiles, expiredBytes);
                        addCandidateGcPartition(partitionId);
                    }
                }
                row.setPartitionUid(partitionId);
                row.setVersion(version);
                row.setCacheStatus(cacheStatus);
                row.setLastRefreshTime(lastRefreshTime);
                row.setTtlExpireAt(ttlExpireAt);
                row.setPartitionField(partitionField);
                row.setPartitionFieldType(partitionFieldType);
                row.setCacheDataSize(cacheDataSize);
                row.setPartitionAbsPrefixPath(partitionPath);
                row.setPartitionUnit(partitionUnit);
                row.setPartitionFieldFormat(partitionFormat);
                row.setHashRingSignature(hashRingSignature);
            }
            metaCache.putPartitionMeta(row);
            metaStore.persistPartitionMeta(row);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removePartitionMeta(long partitionId) {
        lock.writeLock().lock();
        try {
            metaCache.removePartitionMeta(partitionId);
            metaStore.deletePartitionMeta(partitionId);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removePartitionMeta(TableName tableName, String partitionKey) {
        long partitionId = computePartitionId(tableName, partitionKey);
        removePartitionMeta(partitionId);
    }

    public Optional<DataCachePartitionMeta> getPartitionMeta(TableName tableName, String partitionKey) {
        long partitionId = computePartitionId(tableName, partitionKey);
        lock.readLock().lock();
        try {
            return Optional.ofNullable(metaCache.getPartitionMeta(partitionId)).map(DataCachePartitionMeta::copy);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean existsPartition(TableName tableName, String partitionKey) {
        Long partitionId = computePartitionId(tableName, partitionKey);
        lock.readLock().lock();
        try {
            return metaCache.containsPartition(partitionId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getPartitionVersion(TableName tableName, String partitionKey) {
        Long partitionId = computePartitionId(tableName, partitionKey);
        lock.readLock().lock();
        try {
            DataCachePartitionMeta row = metaCache.getPartitionMeta(partitionId);
            return row == null ? 0L : row.getVersion();
        } finally {
            lock.readLock().unlock();
        }
    }

    public String getPartitionHashRingSignature(TableName tableName, String partitionKey) {
        Long partitionId = computePartitionId(tableName, partitionKey);
        lock.readLock().lock();
        try {
            DataCachePartitionMeta row = metaCache.getPartitionMeta(partitionId);
            return row == null ? "" : row.getHashRingSignature();
        } finally {
            lock.readLock().unlock();
        }
    }

    // -------------------------- Replay helpers --------------------------
    public void replayUpsertTableMeta(DataCacheTableMeta meta) {
        lock.writeLock().lock();
        try {
            metaCache.putTableMeta(meta);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayUpsertPartitionMeta(DataCachePartitionMeta meta) {
        lock.writeLock().lock();
        try {
            metaCache.putPartitionMeta(meta);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replayDeletePartitionMeta(long partitionUid) {
        lock.writeLock().lock();
        try {
            DataCachePartitionMeta removed = metaCache.getPartitionMeta(partitionUid);
            metaCache.removePartitionMeta(partitionUid);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---------------------------------------- File metadata APIs ----------------------------------------

    public List<DataCacheRemoteFileDesc> getCacheDeleteRemoteFileDescs(TableName tableName, String partitionKey,
                                                                       CacheDeleteMode mode) {
        long partitionId = computePartitionId(tableName, partitionKey);
        DataCachePartitionMeta prow;
        lock.readLock().lock();
        try {
            prow = metaCache.getPartitionMeta(partitionId);
        } finally {
            lock.readLock().unlock();
        }
        if (prow == null) {
            return ImmutableList.of();
        }

        long currentVersion = prow.getVersion();
        String currentHashRingSignature = prow.getHashRingSignature();

        List<DataCacheRemoteFileDesc> result = new ArrayList<>();
        List<DataCacheFileMeta> entries;
        if (mode == CacheDeleteMode.NORMAL) {
            entries = fileMetaStore.queryFileMeta(partitionId);
        } else if (mode == CacheDeleteMode.GC) {
            entries = fileMetaStore.queryExpiredFileMeta(partitionId, currentVersion, currentHashRingSignature);
        } else {
            throw new RuntimeException("getCacheDeleteRemoteFileDescs: wrong cache delete mode: " + mode);
        }

        if (entries.isEmpty()) {
            return ImmutableList.of();
        }

        for (DataCacheFileMeta entry : entries) {
            if (entry.getBackendId() < 0) {
                throw new RuntimeException("getCacheDeleteRemoteFileDescs: bad backend id: " + entry.getBackendId());
            }

            result.add(new DataCacheRemoteFileDesc(
                    entry.getPartitionUid(),
                    entry.getFileId(),
                    entry.getBackendId(),
                    entry.getFilePath(),
                    entry.getModificationTime(),
                    entry.getFileType(),
                    entry.isRelativePath(),
                    entry.getOffset(),
                    entry.getLength(),
                    entry.getFileSizeBytes()));
        }
        return ImmutableList.copyOf(result);
    }

    public boolean upsertFile(DataCacheFileMeta entry) {
        fileMetaStore.insertFileMeta(entry);
        return true;
    }

    public void removeCacheFileMetaAfterCacheDelete(TableName tableName, String partitionKey, CacheDeleteMode mode) {
        long partitionId = computePartitionId(tableName, partitionKey);
        if (mode == CacheDeleteMode.NORMAL) {
            fileMetaStore.deleteForPartition(partitionId);
        } else {
            DataCachePartitionMeta p = metaCache.getPartitionMeta(partitionId);
            if (p == null) {
                throw new IllegalStateException(
                        String.format("Partition meta not found for %s:%s", tableName, partitionKey));
            }
            long currentVersion = p.getVersion();
            String currentHashRingSignature = p.getHashRingSignature();
            fileMetaStore.deleteExpiredFileMeta(partitionId, currentVersion, currentHashRingSignature);
        }
    }

    // ---------------------------------------- GC & cache delete task ----------------------------------------
    public boolean addCandidateGcPartition(long partitionId) {
        if (!candidateGcTracker.add(partitionId)) {
            return false;
        }
        boolean offerResult = candidateGcPartitions.offer(partitionId);
        if (!offerResult) {
            candidateGcTracker.remove(partitionId);
        }
        return offerResult;
    }

    private void runGc() {
        LOG.debug("Running DataCacheMetaManager GC cycle");
        List<Long> processed = new ArrayList<>();
        Long uid;
        while ((uid = candidateGcPartitions.poll()) != null) {
            candidateGcTracker.remove(uid);
            processed.add(uid);
        }
        LOG.debug("DataCacheMetaManager GC cycle processing {} partitions", processed.size());
        if (processed.isEmpty()) {
            return;
        }
        List<CacheDeleteTaskRequest> deleteTasks = new ArrayList<>();
        lock.writeLock().lock();
        try {
            for (Long partitionUid : processed) {
                DataCachePartitionMeta meta = metaCache.getPartitionMeta(partitionUid);
                if (meta == null) {
                    continue;
                }
                if (meta.getExpiredFiles() < expiredFilesThreshold
                        && meta.getExpiredBytes() < expiredBytesThreshold) {
                    continue;
                }
                DataCacheTableMeta tableMeta = metaCache.getTableMeta(meta.getTableId());
                if (tableMeta != null) {
                    LOG.info("Scheduling cache delete task for partition {} on {}.{}.{} with {}"
                            + "expired files and {} expired bytes",
                            meta.getPartitionKey(),
                            tableMeta.getCatalogName(),
                            tableMeta.getDbName(),
                            tableMeta.getTableName(),
                            meta.getExpiredFiles(),
                            meta.getExpiredBytes());

                    deleteTasks.add(new CacheDeleteTaskRequest(
                            tableMeta.getCatalogName(),
                            tableMeta.getDbName(),
                            tableMeta.getTableName(),
                            meta.getPartitionKey(),
                            CacheDeleteMode.GC));
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        if (!deleteTasks.isEmpty()) {
            LOG.info("Submitting {} cache delete tasks for GC", deleteTasks.size());
            submitCacheDeleteTasks(deleteTasks);
        }
    }

    // ---------------------------------------- Internal helpers & types ----------------------------------------

    private void submitCacheDeleteTasks(List<CacheDeleteTaskRequest> tasks) {
        for (CacheDeleteTaskRequest task : tasks) {
            submitCacheDeleteTask(task);
        }
    }

    public void submitCacheDeleteTask(CacheDeleteTaskRequest task, ConnectContext context) {
        try {
            String sql = buildCacheDeleteSubmitSql(task);
            LOG.info("Submitting cache delete task sql: {}", sql);
            SubmitTaskStmt parsedStmt = (SubmitTaskStmt) SqlParser.parse(sql,
                    context.getSessionVariable()).get(0);
            StatementPlanner.plan(parsedStmt, context);
            ShowResultSet resultSet = context.getGlobalStateMgr().getTaskManager().handleSubmitTaskStmt(parsedStmt);
            if (!isSubmitSucceeded(resultSet)) {
                LOG.warn("Submitting cache delete task failed for partition {} on {}.{}.{} with status {}",
                        task.partition, task.catalogName, task.dbName, task.tableName,
                        extractStatus(resultSet));
                throw new RuntimeException("Submitting cache delete task failed for partition " + task.partition);
            }
            LOG.info("Cache delete task submitted for partition {} on {}.{}.{}",
                    task.partition, task.catalogName, task.dbName, task.tableName);
            LOG.info("Cache delete task submit result: {}", resultSet.getResultRows());
        } catch (Exception e) {
            LOG.warn("Failed to submit cache delete task for partition {} on {}.{}.{}",
                    task.partition, task.catalogName, task.dbName, task.tableName, e);
        }
    }

    public void submitCacheDeleteTask(CacheDeleteTaskRequest task) {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = ConnectContext.build();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(UserIdentity.ROOT);
        context.setCurrentCatalog(task.catalogName);
        context.setDatabase(task.dbName);
        context.setCurrentWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        context.setExecutionId(UUIDUtil.genTUniqueId());
        context.setQueryId(UUIDUtil.genUUID());
        context.setThreadLocalInfo();
        try {
            submitCacheDeleteTask(task, context);
        } finally {
            ConnectContext.remove();
            if (previousContext != null) {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private boolean isSubmitSucceeded(ShowResultSet resultSet) {
        return SubmitResult.SubmitStatus.SUBMITTED.toString().equalsIgnoreCase(extractStatus(resultSet));
    }

    private String extractStatus(ShowResultSet resultSet) {
        if (resultSet == null || resultSet.getResultRows() == null || resultSet.getResultRows().isEmpty()) {
            return "UNKNOWN";
        }
        List<String> row = resultSet.getResultRows().get(0);
        return row.size() < 2 ? "UNKNOWN" : row.get(1);
    }

    public String buildCacheDeleteSubmitSql(CacheDeleteTaskRequest task) {
        String partition = (task.partition == null || task.partition.isEmpty())
                ? task.tableName
                : task.partition;
        String queryId = Optional.ofNullable(ConnectContext.get())
                .map(ConnectContext::getQueryId)
                .map(UUID::toString)
                .orElse(UUIDUtil.genUUID().toString());
        String taskName = "DataCacheDelete-" + partition + "-" + queryId;

        StringBuilder builder = new StringBuilder("SUBMIT TASK ")
                .append(quoteIdentifier(taskName))
                .append(" AS CACHE DELETE * FROM ");
        builder.append(quoteIdentifier(task.catalogName))
                .append(".")
                .append(quoteIdentifier(task.dbName))
                .append(".")
                .append(quoteIdentifier(task.tableName));
        builder.append(" PROPERTIES(\"partition\" = \"")
                .append(escapePropertyValue(partition))
                .append("\", ")
                .append("\"cache_delete_mode\" = \"")
                .append(escapePropertyValue(task.deleteMode.toString()))
                .append("\")");
        return builder.toString();
    }

    private String quoteIdentifier(String identifier) {
        return ParseUtil.backquote(identifier);
    }

    private String escapePropertyValue(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public static final class CacheDeleteTaskRequest {
        private final String catalogName;
        private final String dbName;
        private final String tableName;
        private final String partition;
        private final CacheDeleteMode deleteMode;

        public CacheDeleteTaskRequest(String catalogName, String dbName, String tableName,
                                       String partition, CacheDeleteMode deleteMode) {
            this.catalogName = catalogName;
            this.dbName = dbName;
            this.tableName = tableName;
            this.partition = partition;
            this.deleteMode = deleteMode;
        }
    }

    public enum CacheDeleteMode {
        NORMAL,
        GC
    }

}
