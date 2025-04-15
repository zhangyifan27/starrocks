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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.TableName;
import com.starrocks.common.UserException;
import com.starrocks.monitor.unit.ByteSizeValue;
import com.starrocks.persist.AddDataCacheInfo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TCacheSelectMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class DataCacheSelectExecutor {
    private static final Logger LOG = LogManager.getLogger(DataCacheSelectExecutor.class);

    @SerializedName(value = "dataCacheRecords")
    private final Map<Long, Map<TableName, List<DataCacheRecord>>> dataCacheRecords = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleaner = Executors.newSingleThreadScheduledExecutor();

    public DataCacheSelectExecutor() {
        cleaner.scheduleAtFixedRate(this::cleanExpiredRecords, 30, 30, TimeUnit.SECONDS);
    }

    public DataCacheSelectMetrics cacheSelect(DataCacheSelectStatement statement,
                                                             ConnectContext connectContext) throws Exception {
        // backup original session variable
        SessionVariable sessionVariableBackup = connectContext.getSessionVariable();
        // clone an new session variable
        SessionVariable tmpSessionVariable = (SessionVariable) connectContext.getSessionVariable().clone();
        // overwrite catalog
        tmpSessionVariable.setCatalog(statement.getCatalog());
        // force enable datacache and populate
        tmpSessionVariable.setEnableScanDataCache(true);
        tmpSessionVariable.setEnablePopulateDataCache(true);
        tmpSessionVariable.setDataCachePopulateMode(DataCachePopulateMode.ALWAYS.modeName());
        // make sure all accessed data must be cached
        tmpSessionVariable.setEnableDataCacheAsyncPopulateMode(false);
        tmpSessionVariable.setEnableDataCacheIOAdaptor(false);
        tmpSessionVariable.setDataCacheEvictProbability(100);
        tmpSessionVariable.setDataCachePriority(statement.getPriority());
        tmpSessionVariable.setDatacacheTTLSeconds(statement.getTTLSeconds());
        tmpSessionVariable.setEnableCacheSelect(true);
        if (statement.isDelete()) {
            tmpSessionVariable.setCacheSelectMode(TCacheSelectMode.DELETE.getValue());
        } else if (statement.isDesc()) {
            tmpSessionVariable.setCacheSelectMode(TCacheSelectMode.DESC.getValue());
        }
        connectContext.setSessionVariable(tmpSessionVariable);

        InsertStmt insertStmt = statement.getInsertStmt();
        StmtExecutor stmtExecutor = StmtExecutor.newInternalExecutor(connectContext, insertStmt);
        // Register new StmtExecutor into current ConnectContext's StmtExecutor, so we can handle ctrl+c command
        // If DataCacheSelect is forward to leader, connectContext's Executor is null
        if (connectContext.getExecutor() != null) {
            connectContext.getExecutor().registerSubStmtExecutor(stmtExecutor);
        }
        stmtExecutor.addRunningQueryDetail(insertStmt);
        try {
            stmtExecutor.execute();
        } finally {
            stmtExecutor.addFinishedQueryDetail();
        }

        if (connectContext.getState().isError()) {
            // throw exception if StmtExecutor execute failed
            throw new UserException(connectContext.getState().getErrorMessage());
        }

        DataCacheSelectMetrics metrics = null;
        Coordinator coordinator = stmtExecutor.getCoordinator();
        Preconditions.checkNotNull(coordinator, "Coordinator can't be null");
        coordinator.join(connectContext.getSessionVariable().getQueryTimeoutS());
        if (coordinator.isDone()) {
            metrics = stmtExecutor.getCoordinator().getDataCacheSelectMetrics();
        }
        // set original session variable
        connectContext.setSessionVariable(sessionVariableBackup);

        Preconditions.checkNotNull(metrics, "Failed to retrieve cache select metrics");
        // Don't update datacache metrics after cache select, because of datacache instance still not unified.
        // Here update will display wrong metrics in show backends/compute nodes
        // update backend's datacache metrics after cache select
        // updateBackendDataCacheMetrics(metrics);
        return metrics;
    }

    // update BE's datacache metrics after cache select
    public void updateDataCacheMetrics(DataCacheSelectMetrics metrics, TableName tableName, String partition, long ttlSecond) {
        final SystemInfoService clusterInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (Map.Entry<Long, LoadDataCacheMetrics> metric : metrics.getBeMetrics().entrySet()) {
            ComputeNode computeNode = clusterInfoService.getBackendOrComputeNode(metric.getKey());
            if (computeNode == null) {
                continue;
            }
            computeNode.updateDataCacheMetrics(metric.getValue().getLastDataCacheMetrics());
            DataCacheRecord dataCacheRecord = new DataCacheRecord(tableName, partition,
                    metric.getValue().getWriteBytes().getBytes(), System.currentTimeMillis() + (ttlSecond * 1000));
            addDataCacheRecord(metric.getKey(), tableName, dataCacheRecord);
            GlobalStateMgr.getCurrentState().getEditLog().logDataCacheRecord(
                    new AddDataCacheInfo(metric.getKey(), tableName, dataCacheRecord));
        }
    }

    public void addDataCacheRecord(Long beid, TableName tableName, DataCacheRecord record) {
        dataCacheRecords.compute(beid, (k, v) -> {
            // new be cache info
            if (v == null) {
                v = new ConcurrentHashMap<>();
            }
            v.compute(tableName, (tk, tv) -> {
                        if (tv == null) {
                            tv = new CopyOnWriteArrayList<>();
                        }
                        tv.add(record);
                        return tv;
                    }
            );
            return v;
        });
    }

    public void removeRecord(TableName tableName, DataCacheRecord record) {
        dataCacheRecords.forEach((beid, beidMap) ->
                beidMap.computeIfPresent(tableName, (tk, records) -> {
                    records.remove(record);
                    return records.isEmpty() ? null : records;
                })
        );
    }

    public boolean removeBeRecord(Long beid) {
        if (dataCacheRecords.containsKey(beid)) {
            dataCacheRecords.remove(beid);
            return true;
        } else {
            return false;
        }
    }

    public List<List<String>> getPartitionsDataCacheSize(TableName tableName) {
        List<List<String>> rows = new ArrayList<>();
        Map<String, AtomicLong> partitionCounter = new HashMap<>();
        dataCacheRecords.forEach((beId, map) -> {
            List<DataCacheRecord> dataCacheRecords = map.get(tableName);
            if (dataCacheRecords != null) {
                for (DataCacheRecord dataCacheRecord : dataCacheRecords) {
                    String partition = dataCacheRecord.getPartition();
                    AtomicLong cacheSize = partitionCounter.getOrDefault(partition, new AtomicLong(0L));
                    cacheSize.addAndGet(dataCacheRecord.getCacheDataSize());
                    partitionCounter.put(partition, cacheSize);
                }
            }
        });
        for (Map.Entry<String, AtomicLong> entry : partitionCounter.entrySet()) {
            ByteSizeValue value = new ByteSizeValue(entry.getValue().get());
            rows.add(Lists.newArrayList(entry.getKey(), value.toString()));
        }
        return rows;
    }

    public List<List<String>> getTablesDataCacheSize(String catalogName, String db) {
        List<List<String>> rows = new ArrayList<>();
        Map<TableName, AtomicLong> tableCounter = new HashMap<>();
        dataCacheRecords.forEach((beId, map) -> {
            map.forEach((tableName, list) -> {
                if (tableName.getCatalog().equals(catalogName)
                        && tableName.getDb().equals(db)) {
                    AtomicLong cacheSize = tableCounter.getOrDefault(tableName, new AtomicLong(0L));
                    for (DataCacheRecord dataCacheRecord : list) {
                        cacheSize.addAndGet(dataCacheRecord.getCacheDataSize());
                    }
                    tableCounter.put(tableName, cacheSize);
                }
            });
        });
        for (Map.Entry<TableName, AtomicLong> entry : tableCounter.entrySet()) {
            ByteSizeValue value = new ByteSizeValue(entry.getValue().get());
            rows.add(Lists.newArrayList(entry.getKey().getTbl(), value.toString()));
        }
        return rows;
    }

    private void cleanExpiredRecords() {
        long now = System.currentTimeMillis();
        dataCacheRecords.forEach((beId, map) -> {
            map.forEach((tableName, list) -> {
                list.removeIf(dataCacheRecord -> dataCacheRecord.getTtlTime() < now);
            });
        });
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.DATA_CACHE_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        DataCacheSelectExecutor newData = reader.readJson(DataCacheSelectExecutor.class);
        dataCacheRecords.clear();
        dataCacheRecords.putAll(newData.dataCacheRecords);
    }
}
