// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.server;

import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.system.Backend;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class SystemStatistics extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(SystemStatistics.class);

    private long totalTabletNum = 0L;
    private long totalCapacityB = 0L;
    private long diskAvailableCapacityB = 0L;
    private int routineLoadTotalConcurrency = 0;

    public SystemStatistics() {
        super("system statistics", Config.system_statistics_checker_interval_seconds * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        if (!globalStateMgr.isLeader()) {
            return;
        }
        updateTotalTabletNum(globalStateMgr);
        updateStorageUsage(globalStateMgr);
        updateRoutineLoadTotalConcurrency(globalStateMgr);
    }

    public long getTotalTabletNum() {
        return totalTabletNum;
    }

    public double getStorageUsedPct() {
        return (totalCapacityB - diskAvailableCapacityB) / (double) (totalCapacityB <= 0 ? 1 : totalCapacityB);
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public long getDiskAvailableCapacityB() {
        return diskAvailableCapacityB;
    }

    public int getRoutineLoadTotalConcurrency() {
        return routineLoadTotalConcurrency;
    }

    protected void updateTotalTabletNum(GlobalStateMgr globalStateMgr) {
        try {
            List<Long> dbIds = globalStateMgr.getLocalMetastore().getDbIds();
            if (dbIds == null || dbIds.isEmpty()) {
                // empty
                totalTabletNum = 0;
                return;
            }
            int newTotalTabletNum = 0;
            for (Long dbId : dbIds) {
                if (dbId == 0) {
                    // skip information_schema database
                    continue;
                }
                Database db = globalStateMgr.getDb(dbId);
                if (db == null) {
                    continue;
                }

                for (Table table : db.getTables()) {
                    if (!table.isNativeTableOrMaterializedView()) {
                        continue;
                    }
                    OlapTable olapTable = (OlapTable) table;

                    for (Partition partition : olapTable.getAllPartitions()) {
                        for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                            for (MaterializedIndex materializedIndex : physicalPartition.getMaterializedIndices(
                                    MaterializedIndex.IndexExtState.VISIBLE)) {
                                List<Tablet> tablets = materializedIndex.getTablets();
                                if (tablets != null) {
                                    newTotalTabletNum += tablets.size();
                                } // end for tablets
                            } // end for indices
                        }
                    } // end for partitions
                } // end for tables
            } // end for dbs
            totalTabletNum = newTotalTabletNum;
        } catch (Throwable e) {
            LOG.error("update total tablet num error", e);
        }
    }

    protected void updateStorageUsage(GlobalStateMgr globalStateMgr) {
        try {
            List<Backend> clusterBackends = globalStateMgr.getNodeMgr().getClusterInfo().getBackends();
            long newTotalCapacityB = 0L;
            long newDiskAvailableCapacityB = 0L;
            for (Backend backend : clusterBackends) {
                // Here we do not check if backend is alive,
                // We suppose the dead backends will back to alive later.
                if (backend.isDecommissioned()) {
                    // Data on decommissioned backend will move to other backends,
                    // So we need to minus size of those data.
                    newDiskAvailableCapacityB -= backend.getDataUsedCapacityB();
                } else {
                    newDiskAvailableCapacityB += backend.getAvailableCapacityB();
                    newTotalCapacityB += backend.getTotalCapacityB();
                }
            }
            diskAvailableCapacityB = newDiskAvailableCapacityB;
            totalCapacityB = newTotalCapacityB;
        } catch (Throwable e) {
            LOG.error("update storage usage error", e);
        }
    }

    protected void updateRoutineLoadTotalConcurrency(GlobalStateMgr globalStateMgr) {
        try {
            List<RoutineLoadJob> routineLoadJobs = globalStateMgr.getRoutineLoadMgr().getRoutineLoadJobByState(
                    ImmutableSet.of(RoutineLoadJob.JobState.RUNNING, RoutineLoadJob.JobState.PAUSED,
                            RoutineLoadJob.JobState.NEED_SCHEDULE));
            int newRoutineLoadTotalConcurrency = 0;
            for (RoutineLoadJob routineLoadJob : routineLoadJobs) {
                newRoutineLoadTotalConcurrency += routineLoadJob.calculateCurrentConcurrentTaskNum();
            }
            routineLoadTotalConcurrency = newRoutineLoadTotalConcurrency;
        } catch (Throwable e) {
            LOG.error("update routine load total concurrency error", e);
        }
    }

    public void checkTabletExceedLimit() throws DdlException {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        if (totalTabletNum >= Config.max_tablet_count_limit) {
            String errorMessage = "Reached the limit of tablet in cluster, please try to delete some tables or " +
                    "increase the 'max_tablet_count_limit' configuration in the frontend. Current limit: " +
                    Config.max_tablet_count_limit + ", current tablet count: " + totalTabletNum;
            LOG.warn(errorMessage);
            throw new DdlException(errorMessage);
        }
    }

    public void checkStorageUsageExceedLimit() throws DdlException {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        if (getStorageUsedPct() >= Config.max_storage_usage) {
            String errorMessage = "Reached the maximum storage usage in cluster, please try to drop some data or " +
                    "add backends, Current limit: " + Config.max_storage_usage + ", current storage usage: " +
                    getStorageUsedPct();
            LOG.warn(errorMessage);
            throw new DdlException(errorMessage);
        }
    }

    public void checkRoutineLoadTotalConcurrencyExceedLimit() throws DdlException {
        if (GlobalStateMgr.isCheckpointThread()) {
            return;
        }
        if (routineLoadTotalConcurrency >= Config.max_concurrent_routine_load_tasks) {
            String errorMessage = "Reached the limit of tablet in cluster, please try to add backends or " +
                    "increace the 'max_concurrent_routine_load_tasks' configuration in the frontend. " + "Current limit: " +
                    Config.max_concurrent_routine_load_tasks + ", current routine load task concurrency: " +
                    routineLoadTotalConcurrency;
            LOG.warn(errorMessage);
            throw new DdlException(errorMessage);
        }
    }
}
