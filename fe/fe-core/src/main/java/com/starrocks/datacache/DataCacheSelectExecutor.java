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
import com.starrocks.common.CloseableLock;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.thrift.TCacheSelectMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataCacheSelectExecutor {
    private static final Logger LOG = LogManager.getLogger(DataCacheSelectExecutor.class);

    public DataCacheSelectMetrics cacheSelect(DataCacheSelectStatement statement,
                                                             ConnectContext connectContext) throws Exception {
        DataCacheMetaManager cacheMetaManager = GlobalStateMgr.getCurrentState().getDataCacheMetaManager();
        if (!cacheMetaManager.isInitialized()) {
            throw new UserException("DataCacheMetaManager is not initialized yet, try later");
        }
        try (CloseableLock ignored = cacheMetaManager.lockPartition(statement.getTableName(), statement.getPartition())) {
            return doCacheSelect(statement, connectContext, cacheMetaManager);
        }
    }

    private DataCacheSelectMetrics doCacheSelect(DataCacheSelectStatement statement,
                                                ConnectContext connectContext,
                                                DataCacheMetaManager cacheMetaManager) throws Exception {
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
        if (RunMode.getCurrentRunMode() != RunMode.SHARED_DATA &&
                Config.cache_node_mode.equalsIgnoreCase("cn")) {
            tmpSessionVariable.setWarehouseName(WarehouseManager.DATACACHE_WAREHOUSE_NAME);
        }
        if (statement.isCacheDelete()) {
            tmpSessionVariable.setCacheSelectMode(TCacheSelectMode.DELETE.getValue());
        } else if (statement.isCacheDesc()) {
            tmpSessionVariable.setCacheSelectMode(TCacheSelectMode.DESC.getValue());
        }
        connectContext.setSessionVariable(tmpSessionVariable);
        connectContext.setDataCacheSelectStatement(statement);

        DataCacheSelectMetrics metrics = null;
        try {
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

            Coordinator coordinator = stmtExecutor.getCoordinator();
            Preconditions.checkNotNull(coordinator, "Coordinator can't be null");
            LOG.debug("DataCache select coordinator is done: {}", coordinator.isDone());
            metrics = stmtExecutor.getCoordinator().getDataCacheSelectMetrics();
            Preconditions.checkNotNull(metrics, "Failed to retrieve cache select metrics");
            if (coordinator.getExecStatus().ok()) {
                // Only update cache metadata when oteam datacache is enabled
                if (Config.enable_oteam_datacache && coordinator instanceof DefaultCoordinator) {
                    DefaultCoordinator defaultCoordinator = (DefaultCoordinator) coordinator;
                    cacheMetaManager.updateDataCacheMeta(statement, defaultCoordinator.getExecPlan(),
                            defaultCoordinator.getExecutionDAG(), metrics);
                }
            } else {
                // TODO: 考虑执行失败的情况，是否需要更新cache meta
            }

            // WARNING:
            // Don't update datacache metrics after cache select, because of datacache instance still not unified.
            // Here update will display wrong metrics in show backends/compute nodes
            // update backend's datacache metrics after cache select
            // updateBackendDataCacheMetrics(metrics);
            return metrics;
        } finally {
            LOG.debug("Restoring original session variable after data cache select");
            connectContext.setSessionVariable(sessionVariableBackup);
            connectContext.setDataCacheSelectStatement(null);
        }
    }
}
