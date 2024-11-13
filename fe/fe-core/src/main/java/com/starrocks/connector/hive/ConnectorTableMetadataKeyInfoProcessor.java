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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.connector.CacheUpdateProcessor;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class ConnectorTableMetadataKeyInfoProcessor extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(ConnectorTableMetadataKeyInfoProcessor.class);

    private final ExecutorService refreshHiveTableKeyInfoExecutor;
    private final ConnectorTableMetadataProcessor tableMetadataProcessor;

    public ConnectorTableMetadataKeyInfoProcessor(ConnectorTableMetadataProcessor tableMetadataProcessor) {
        super(ConnectorTableMetadataKeyInfoProcessor.class.getName(),
                Config.background_refresh_metadata_key_info_interval_millis);
        this.tableMetadataProcessor = tableMetadataProcessor;
        refreshHiveTableKeyInfoExecutor =
                Executors.newFixedThreadPool(Config.background_refresh_hive_table_key_info_cache_concurrency,
                        new ThreadFactoryBuilder().setNameFormat("background-refresh-hive-table-key-info-cache-%d")
                                .setDaemon(true).build());
    }

    @Override
    protected void runAfterCatalogReady() {
        if (Config.enable_background_refresh_connector_metadata) {
            refreshCatalogTable();
        }
    }

    private void refreshCatalogTable() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Map<String, CacheUpdateProcessor> cacheUpdateProcessors = tableMetadataProcessor.getCacheUpdateProcessors();
        List<String> catalogNames = Lists.newArrayList(cacheUpdateProcessors.keySet());
        for (String catalogName : catalogNames) {
            CacheUpdateProcessor updateProcessor = cacheUpdateProcessors.get(catalogName);
            if (updateProcessor == null) {
                LOG.error("Failed to get cacheUpdateProcessor by catalog {}.", catalogName);
                continue;
            }

            List<Future<?>> futures = Lists.newArrayList();
            for (DatabaseTableName cachedTableName : updateProcessor.getCachedTableNamesForPartitionKeysAndValues()) {
                futures.add(refreshHiveTableKeyInfoExecutor.submit(
                        new RunnableTask(metadataMgr, updateProcessor, catalogName, cachedTableName)));
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Throwable e) {
                }
            }
            LOG.info("refresh connector metadata key info {} finished", catalogName);
        }
    }

    private class RunnableTask implements Runnable {
        private MetadataMgr metadataMgr;
        private CacheUpdateProcessor updateProcessor;
        private String catalogName;
        private DatabaseTableName cachedTableName;

        RunnableTask(MetadataMgr metadataMgr, CacheUpdateProcessor updateProcessor, String catalogName,
                     DatabaseTableName cachedTableName) {
            this.metadataMgr = metadataMgr;
            this.updateProcessor = updateProcessor;
            this.catalogName = catalogName;
            this.cachedTableName = cachedTableName;
        }

        @Override
        public void run() {
            String dbName = cachedTableName.getDatabaseName();
            String tableName = cachedTableName.getTableName();
            Table table;
            try {
                table = metadataMgr.getTable(catalogName, dbName, tableName);
            } catch (Throwable e) {
                LOG.warn("can't get table of {}.{}.{}，msg: ", catalogName, dbName, tableName, e);
                return;
            }
            if (table == null) {
                LOG.warn("{}.{}.{} not exist", catalogName, dbName, tableName);
                return;
            }
            try {
                updateProcessor.refreshTableKeyInfoBackground(table);
            } catch (Throwable e) {
                if (Config.invalidate_cache_when_refresh_fail) {
                    updateProcessor.invalidateTable(dbName, tableName);
                }
                LOG.warn("refresh table partition key {}.{}.{} meta store info failed, msg : ", catalogName, dbName,
                        tableName, e);
                return;
            }
            LOG.info("refresh table partition key {}.{}.{} success", catalogName, dbName, tableName);
        }
    }
}
