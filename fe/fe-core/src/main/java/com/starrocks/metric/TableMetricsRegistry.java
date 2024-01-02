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


package com.starrocks.metric;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.starrocks.common.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public final class TableMetricsRegistry {
    private static final Logger LOG = LogManager.getLogger(TableMetricsRegistry.class);

    private final Cache<Long, TableMetricsEntity> idToTableMetrics;
    private static final TableMetricsRegistry INSTANCE = new TableMetricsRegistry();

    private TableMetricsRegistry() {
        idToTableMetrics = CacheBuilder.newBuilder()
                .maximumSize(Config.max_table_metrics_num)
                .removalListener((RemovalListener<Long, TableMetricsEntity>) removalNotification -> {
                    MetricVisitor visitor = new SimpleCoreMetricVisitor("starrocks_fe");
                    LOG.info("Removed table metrics from table [" + removalNotification.getKey() + "] " +
                            removalNotification.getValue().getMetrics().stream().map(m -> {
                                visitor.visit(m);
                                return visitor.build();
                            }).collect(Collectors.joining(", ")));
                }).build();
    }

    public static TableMetricsRegistry getInstance() {
        return INSTANCE;
    }

    public TableMetricsEntity getMetricsEntity(long tableId) {
        try {
            return idToTableMetrics.get(tableId, TableMetricsEntity::new);
        } catch (ExecutionException e) {
            LOG.error("Failed to obtain table [" + tableId + "] metrics!");
        }
        // Here suppose to be unreachable.
        return null;
    }
}
