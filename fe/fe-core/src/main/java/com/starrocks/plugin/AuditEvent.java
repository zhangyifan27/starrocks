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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/plugin/AuditEvent.java

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

package com.starrocks.plugin;

import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.starrocks.qe.QueryState;
import com.starrocks.server.WarehouseManager;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * AuditEvent contains all information about audit log info.
 * It should be created by AuditEventBuilder. For example:
 *
 *      AuditEvent event = new AuditEventBuilder()
 *          .setEventType(AFTER_QUERY)
 *          .setClientIp(xxx)
 *          ...
 *          .build();
 */
public class AuditEvent {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public enum EventType {
        CONNECTION,
        DISCONNECTION,
        BEFORE_QUERY,
        AFTER_QUERY
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface AuditField {
        String value() default "";

        boolean ignore_zero() default false;
    }

    public EventType type;

    // all fields which is about to be audit should be annotated by "@AuditField"
    // make them all "public" so that easy to visit.
    @AuditField(value = "Timestamp")
    public long timestamp = -1;
    @AuditField(value = "Client")
    public String clientIp = "";
    // The original login user
    @AuditField(value = "User")
    public String user = "";
    // The user used to authorize
    // `User` could be different from `AuthorizedUser` if impersonated
    @AuditField(value = "AuthorizedUser")
    public String authorizedUser = "";
    @AuditField(value = "ResourceGroup")
    public String resourceGroup = "";
    @AuditField(value = "Catalog")
    public String catalog = "";
    @AuditField(value = "Db")
    public String db = "";
    @AuditField(value = "Table")
    public String table = "";
    @AuditField(value = "State")
    public String state = "";
    @AuditField(value = "ErrorCode")
    public String errorCode = "";
    @AuditField(value = "Time")
    public long queryTime = -1;
    @AuditField(value = "ScanBytes")
    public long scanBytes = -1;
    @AuditField(value = "ScanRows")
    public long scanRows = -1;
    @AuditField(value = "ReturnRows")
    public long returnRows = -1;
    @AuditField(value = "CpuCostNs", ignore_zero = true)
    public long cpuCostNs = -1;
    @AuditField(value = "MemCostBytes", ignore_zero = true)
    public long memCostBytes = -1;
    @AuditField(value = "FeedbackMemCostBytes", ignore_zero = true)
    public double feedbackMemCostBytes = -1;
    @AuditField(value = "StmtId")
    public long stmtId = -1;
    @AuditField(value = "QueryId")
    public String queryId = "";
    @AuditField(value = "IsQuery")
    public boolean isQuery = false;
    @AuditField(value = "DataSource")
    public String dataSource = "";
    @AuditField(value = "RequestType")
    public String requestType = "";
    @AuditField(value = "feIp")
    public String feIp = "";
    @AuditField(value = "Stmt")
    public String stmt = "";
    @AuditField(value = "Digest")
    public String digest = "";
    @AuditField(value = "PlanCpuCost")
    public double planCpuCosts = -1;
    @AuditField(value = "PlanMemCost")
    public double planMemCosts = -1;
    @AuditField(value = "PendingTimeMs")
    public long pendingTimeMs = -1;
    @AuditField(value = "Slots")
    public int numSlots = -1;
    @AuditField(value = "BigQueryLogCPUSecondThreshold")
    public long bigQueryLogCPUSecondThreshold = -1;
    @AuditField(value = "BigQueryLogScanBytesThreshold")
    public long bigQueryLogScanBytesThreshold = -1;
    @AuditField(value = "BigQueryLogScanRowsThreshold")
    public long bigQueryLogScanRowsThreshold = -1;
    @AuditField(value = "SpilledBytes", ignore_zero = true)
    public long spilledBytes = -1;
    @AuditField(value = "Warehouse")
    public String warehouse = WarehouseManager.DEFAULT_WAREHOUSE_NAME;

    // Materialized View usage info
    @AuditField(value = "CandidateMVs", ignore_zero = true)
    public String candidateMvs;
    @AuditField(value = "HitMvs", ignore_zero = true)
    public String hitMVs;

    @AuditField(value = "IsForwardToLeader")
    public boolean isForwardToLeader = false;

    @AuditField(value = "Exception")
    public String exception = "";
    @AuditField(value = "ErrorMessage")
    public String errorMessage = "";
    @AuditField(value = "stmtType")
    public String stmtType = "";

    @AuditField(value = "supersqlTraceId")
    public String supersqlTraceId = "";
    @AuditField(value = "Statistics")
    public String statistics = "";
    @AuditField(value = "TableUseOmsStatistics")
    public String tableUseOmsStatistics = "";
    @AuditField(value = "profileSize")
    public long profileSize = -1;

    public static class TableStatisticsInfo {
        public String engine = "starrocks";
        public String type = "statistics";
        public List<TableStatistics> tableStatistics;
    }

    public static class TableStatistics {
        @SerializedName(value = "db")
        public String db;
        @SerializedName(value = "table")
        public String table;
        @SerializedName(value = "column_list")
        public Set<String> columnList = new HashSet<>();
        @SerializedName(value = "partition")
        public Set<String> partition = new HashSet<>();

        public TableStatistics(String db, String table) {
            this.db = db;
            this.table = table;
        }

        public void addColumns(List<String> columns) {
            if (columns != null && columns.size() > 0) {
                this.columnList.addAll(columns);
            }
        }

        public void addPartitions(List<String> partitionNames) {
            if (partitionNames == null || partitionNames.isEmpty()) {
                return;
            }
            this.partition.addAll(partitionNames);
            if (this.partition.size() > 5) {
                List<String> top5 =
                        partition.stream().sorted(Comparator.reverseOrder()).limit(5).collect(Collectors.toList());
                this.partition = new HashSet<>(top5);
            }
        }
    }

    @AuditField(value = "isScanAllPartitions")
    public boolean isScanAllPartitions = true;

    @AuditField(value = "isPartitionPruningSuccess")
    public boolean isPartitionPruningSuccess = true;

    public static class AuditEventBuilder {
        private List<String> tables = new ArrayList<>();
        private Set<String> exceptions = new HashSet<>();
        private TableStatisticsInfo statistics = null;
        private Set<String> tableUseOmsStatistics = new HashSet<>();

        private AuditEvent auditEvent = new AuditEvent();

        public AuditEventBuilder() {
        }

        public void reset() {
            auditEvent = new AuditEvent();
            tables = new ArrayList<>();
            exceptions = new HashSet<>();
            tableUseOmsStatistics = new HashSet<>();
            statistics = null;
        }

        public AuditEventBuilder setEventType(EventType eventType) {
            auditEvent.type = eventType;
            return this;
        }

        public AuditEventBuilder setTimestamp(long timestamp) {
            auditEvent.timestamp = timestamp;
            return this;
        }

        public AuditEventBuilder setClientIp(String clientIp) {
            auditEvent.clientIp = clientIp;
            return this;
        }

        public AuditEventBuilder setUser(String user) {
            auditEvent.user = user;
            return this;
        }

        public AuditEventBuilder setAuthorizedUser(String authorizedUser) {
            auditEvent.authorizedUser = authorizedUser;
            return this;
        }

        public AuditEventBuilder setResourceGroup(String resourceGroup) {
            auditEvent.resourceGroup = resourceGroup;
            return this;
        }

        public AuditEventBuilder setCatalog(String catalog) {
            auditEvent.catalog = catalog;
            return this;
        }

        public AuditEventBuilder setDb(String db) {
            auditEvent.db = db;
            return this;
        }

        public AuditEventBuilder setState(String state) {
            auditEvent.state = state;
            return this;
        }

        public AuditEventBuilder setErrorCode(String errorCode) {
            auditEvent.errorCode = errorCode;
            return this;
        }

        public AuditEventBuilder setQueryTime(long queryTime) {
            auditEvent.queryTime = queryTime;
            return this;
        }

        public AuditEventBuilder setScanBytes(long scanBytes) {
            auditEvent.scanBytes = scanBytes;
            return this;
        }

        public AuditEventBuilder setScanRows(long scanRows) {
            auditEvent.scanRows = scanRows;
            return this;
        }

        public AuditEventBuilder setReturnRows(long returnRows) {
            auditEvent.returnRows = returnRows;
            return this;
        }

        /**
         * Cpu cost in nanoseconds
         */
        public AuditEventBuilder setCpuCostNs(long cpuNs) {
            auditEvent.cpuCostNs = cpuNs;
            return this;
        }

        public AuditEventBuilder setMemCostBytes(long memCostBytes) {
            auditEvent.memCostBytes = memCostBytes;
            return this;
        }

        public AuditEventBuilder setFeedbackMemCostBytes(double feedbackMemCostBytes) {
            auditEvent.feedbackMemCostBytes = feedbackMemCostBytes;
            return this;
        }


        public AuditEventBuilder setSpilledBytes(long spilledBytes) {
            auditEvent.spilledBytes = spilledBytes;
            return this;
        }

        public AuditEventBuilder setWarehouse(String warehouse) {
            auditEvent.warehouse = warehouse;
            return this;
        }

        public AuditEventBuilder setStmtId(long stmtId) {
            auditEvent.stmtId = stmtId;
            return this;
        }

        public AuditEventBuilder setQueryId(String queryId) {
            auditEvent.queryId = queryId;
            return this;
        }

        public AuditEventBuilder setIsQuery(boolean isQuery) {
            auditEvent.isQuery = isQuery;
            return this;
        }

        public AuditEventBuilder setDataSource(String dataSource) {
            auditEvent.dataSource = dataSource;
            return this;
        }

        public AuditEventBuilder setRequestType(QueryState.RequestType requestType) {
            auditEvent.requestType = requestType.name();
            return this;
        }

        public AuditEventBuilder setFeIp(String feIp) {
            auditEvent.feIp = feIp;
            return this;
        }

        public AuditEventBuilder setStmt(String stmt) {
            auditEvent.stmt = stmt;
            return this;
        }

        public AuditEventBuilder setDigest(String digest) {
            auditEvent.digest = digest;
            return this;
        }

        public AuditEventBuilder setPlanCpuCosts(double cpuCosts) {
            auditEvent.planCpuCosts = cpuCosts;
            return this;
        }

        public AuditEventBuilder setPlanMemCosts(double memCosts) {
            auditEvent.planMemCosts = memCosts;
            return this;
        }

        public AuditEventBuilder setPendingTimeMs(long pendingTimeMs) {
            auditEvent.pendingTimeMs = pendingTimeMs;
            return this;
        }

        public AuditEventBuilder setNumSlots(int numSlots) {
            auditEvent.numSlots = numSlots;
            return this;
        }

        public AuditEventBuilder setBigQueryLogCPUSecondThreshold(long bigQueryLogCPUSecondThreshold) {
            auditEvent.bigQueryLogCPUSecondThreshold = bigQueryLogCPUSecondThreshold;
            return this;
        }

        public AuditEventBuilder setBigQueryLogScanBytesThreshold(long bigQueryLogScanBytesThreshold) {
            auditEvent.bigQueryLogScanBytesThreshold = bigQueryLogScanBytesThreshold;
            return this;
        }

        public AuditEventBuilder setBigQueryLogScanRowsThreshold(long bigQueryLogScanRowsThreshold) {
            auditEvent.bigQueryLogScanRowsThreshold = bigQueryLogScanRowsThreshold;
            return this;
        }

        public AuditEventBuilder setCandidateMvs(List<String> mvs) {
            this.auditEvent.candidateMvs = Joiner.on(",").join(mvs);
            return this;
        }

        public AuditEventBuilder setHitMvs(List<String> mvs) {
            this.auditEvent.hitMVs = Joiner.on(",").join(mvs);
            return this;
        }

        public String getHitMvs() {
            return this.auditEvent.hitMVs;
        }

        public AuditEventBuilder setIsForwardToLeader(boolean isForwardToLeader) {
            auditEvent.isForwardToLeader = isForwardToLeader;
            return this;
        }

        public AuditEventBuilder setException(String exception) {
            auditEvent.exception = exception;
            return this;
        }

        public AuditEventBuilder setErrorMessage(String errorMessage) {
            auditEvent.errorMessage = errorMessage;
            return this;
        }

        public AuditEventBuilder setStmtType(String stmtType) {
            auditEvent.stmtType = stmtType;
            return this;
        }

        public AuditEventBuilder addTable(String table) {
            tables.add(table);
            return this;
        }

        public AuditEventBuilder addException(String exception) {
            exceptions.add(exception);
            return this;
        }

        public AuditEventBuilder setSupersqlTraceId(String supersqlTraceId) {
            auditEvent.supersqlTraceId = supersqlTraceId;
            return this;
        }

        public AuditEventBuilder setIsScanAllPartitions(boolean isScanAllPartitions) {
            auditEvent.isScanAllPartitions = isScanAllPartitions;
            return this;
        }

        public AuditEventBuilder setIsPartitionPruningSuccess(boolean isPartitionPruningSuccess) {
            auditEvent.isPartitionPruningSuccess = isPartitionPruningSuccess;
            return this;
        }

        public AuditEventBuilder setProfileSize(long profileSize) {
            auditEvent.profileSize = profileSize;
            return this;
        }

        public AuditEventBuilder addTableUseOmsStatistics(String table) {
            tableUseOmsStatistics.add(table);
            return this;
        }

        public AuditEventBuilder addTableStatisticInfo(String db, String tableName,
                                                       List<String> columnNames, List<String> partitionNames) {
            if (statistics == null) {
                statistics = new TableStatisticsInfo();
            }
            if (statistics.tableStatistics == null) {
                statistics.tableStatistics = new ArrayList<>(1);
            }
            for (TableStatistics tableStatistics : statistics.tableStatistics) {
                if (tableStatistics.db.equals(db) && tableStatistics.table.equals(tableName)) {
                    tableStatistics.addColumns(columnNames);
                    tableStatistics.addPartitions(partitionNames);
                    return this;
                }
            }
            TableStatistics newTable = new TableStatistics(db, tableName);
            newTable.addColumns(columnNames);
            newTable.addPartitions(partitionNames);
            statistics.tableStatistics.add(newTable);
            return this;
        }

        public AuditEvent build() {
            if (!tables.isEmpty()) {
                auditEvent.table = String.join(",", tables);
            }
            if (!exceptions.isEmpty()) {
                auditEvent.exception = String.join(",", exceptions);
            }
            if (statistics == null || statistics.tableStatistics == null || statistics.tableStatistics.isEmpty()) {
                auditEvent.statistics = "";
            } else {
                auditEvent.statistics = GSON.toJson(statistics);
            }
            if (!tableUseOmsStatistics.isEmpty()) {
                auditEvent.tableUseOmsStatistics = String.join(",", tableUseOmsStatistics);
            }
            return this.auditEvent;
        }
    }
}
