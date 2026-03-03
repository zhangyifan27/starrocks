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

import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateDataCacheJobStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class DataCacheJobMgr {
    private static final Logger LOG = LogManager.getLogger(DataCacheJobMgr.class);

    public static final String PARTITION_SCHEDULE = "__SCHEDULE__";
    public static final String PARTITION_PREFIX = "p";

    public void createJob(CreateDataCacheJobStmt stmt, ConnectContext context) throws DdlException {
        DataCacheSelectStatement cacheSelectStatement = stmt.getDataCacheSelectStatement();

        cacheHistoryPartitions(stmt, cacheSelectStatement, context);

        //submit schedule task
        if (stmt.getSchedule() != null) {
            Map<String, String> properties = new HashMap<>(cacheSelectStatement.getProperties());
            StringBuilder sql = new StringBuilder("SUBMIT TASK ");
            String jobStmt = stmt.getOrigStmt().originStmt
                    .substring("CREATE DATA CACHE JOB".length(), stmt.getDataCacheSelectStart());
            sql.append(jobStmt).append(cacheSelectStatement.toSQLStringWithoutProperties());
            if (!cacheSelectStatement.isFullTableCache()) {
                properties.put("partition", PARTITION_SCHEDULE);
            }
            sql.append(" ").append(buildProperties(properties));

            executeSubmitTaskStmt(sql.toString(), context);
            // Record the schedule task name in table meta so we can trace the job from SHOW DATA CACHE.

            TableName tableName = cacheSelectStatement.getTableName();
            if (tableName != null) {
                DataCacheMetaManager metaManager = GlobalStateMgr.getCurrentState().getDataCacheMetaManager();
                if (metaManager != null) {
                    metaManager.upsertTableMeta(tableName, stmt.getTaskName());
                }
            }
        }
    }

    private void cacheHistoryPartitions(CreateDataCacheJobStmt stmt,
                                        DataCacheSelectStatement cacheSelectStatement,
                                        ConnectContext context) throws DdlException {
        int cachePartitionNum = stmt.getCachePartitionNum();
        if (cachePartitionNum <= 1) {
            return;
        }

        LocalDateTime now = LocalDateTime.now(TimeUtils.getTimeZone().toZoneId());
        LocalDateTime baseTime = now;
        if (stmt.getSchedule() != null && stmt.getSchedule().getStartTime() > 0) {
            LocalDateTime startScheduleTime = Instant.ofEpochMilli(stmt.getSchedule().getStartTime())
                    .atZone(TimeUtils.getTimeZone().toZoneId()).toLocalDateTime();
            if (startScheduleTime.isAfter(now)) {
                baseTime = startScheduleTime;
            }
        }

        long ttlSeconds = cacheSelectStatement.getTTLSeconds();
        String partitionUnit = cacheSelectStatement.getPartitionUnit();

        long partitionUnitSeconds;
        if (partitionUnit.equalsIgnoreCase("hour")) {
            partitionUnitSeconds = 60L * 60L;
        } else if (partitionUnit.equalsIgnoreCase("day")) {
            partitionUnitSeconds = 24L * 60L * 60L;
        } else if (partitionUnit.equalsIgnoreCase("month")) {
            partitionUnitSeconds = 31L * 24L * 60L * 60L;
        } else { // YEAR
            partitionUnitSeconds = 365L * 24L * 60L * 60L;
        }

        String currentPartition = computeKthPreviousPartition(now, 0, partitionUnit);

        for (int beforeUnit = cachePartitionNum; beforeUnit >= 1; beforeUnit--) {
            Map<String, String> properties = new HashMap<>(cacheSelectStatement.getProperties());
            long consumedTime = (beforeUnit - 1) * partitionUnitSeconds;
            long remainingSeconds = ttlSeconds - consumedTime;
            if (remainingSeconds < 0) {
                remainingSeconds = 0;
                LOG.error("CREATE CACHE JOB: invalid cachePartitionNum:{} and ttl:{}", cachePartitionNum, ttlSeconds);
                continue;
            }

            String partition = computeKthPreviousPartition(baseTime, beforeUnit, partitionUnit);

            if (partition.compareTo(currentPartition) > 0) {
                break;
            }

            if (partitionUnit.equalsIgnoreCase("hour")) {
                long ttlHour = remainingSeconds / (60L * 60L);
                properties.put("ttl", "PT" + ttlHour + "H");
            } else {
                long ttlDay = remainingSeconds / (24L * 60L * 60L);
                properties.put("ttl", "P" + ttlDay + "D");
            }

            StringBuilder sql = new StringBuilder("SUBMIT TASK ")
                    .append(stmt.getTaskName()).append("_").append(partition).append(" ");
            if (stmt.getProperties() != null && !stmt.getProperties().isEmpty()) {
                String taskProperties = buildProperties(stmt.getProperties());
                sql.append(taskProperties);
            }
            sql.append(" AS ");
            sql.append(cacheSelectStatement.toSQLStringWithoutProperties());
            properties.put("partition", partition);
            sql.append(" ").append(buildProperties(properties));

            executeSubmitTaskStmt(sql.toString(), context);
        }
    }

    public static String computeKthPreviousPartition(LocalDateTime baseTime, int k, String partitionUnit) {
        LocalDateTime partitionTime;
        if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
            partitionTime = baseTime.minusHours(k).truncatedTo(ChronoUnit.HOURS);
            return PARTITION_PREFIX + DateUtils.HOUR_FORMATTER_UNIX.format(partitionTime);
        } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
            partitionTime = baseTime.minusDays(k).truncatedTo(ChronoUnit.DAYS);
            return PARTITION_PREFIX + DateUtils.DATEKEY_FORMATTER_UNIX.format(partitionTime);
        } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
            // truncatedTo(ChronoUnit.MONTHS) is not supported, manually truncate to first day of month
            LocalDateTime adjustedTime = baseTime.minusMonths(k);
            partitionTime = adjustedTime.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            return PARTITION_PREFIX + DateUtils.MONTH_FORMATTER_UNIX.format(partitionTime);
        } else { // YEAR
            // truncatedTo(ChronoUnit.YEARS) is not supported, manually truncate to first day of year
            LocalDateTime adjustedTime = baseTime.minusYears(k);
            partitionTime = adjustedTime.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
            return PARTITION_PREFIX + DateUtils.YEAR_FORMATTER_UNIX.format(partitionTime);
        }
    }

    private void executeSubmitTaskStmt(String sql, ConnectContext context) throws DdlException {
        SubmitTaskStmt parsedStmt = (SubmitTaskStmt) SqlParser.parse(sql,
                context.getSessionVariable()).get(0);
        StatementPlanner.plan(parsedStmt, context);
        context.getGlobalStateMgr().getTaskManager().handleSubmitTaskStmt(parsedStmt);
    }

    private String buildProperties(Map<String, String> properties) {
        StringBuilder propertiesSql = new StringBuilder("PROPERTIES(");
        boolean first = true;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (!first) {
                propertiesSql.append(",");
            } else {
                first = false;
            }
            propertiesSql.append("\"").append(entry.getKey()).append("\" = \"").append(entry.getValue()).append("\"");
        }
        propertiesSql.append(")");
        return propertiesSql.toString();
    }

}
