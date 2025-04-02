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

import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.CreateDataCacheJobStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class DataCacheJobMgr {
    private static final Logger LOG = LogManager.getLogger(DataCacheJobMgr.class);

    public static final String PARTITION_SCHEDULE = "__SCHEDULE__";
    public static final String PARTITION_PREFIX = "p";

    public void createJob(CreateDataCacheJobStmt stmt, ConnectContext context) throws DdlException {
        DataCacheSelectStatement cacheSelectStatement = stmt.getDataCacheSelectStatement();
        String dataCacheSelectWithoutProperties = stmt.getOrigStmt().originStmt.substring(stmt.getDataCacheSelectStart(),
                stmt.getDataCacheSelectPropertiesStart());
        Map<String, String> properties = cacheSelectStatement.getProperties();
        properties.put("verbose", "true");
        // use original ttl for schedule task
        String ttl = properties.get("ttl");
        // full table cache
        if (stmt.getPartitionFiled() == null) {
            StringBuilder sql = new StringBuilder("SUBMIT TASK ");
            String jobStmt = stmt.getOrigStmt().originStmt
                    .substring("CREATE DATA CACHE JOB".length(), stmt.getDataCacheSelectStart());
            sql.append(jobStmt).append(dataCacheSelectWithoutProperties);
            sql.append(" ").append(buildProperties(properties));
            executeSubmitTaskStmt(sql.toString(), context);
        } else { // cache previous partition
            String partition;
            LocalDateTime now = LocalDateTime.now();
            String partitionUnit = stmt.getPartitionUnit();
            long ttlSeconds = cacheSelectStatement.getTTLSeconds();
            int beforeUnit = 0;
            // there is schedule and no start time; last unit cache by schedule task;
            if (stmt.getSchedule() != null && stmt.getSchedule().getStartTime() == 0) {
                beforeUnit =  1;
            }
            for (; beforeUnit < stmt.getCachePartitionNum(); beforeUnit++) {
                if (cacheSelectStatement.getProperties() != null && !cacheSelectStatement.getProperties().isEmpty()) {
                    if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                        long ttlHour = (ttlSeconds - (beforeUnit * 60 * 60L)) / (60 * 60L);
                        properties.put("ttl", "PT" + ttlHour + "H");
                        partition = DateUtils.HOUR_FORMATTER_UNIX.format(now.minusHours(beforeUnit + 1)
                                .truncatedTo(ChronoUnit.HOURS));
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                        long ttlDay = (ttlSeconds - (beforeUnit * 24 * 60 * 60L)) / (24 * 60 * 60L);
                        properties.put("ttl", "P" + ttlDay + "D");
                        partition = DateUtils.DATEKEY_FORMATTER_UNIX.format(now.minusDays(beforeUnit + 1)
                                .truncatedTo(ChronoUnit.DAYS));
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                        long ttlDay = (ttlSeconds - (beforeUnit * 31 * 24 * 60 * 60L)) / (24 * 60 * 60L);
                        properties.put("ttl", "P" + ttlDay + "D");
                        partition = DateUtils.MONTH_FORMATTER_UNIX.format(now.minusMonths(beforeUnit + 1)
                                .truncatedTo(ChronoUnit.MONTHS));
                    } else { // YEAR
                        long ttlDay = (ttlSeconds - (beforeUnit * 365 * 31 * 24 * 60 * 60L)) / (24 * 60 * 60L);
                        properties.put("ttl", "P" + ttlDay + "D");
                        partition = DateUtils.YEAR_FORMATTER_UNIX.format(now.minusYears(beforeUnit + 1)
                                .truncatedTo(ChronoUnit.YEARS));
                    }

                    StringBuilder sql = new StringBuilder("SUBMIT TASK ")
                            .append(stmt.getTaskName()).append("_").append(PARTITION_PREFIX).append(partition).append(" ");
                    if (stmt.getProperties() != null && !stmt.getProperties().isEmpty()) {
                        String taskProperties = buildProperties(stmt.getProperties());
                        sql.append(taskProperties);
                    }
                    sql.append(" AS ");
                    sql.append(dataCacheSelectWithoutProperties);
                    String where = buildWhere(stmt, beforeUnit);
                    sql.append(where);
                    properties.put("partition", PARTITION_PREFIX + partition);
                    sql.append(" ").append(buildProperties(properties));

                    executeSubmitTaskStmt(sql.toString(), context);
                }
            }

            //submit schedule task
            if (stmt.getSchedule() != null) {
                StringBuilder sql = new StringBuilder("SUBMIT TASK ");
                String jobStmt = stmt.getOrigStmt().originStmt
                        .substring("CREATE DATA CACHE JOB".length(), stmt.getDataCacheSelectStart());
                sql.append(jobStmt).append(dataCacheSelectWithoutProperties);
                String where = buildScheduleWhere(stmt);
                sql.append(where);
                properties.put("verbose", "true");
                properties.put("partition", PARTITION_SCHEDULE);
                if (ttl != null) {
                    properties.put("ttl", ttl);
                }
                sql.append(" ").append(buildProperties(properties));

                executeSubmitTaskStmt(sql.toString(), context);
            }
        }
    }

    private void executeSubmitTaskStmt(String sql, ConnectContext context) throws DdlException {
        SubmitTaskStmt parsedStmt = (SubmitTaskStmt) SqlParser.parse(sql,
                context.getSessionVariable()).get(0);
        StatementPlanner.plan(parsedStmt, context);
        context.getGlobalStateMgr().getTaskManager().handleSubmitTaskStmt(parsedStmt);
    }

    private String buildScheduleWhere(CreateDataCacheJobStmt stmt) {
        StringBuilder whereSql = new StringBuilder(" WHERE ");
        String partitionFiled = stmt.getPartitionFiled();
        whereSql.append(partitionFiled).append(" >= ");

        Type partitionFiledType = stmt.getPartitionFiledType();
        String partitionUnit = stmt.getPartitionUnit();
        String start;
        String end;
        if (partitionFiledType.isInt() || partitionFiledType.isBigint()) {
            if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                start = "CAST(date_format(hours_sub(hours_add(to_date(now()), hour(now())), 1), '%Y%m%d%H') AS INT)";
                end = "CAST(date_format(hours_add(to_date(now()), hour(now())), '%Y%m%d%H') AS INT)";
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                start = "CAST(date_format(days_sub(to_date(now()), 1), '%Y%m%d') AS INT)";
                end = "CAST(date_format(to_date(now()), '%Y%m%d') AS INT)";
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                start = "CAST(date_format(months_sub(days_sub(to_date(now()), dayofmonth(now()) - 1), 1), '%Y%m%d') AS INT)";
                end = "CAST(date_format(days_sub(to_date(now()), dayofmonth(now()) - 1), '%Y%m%d') AS INT)";
            } else { // YEAR
                start = "CAST(date_format(years_sub(days_sub(to_date(now()), dayofyear(now()) - 1), 1), '%Y%m%d') AS INT)";
                end = "CAST(date_format(days_sub(to_date(now()), dayofyear(now()) - 1), '%Y%m%d') AS INT)";
            }
        } else { // date or datetime
            if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                start = "hours_sub(hours_add(to_date(now()), hour(now())), 1)";
                end = "hours_add(to_date(now()), hour(now()))";
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                start = "days_sub(to_date(now()), 1)";
                end = "to_date(now())";
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                start = "months_sub(days_sub(to_date(now()), dayofmonth(now()) - 1), 1)";
                end = "days_sub(to_date(now()), dayofmonth(now()) - 1)";
            } else { // YEAR
                start = "years_sub(days_sub(to_date(now()), dayofyear(now()) - 1), 1)";
                end = "days_sub(to_date(now()), dayofyear(now()) - 1)";
            }
        }
        whereSql.append(start).append(" AND ").append(partitionFiled).append(" < ").append(end);
        return whereSql.toString();
    }

    private String buildWhere(CreateDataCacheJobStmt stmt, int beforeUnit) {
        StringBuilder whereSql = new StringBuilder(" WHERE ");
        String partitionFiled = stmt.getPartitionFiled();
        whereSql.append(partitionFiled).append(" >= ");

        Type partitionFiledType = stmt.getPartitionFiledType();
        String partitionUnit = stmt.getPartitionUnit();
        LocalDateTime now = LocalDateTime.now();
        String start;
        String end;
        if (partitionFiledType.isInt() || partitionFiledType.isBigint()) {
            if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                start = DateUtils.HOUR_FORMATTER_UNIX.format(now.minusHours(beforeUnit + 1).truncatedTo(ChronoUnit.HOURS));
                end = DateUtils.HOUR_FORMATTER_UNIX.format(now.minusHours(beforeUnit).truncatedTo(ChronoUnit.HOURS));
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                start = DateUtils.DATEKEY_FORMATTER_UNIX.format(now.minusDays(beforeUnit + 1).truncatedTo(ChronoUnit.DAYS));
                end = DateUtils.DATEKEY_FORMATTER_UNIX.format(now.minusDays(beforeUnit).truncatedTo(ChronoUnit.DAYS));
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                start = DateUtils.MONTH_FORMATTER_UNIX.format(now.minusMonths(beforeUnit + 1).truncatedTo(ChronoUnit.MONTHS));
                end = DateUtils.MONTH_FORMATTER_UNIX.format(now.minusMonths(beforeUnit).truncatedTo(ChronoUnit.MONTHS));
            } else { // YEAR
                start = DateUtils.YEAR_FORMATTER_UNIX.format(now.minusYears(beforeUnit + 1).truncatedTo(ChronoUnit.YEARS));
                end = DateUtils.YEAR_FORMATTER_UNIX.format(now.minusYears(beforeUnit).truncatedTo(ChronoUnit.YEARS));
            }
            whereSql.append(start).append(" AND ").append(partitionFiled).append(" < ").append(end);
        } else if (partitionFiledType.isDate()) {
            if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                start = DateUtils.DATE_FORMATTER_UNIX.format(now.minusDays(beforeUnit + 1).truncatedTo(ChronoUnit.DAYS));
                end = DateUtils.DATE_FORMATTER_UNIX.format(now.minusDays(beforeUnit).truncatedTo(ChronoUnit.DAYS));
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                start = DateUtils.DATE_FORMATTER_UNIX.format(now.minusMonths(beforeUnit + 1).truncatedTo(ChronoUnit.MONTHS));
                end = DateUtils.DATE_FORMATTER_UNIX.format(now.minusMonths(beforeUnit).truncatedTo(ChronoUnit.MONTHS));
            } else { // YEAR
                start = DateUtils.DATE_FORMATTER_UNIX.format(now.minusYears(beforeUnit + 1).truncatedTo(ChronoUnit.YEARS));
                end = DateUtils.DATE_FORMATTER_UNIX.format(now.minusYears(beforeUnit).truncatedTo(ChronoUnit.YEARS));
            }
            whereSql.append("\"").append(start).append("\"").append(" AND ").append(partitionFiled).append(" < ")
                    .append("\"").append(end).append("\"");
        } else { // datetime
            if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                start = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusHours(beforeUnit + 1).truncatedTo(ChronoUnit.HOURS));
                end = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusHours(beforeUnit).truncatedTo(ChronoUnit.HOURS));
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                start = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusDays(beforeUnit + 1).truncatedTo(ChronoUnit.DAYS));
                end = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusDays(beforeUnit).truncatedTo(ChronoUnit.DAYS));
            } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                start = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusMonths(beforeUnit + 1).truncatedTo(ChronoUnit.MONTHS));
                end = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusMonths(beforeUnit).truncatedTo(ChronoUnit.MONTHS));
            } else { // YEAR
                start = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusYears(beforeUnit + 1).truncatedTo(ChronoUnit.YEARS));
                end = DateUtils.DATE_TIME_FORMATTER_UNIX.format(now.minusYears(beforeUnit).truncatedTo(ChronoUnit.YEARS));
            }
            whereSql.append("\"").append(start).append("\"").append(" AND ").append(partitionFiled).append(" < ")
                    .append("\"").append(end).append("\"");
        }
        return whereSql.toString();
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
