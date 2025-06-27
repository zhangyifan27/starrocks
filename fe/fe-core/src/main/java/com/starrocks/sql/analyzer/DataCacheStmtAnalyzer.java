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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.datacache.DataCacheJobMgr;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ClearDataCacheRulesStmt;
import com.starrocks.sql.ast.CreateDataCacheJobStmt;
import com.starrocks.sql.ast.CreateDataCacheRuleStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.DropDataCacheRuleStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.thrift.TCacheSelectMode;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DataCacheStmtAnalyzer {
    private DataCacheStmtAnalyzer() {
    }

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new DataCacheStmtAnalyzerVisitor().analyze(stmt, session);
    }

    static class DataCacheStmtAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        private final DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();

        public void analyze(StatementBase statement, ConnectContext session) {
            visit(statement, session);
        }

        @Override
        public Void visitCreateDataCacheRuleStatement(CreateDataCacheRuleStmt statement, ConnectContext context) {
            int priority = statement.getPriority();
            if (priority != -1) {
                throw new SemanticException("DataCache only support priority = -1 (aka BlackList) now");
            }

            Map<String, String> properties = statement.getProperties();
            if (properties != null) {
                throw new SemanticException("DataCache don't support specify properties now");
            }

            List<String> parts = statement.getTarget().getParts();

            // check target existed
            String catalogName = parts.get(0);
            String dbName = parts.get(1);
            String tblName = parts.get(2);

            if (CatalogMgr.isInternalCatalog(catalogName)) {
                throw new SemanticException("DataCache only support external catalog now");
            }

            throwExceptionIfTargetIsInvalid(catalogName, dbName, tblName);

            // If catalog/db/tbl does not exist, it will throw exception
            Optional<Table> optionalTable = getTable(catalogName, dbName, tblName);

            // Check new dataCache rule is conflicted with existed rule
            dataCacheMgr.throwExceptionIfRuleIsConflicted(catalogName, dbName, tblName);

            Expr predicates = statement.getPredicates();
            if (predicates != null) {
                if (!optionalTable.isPresent()) {
                    throw new SemanticException("You must have a specific table when using where clause");
                }
                // Build scope
                ImmutableList.Builder<Field> fields = ImmutableList.builder();
                TableName tableName = new TableName(catalogName, dbName, tblName);
                for (Column column : optionalTable.get().getColumns()) {
                    Field field = new Field(column.getName(), column.getType(), tableName,
                            new SlotRef(tableName, column.getName(), column.getName()), true, column.isAllowNull());
                    fields.add(field);
                }
                Scope scope = new Scope(RelationId.anonymous(), new RelationFields(fields.build()));
                ExpressionAnalyzer.analyzeExpression(predicates, new AnalyzeState(), scope, null);
            }

            return null;
        }

        @Override
        public Void visitDropDataCacheRuleStatement(DropDataCacheRuleStmt statement, ConnectContext context) {
            long cacheRuleId = statement.getCacheRuleId();
            if (!dataCacheMgr.isExistCacheRule(cacheRuleId)) {
                throw new SemanticException(String.format("DataCache rule id = %d does not exist", cacheRuleId));
            }
            return null;
        }

        @Override
        public Void visitClearDataCacheRulesStatement(ClearDataCacheRulesStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitDataCacheSelectStatement(DataCacheSelectStatement statement, ConnectContext context) {
            InsertStmt insertStmt = statement.getInsertStmt();
            QueryStatement queryStatement = insertStmt.getQueryStatement();
            // Analyze query sql is valid
            Analyzer.analyze(queryStatement, context);

            SelectRelation selectRelation = (SelectRelation) queryStatement.getQueryRelation();
            if (!(selectRelation.getRelation() instanceof TableRelation)) {
                throw new SemanticException("Cache select only support olap table, external table or materialized view.");
            }
            TableRelation tableRelation = (TableRelation) selectRelation.getRelation();
            TableName tableName = tableRelation.getResolveTableName();
            if (CatalogMgr.isInternalCatalog(tableName.getCatalog()) && RunMode.isSharedNothingMode()) {
                throw new SemanticException("Currently cache select is not supported in local olap table");
            }
            statement.setCatalog(tableName.getCatalog());
            statement.setTableName(tableName);

            Map<String, String> properties = statement.getProperties();
            statement.setVerbose(Boolean.parseBoolean(properties.getOrDefault("verbose", "false")));

            String partition = properties.get("partition");

            if (statement.mode() == TCacheSelectMode.DELETE) {
                if (partition == null) {
                    if (selectRelation.getPredicate() != null) {
                        throw new SemanticException("cache delete is only support full table delete or partition delete," +
                                "not supported predicate by where without partition property");
                    }
                    statement.setPartition(tableName.getTbl());
                } else {
                    statement.setPartition(partition);
                }
            }

            if (!statement.isCreateByJob() && statement.mode() == TCacheSelectMode.DEFAULT) {
                if (partition == null) {
                    if (Config.disable_datacache_without_partition && selectRelation.getPredicate() != null) {
                        throw new SemanticException("cache select is not supported without partition, " +
                                "please data cache by partition or change disable_datacache_without_partition to false.");
                    }
                    statement.setPartition(tableName.getTbl());
                } else if (DataCacheJobMgr.PARTITION_SCHEDULE.equalsIgnoreCase(partition)) {
                    String partitionUnit = properties.get("partition_unit");
                    LocalDateTime now = LocalDateTime.now();
                    if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                        partition = DateUtils.HOUR_FORMATTER_UNIX.format(now.minusHours(1).truncatedTo(ChronoUnit.HOURS));
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                        partition = DateUtils.DATEKEY_FORMATTER_UNIX.format(now.minusDays(1).truncatedTo(ChronoUnit.DAYS));
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                        partition = DateUtils.MONTH_FORMATTER_UNIX.format(now.minusMonths(1).truncatedTo(ChronoUnit.MONTHS));
                    } else { // YEAR
                        partition = DateUtils.YEAR_FORMATTER_UNIX.format(now.minusYears(1).truncatedTo(ChronoUnit.YEARS));
                    }
                    statement.setPartition(DataCacheJobMgr.PARTITION_PREFIX + partition);
                } else {
                    statement.setPartition(partition.toLowerCase());
                }
            }

            int priority = Integer.parseInt(properties.getOrDefault("priority", "0"));
            if (priority != 0 && priority != 1) {
                throw new SemanticException("DataCache's priority can only be set to 0 or 1");
            }
            statement.setPriority(priority);

            // Duration for cache remains active.
            // Use PT0M to prevent expiration of the rule.
            // Use duration specified in ISO-8601 duration format (PnDTnHnMn).
            long ttlSeconds = 0L;
            try {
                ttlSeconds = Duration.parse(properties.getOrDefault("ttl", "PT0M")).toSeconds();
            } catch (DateTimeParseException e) {
                throw new SemanticException(String.format(
                        "Illegal ttl format, use duration specified in ISO-8601 duration format (PnDTnHnMn). Error msg: %s",
                        e.getMessage()));
            }
            if (priority > 0 && ttlSeconds == 0) {
                throw new SemanticException("TTL must be specified when priority > 0");
            }
            statement.setTTLSeconds(ttlSeconds);

            return null;
        }

        @Override
        public Void visitCreateDataCacheJobStatement(CreateDataCacheJobStmt statement, ConnectContext context) {
            DataCacheSelectStatement dataCacheSelectStatement = statement.getDataCacheSelectStatement();
            dataCacheSelectStatement.setCreateByJob(true);
            visit(dataCacheSelectStatement, context);
            SelectRelation selectRelation =
                    (SelectRelation) dataCacheSelectStatement.getInsertStmt().getQueryStatement().getQueryRelation();
            if (selectRelation.getPredicate() != null) {
                throw new SemanticException("current data cache job not supportes predicat");
            }

            // need collect all be data cache info
            dataCacheSelectStatement.setVerbose(true);

            Map<String, String> properties = statement.getProperties();
            if (properties == null || properties.isEmpty()) {
                throw new SemanticException("Data cache job partition_field or full_table_cache properties is necessary");
            }

            String partitionFiled = properties.get("partition_field");
            String fullTableCache = properties.get("full_table_cache");

            if (partitionFiled != null) {
                statement.setPartitionFiled(partitionFiled);

                if (!properties.containsKey("partition_unit")) {
                    throw new SemanticException("Data cache job partition_unit properties is necessary");
                }
                String partitionUnit = properties.get("partition_unit");
                if (!partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())
                        && !partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())
                        && !partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())
                        && !partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.YEAR.toString())) {
                    throw new SemanticException("Data cache job only supportes partition unit : hour|day|month|year");
                }
                statement.setPartitionUnit(properties.get("partition_unit"));
                dataCacheSelectStatement.getProperties().put("partition_unit", properties.get("partition_unit"));

                // cache_partition_num <= ttl
                if (properties.containsKey("cache_partition_num")) {
                    long ttlSeconds = dataCacheSelectStatement.getTTLSeconds();
                    int cachePartitionNum = Integer.parseInt(properties.get("cache_partition_num"));
                    long cacheTime;
                    if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                        cacheTime = cachePartitionNum * 60 * 60L;
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.DAY.toString())) {
                        cacheTime = cachePartitionNum * 24 * 60 * 60L;
                    } else if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.MONTH.toString())) {
                        cacheTime = cachePartitionNum * 31 * 24 * 60 * 60L;
                    } else { // YEAR
                        cacheTime = cachePartitionNum * 365 * 31 * 24 * 60 * 60L;
                    }
                    if (cacheTime > ttlSeconds) {
                        throw new SemanticException("cache_partition_num * partition_unit need less than ttl");
                    }
                }
                statement.setCachePartitionNum(Integer.parseInt(properties.getOrDefault("cache_partition_num", "0")));

                if (!properties.containsKey("partition_field_type")) {
                    throw new DmlException("Data cache job can not identification the partition field type, " +
                            "please add partition_field_type property");
                }
                String partitionFiledTypeFromProperties = properties.get("partition_field_type");
                if ("int".equalsIgnoreCase(partitionFiledTypeFromProperties)
                        || "bigint".equalsIgnoreCase(partitionFiledTypeFromProperties)) {
                    statement.setPartitionFiledType(Type.INT);
                } else if ("date".equalsIgnoreCase(partitionFiledTypeFromProperties)) {
                    if (partitionUnit.equalsIgnoreCase(TimestampArithmeticExpr.TimeUnit.HOUR.toString())) {
                        throw new DmlException("date partition type not supports hour partition unit~");
                    }
                    statement.setPartitionFiledType(Type.DATE);
                } else if ("datetime".equalsIgnoreCase(partitionFiledTypeFromProperties)) {
                    statement.setPartitionFiledType(Type.DATETIME);
                } else if ("string".equalsIgnoreCase(partitionFiledTypeFromProperties)) {
                    if (!properties.containsKey("partition_field_format")) {
                        throw new DmlException("string partition type need partition_field_format property");
                    }
                    String partitionFiledFormat = properties.get("partition_field_format");
                    statement.setPartitionFiledType(Type.STRING);
                    statement.setPartitionFiledFormat(partitionFiledFormat);
                } else {
                    throw new DmlException("Data cache job only supportes partition column type : " +
                            "date|datetime|int|bigint|string");
                }
            } else if (!Boolean.parseBoolean(fullTableCache)) {
                throw new SemanticException("Data cache job partition_field or full_table_cache properties is necessary");
            }
            return null;
        }
    }

    private static boolean isSelectAll(String s) {
        return s.equals("*");
    }

    // If catalogName is '*', dbName and tblName must use '*' either
    // If dbName is '*', tblName must use '*'
    private static void throwExceptionIfTargetIsInvalid(String catalogName, String dbName, String tblName) throws
            SemanticException {
        // check validity
        if (isSelectAll(catalogName)) {
            if (!isSelectAll(dbName) || !isSelectAll(tblName)) {
                throw new SemanticException("Catalog is *, database and table must use * either");
            }
            // *.*.* will go here, return directly, don't need to check dbName anymore
            return;
        }

        if (isSelectAll(dbName)) {
            if (!isSelectAll(tblName)) {
                throw new SemanticException("Database is *, table must use * either");
            }
        }
    }

    private static Optional<Table> getTable(String catalogName, String dbName, String tblName) throws SemanticException {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        // Check target is existed
        Table table = null;
        if (!isSelectAll(catalogName)) {
            // Check catalog is existed
            if (!metadataMgr.getOptionalMetadata(catalogName).isPresent()) {
                throw new SemanticException(String.format("DataCache target catalog: %s does not exist", catalogName));
            }

            if (!isSelectAll(dbName)) {
                // Check db is existed
                Database db = metadataMgr.getDb(catalogName, dbName);
                if (db == null) {
                    throw new SemanticException(String.format("DataCache target database: %s does not exist " +
                            "in [catalog: %s]", dbName, catalogName));
                }
                if (!isSelectAll(tblName)) {
                    // Check tbl is existed
                    table = metadataMgr.getTable(catalogName, dbName, tblName);
                    if (table == null) {
                        throw new SemanticException(String.format("DataCache target table: %s does not exist in " +
                                "[catalog: %s, database: %s]", tblName, catalogName, dbName));
                    }
                }
            }
        }
        return Optional.ofNullable(table);
    }
}
