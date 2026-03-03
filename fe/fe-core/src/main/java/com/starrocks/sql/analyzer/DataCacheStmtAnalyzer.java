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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TimestampArithmeticExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.DateUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.datacache.DataCacheJobMgr;
import com.starrocks.datacache.DataCacheMetaManager;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.datacache.DataCachePartitionMeta;
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
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.DmlException;
import com.starrocks.thrift.TCacheSelectMode;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class DataCacheStmtAnalyzer {
    private static final Logger LOG = LogManager.getLogger(DataCacheStmtAnalyzer.class);
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
            Table table = tableRelation.getTable();
            if (table == null) {
                throw new SemanticException("Cache select table does not exist");
            }

            statement.setCatalog(tableName.getCatalog());
            statement.setTableName(tableName);
            statement.setTable(table);
            assignProperties(statement);

            if (Config.enable_oteam_datacache) {
                if (statement.hasUserPredicate()) {
                    throw new SemanticException("CACHE SELECT does not support explicit WHERE clause" +
                            " if enable_oteam_datacache set to true");
                }
                if (statement.mode() == TCacheSelectMode.DELETE) {
                    context.getSessionVariable().setForceScheduleLocal(true);
                    prepareCacheDelete(statement);
                } else if (statement.mode() == TCacheSelectMode.DESC) {
                    prepareCacheDesc(statement);
                }
                validateSelectList(selectRelation);
                validatePartitionProperties(statement);
                validateCacheMeta(statement);
                injectPartitionPredicate(statement, selectRelation);
            } else {
                if (statement.mode() == TCacheSelectMode.DESC || statement.mode() == TCacheSelectMode.DELETE) {
                    throw new SemanticException("Only oteam data cache implementation support cache desc & delete");
                }
            }
            validateCommonProperties(statement);

            return null;
        }

        @Override
        public Void visitCreateDataCacheJobStatement(CreateDataCacheJobStmt statement, ConnectContext context) {
            DataCacheSelectStatement dataCacheSelectStatement = statement.getDataCacheSelectStatement();
            dataCacheSelectStatement.setCreateByJob(true);

            if (!Config.enable_oteam_datacache) {
                throw new SemanticException("only oteam data cache implementation support CREATE DATA CACHE JOB");
            }

            Map<String, String> cacheSelectProperties = dataCacheSelectStatement.getProperties();
            if (!cacheSelectProperties.getOrDefault("partition", "").isEmpty()) {
                throw new SemanticException("Create data cache job generate partition dynamically, should not set manually");
            }
            cacheSelectProperties.put("partition", DataCacheJobMgr.PARTITION_SCHEDULE);
            cacheSelectProperties.put("verbose", "true");
            visit(dataCacheSelectStatement, context);

            Map<String, String> dataCacheJobProperties = statement.getProperties();
            int cachePartitionNum = 1;
            if (dataCacheJobProperties != null && !dataCacheJobProperties.isEmpty()) {
                cachePartitionNum = Integer.parseInt(dataCacheJobProperties.getOrDefault("cache_partition_num", "1"));
            }
            statement.setCachePartitionNum(cachePartitionNum);

            String partitionField = dataCacheSelectStatement.getPartitionField();
            String partitionUnit = dataCacheSelectStatement.getPartitionUnit();
            if (partitionField != null && !partitionField.isEmpty()) {
                // cache_partition_num <= ttl
                if (cachePartitionNum > 1) {
                    long ttlSeconds = dataCacheSelectStatement.getTTLSeconds();
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
            }

            return null;
        }

        private void validateCacheMeta(DataCacheSelectStatement statement) {
            if (statement.isCacheSelect() || statement.isCacheDesc()) {
                return;
            }

            TableName tableName = statement.getTableName();
            String partition =  statement.getPartition();
            DataCacheMetaManager metaManager = GlobalStateMgr.getCurrentState().getDataCacheMetaManager();
            if (!metaManager.existsTable(statement.getTableName())) {
                throw new SemanticException(String.format(
                        "Table %s is not present in table_cache_meta", tableName));
            }
            if (!metaManager.existsPartition(statement.getTableName(), partition)) {
                throw new SemanticException(String.format(
                        "Partition '%s' not found in partition_cache_meta for table %s", partition, tableName));
            }
        }

        private void validateSelectList(SelectRelation selectRelation) {
            SelectList selectList = selectRelation.getSelectList();
            if (selectList == null || selectList.getItems().isEmpty()) {
                throw new SemanticException("CACHE SELECT/DELETE requires SELECT *");
            }
            boolean onlyStar = selectList.getItems().stream().allMatch(SelectListItem::isStar);
            if (!onlyStar) {
                throw new SemanticException("CACHE SELECT/DELETE only supports SELECT *");
            }
        }

        private void prepareCacheDelete(DataCacheSelectStatement statement) {
            Map<String, String> properties = statement.getProperties();
            String partition = properties.getOrDefault("partition", "").toLowerCase();

            // Handle full table cache partition key (case-insensitive)
            if (partition.equalsIgnoreCase(DataCacheSelectStatement.PARTITION_FULL_TABLE)) {
                partition = DataCacheSelectStatement.PARTITION_FULL_TABLE;
                statement.setFullTableCache(true);
                statement.setPartition(partition);
            }

            String deleteModeValue = properties.getOrDefault("cache_delete_mode", "normal").toLowerCase();
            if ("normal".equals(deleteModeValue)) {
                statement.setDeleteMode(DataCacheMetaManager.CacheDeleteMode.NORMAL);
            } else if ("gc".equals(deleteModeValue)) {
                statement.setDeleteMode(DataCacheMetaManager.CacheDeleteMode.GC);
            } else {
                throw new SemanticException("CACHE DELETE cache_delete_mode must be either 'normal' or 'gc'");
            }

            DataCachePartitionMeta partitionMeta = getDataCachePartitionMeta(statement, partition);
            String partitionField = partitionMeta.getPartitionField();
            String partitionUnit = partitionMeta.getPartitionUnit();
            String partitionFieldFormat = partitionMeta.getPartitionFieldFormat();
            String partitionFieldType = partitionMeta.getPartitionFieldType();

            if (!properties.containsKey("partition_field")) {
                properties.put("partition_field", partitionField);
                statement.setPartitionField(partitionField);
            }
            if (!properties.containsKey("partition_unit")) {
                properties.put("partition_unit", partitionUnit);
                statement.setPartitionUnit(partitionUnit);
            }
            if (!properties.containsKey("partition_field_format")) {
                properties.put("partition_field_format", partitionFieldFormat);
                statement.setPartitionFieldFormat(partitionFieldFormat);
            }
            if (!properties.containsKey("partition_field_type")) {
                properties.put("partition_field_type", partitionFieldType);
                statement.setPartitionFieldType(partitionFieldType);
            }
        }

        private void prepareCacheDesc(DataCacheSelectStatement statement) {
            Map<String, String> properties = statement.getProperties();
            String partition = properties.getOrDefault("partition", "").toLowerCase();

            // Handle full table cache partition key (case-insensitive)
            if (partition.equalsIgnoreCase(DataCacheSelectStatement.PARTITION_FULL_TABLE)) {
                partition = DataCacheSelectStatement.PARTITION_FULL_TABLE;
                statement.setFullTableCache(true);
                statement.setPartition(partition);
            }

            DataCachePartitionMeta partitionMeta = getDataCachePartitionMeta(statement, partition);
            String partitionField = partitionMeta.getPartitionField();
            String partitionUnit = partitionMeta.getPartitionUnit();
            String partitionFieldFormat = partitionMeta.getPartitionFieldFormat();
            String partitionFieldType = partitionMeta.getPartitionFieldType();

            if (!properties.containsKey("partition_field")) {
                properties.put("partition_field", partitionField);
                statement.setPartitionField(partitionField);
            }
            if (!properties.containsKey("partition_unit")) {
                properties.put("partition_unit", partitionUnit);
                statement.setPartitionUnit(partitionUnit);
            }
            if (!properties.containsKey("partition_field_format")) {
                properties.put("partition_field_format", partitionFieldFormat);
                statement.setPartitionFieldFormat(partitionFieldFormat);
            }
            if (!properties.containsKey("partition_field_type")) {
                properties.put("partition_field_type", partitionFieldType);
                statement.setPartitionFieldType(partitionFieldType);
            }
        }


        private void assignProperties(DataCacheSelectStatement statement) {
            Map<String, String> properties = statement.getProperties();
            String partition = properties.getOrDefault("partition", "");
            String partitionField = properties.getOrDefault("partition_field", "");
            String partitionFieldType = properties.getOrDefault("partition_field_type", "");
            String partitionFieldFormat = properties.getOrDefault("partition_field_format", "");
            String partitionUnit = properties.getOrDefault("partition_unit", "");
            boolean fullTableCache = Boolean.parseBoolean(properties.getOrDefault("full_table_cache", ""));
            int priority = Integer.parseInt(properties.getOrDefault("priority", "0"));
            long ttlSeconds;
            try {
                ttlSeconds = Duration.parse(properties.getOrDefault("ttl", "PT0M")).toSeconds();
            } catch (DateTimeParseException e) {
                throw new SemanticException(String.format(
                        "Illegal ttl format, use duration specified in ISO-8601 duration format (PnDTnHnMn). Error msg: %s",
                        e.getMessage()));
            }

            statement.setVerbose(Boolean.parseBoolean(properties.getOrDefault("verbose", "true")));
            statement.setPartition(partition);
            statement.setPartitionField(partitionField);
            statement.setPartitionFieldType(partitionFieldType);
            statement.setPartitionFieldFormat(partitionFieldFormat);
            statement.setPartitionUnit(partitionUnit);
            statement.setFullTableCache(fullTableCache);
            statement.setPriority(priority);
            statement.setTTLSeconds(ttlSeconds);
        }

        private void validateCommonProperties(DataCacheSelectStatement statement) {
            int priority = statement.getPriority();
            if (priority != 0 && priority != 1) {
                throw new SemanticException("DataCache's priority can only be set to 0 or 1");
            }

            long ttlSeconds = statement.getTTLSeconds();
            if (priority > 0 && ttlSeconds == 0) {
                throw new SemanticException("TTL must be specified when priority > 0");
            }
        }

        private void validatePartitionProperties(DataCacheSelectStatement statement) {
            String partition = statement.getPartition();
            String partitionField = statement.getPartitionField();
            String partitionFieldType = statement.getPartitionFieldType();
            String partitionFieldFormat = statement.getPartitionFieldFormat();
            String partitionUnit = statement.getPartitionUnit();
            boolean fullTableCache = statement.isFullTableCache();

            if (fullTableCache) {
                // For full table cache, partition can be empty or __FULL_TABLE__
                boolean isPartitionValid = partition.isEmpty() ||
                        partition.equals(DataCacheSelectStatement.PARTITION_FULL_TABLE);
                if (!isPartitionValid || !partitionField.isEmpty() || !partitionFieldType.isEmpty() ||
                        !partitionFieldFormat.isEmpty() || !partitionUnit.isEmpty()) {
                    throw new SemanticException("CACHE SELECT: full table cache should not has partition properties.");
                }
                statement.setPartitionName(DataCacheSelectStatement.PARTITION_FULL_TABLE);
                statement.setPartition(DataCacheSelectStatement.PARTITION_FULL_TABLE);
                return;
            }

            if (partition.isEmpty() || partitionField.isEmpty() || partitionFieldType.isEmpty() || partitionUnit.isEmpty()) {
                throw new SemanticException("CACHE SELECT requires partition_field, " +
                        "partition_field_type, partition_unit properties (if not full table cache)");
            }

            if (partition.equalsIgnoreCase(DataCacheJobMgr.PARTITION_SCHEDULE)) {
                partition = DataCacheJobMgr.computeKthPreviousPartition(LocalDateTime.now(), 1, partitionUnit);
            }
            statement.getProperties().put("partition", partition);
            statement.setPartition(partition);

            checkPartitionUnit(statement);

            if (partitionFieldType.equalsIgnoreCase("string")) {
                if (partitionFieldFormat.isEmpty()) {
                    throw new SemanticException("CACHE SELECT requires partition_field_format for string field");
                }
            }

            setNormalizedPartitionName(statement);
        }
    }



    @NotNull
    private static DataCachePartitionMeta getDataCachePartitionMeta(DataCacheSelectStatement statement, String partition) {
        TableName tableName = statement.getTableName();
        DataCacheMetaManager metaManager = GlobalStateMgr.getCurrentState().getDataCacheMetaManager();
        Optional<DataCachePartitionMeta> partitionMetaOpt = metaManager.getPartitionMeta(tableName, partition);
        if (partitionMetaOpt.isEmpty()) {
            throw new SemanticException(String.format(
                    "Partition '%s' not found in partition_cache_meta for table %s", partition, tableName));
        }

        return partitionMetaOpt.get();
    }

    private static Type transformPartitionFieldType(String type) {
        if ("int".equalsIgnoreCase(type)
                || "bigint".equalsIgnoreCase(type)) {
            return Type.INT;
        } else if ("date".equalsIgnoreCase(type)) {
            return Type.DATE;
        } else if ("datetime".equalsIgnoreCase(type)) {
            return Type.DATETIME;
        } else if ("string".equalsIgnoreCase(type)) {
            return Type.STRING;
        } else {
            throw new DmlException("Unsupported partition_field_type: " + type
                    + ", supported types: date|datetime|int|bigint|string");
        }
    }

    private static void setNormalizedPartitionName(DataCacheSelectStatement statement) {
        String partition = statement.getPartition();
        Optional<String> normalizedPartitionName = Optional.empty();
        try {
            Type type = transformPartitionFieldType(statement.getPartitionFieldType());
            ChronoUnit chronoUnit = resolveChronoUnit(statement.getPartitionUnit(), partition);
            LocalDateTime base = parsePartitionValue(partition);
            LocalDateTime lowerBound = truncateToUnit(base, chronoUnit);
            LiteralExpr lowerLiteral = buildPartitionLiteral(type, lowerBound, statement.getPartitionFieldFormat(),
                    chronoUnit);
            normalizedPartitionName = buildNormalizedPartitionName(statement.getTable(), statement, chronoUnit,
                    lowerBound, lowerLiteral);
        } catch (Exception e) {
            LOG.warn("Failed to construct normalized partition name, use raw partition. error: {}", e.getMessage());
        }

        statement.setPartitionName(normalizedPartitionName.orElse(partition));
    }

    private static void checkPartitionUnit(DataCacheSelectStatement statement) {
        Map<String, String> properties = statement.getProperties();
        String partitionUnit = properties.get("partition_unit");
        String partition = properties.get("partition");
        String parsedPartitionUnit = null;

        try {
            parsedPartitionUnit = parsePartitionUnit(partition);
        } catch (AnalysisException e) {
            throw new SemanticException(String.format(
                    "Failed to parse partition value '%s' to infer partition unit: %s",
                    partition, e.getMessage()));
        }

        if (!parsedPartitionUnit.equalsIgnoreCase(partitionUnit)) {
            throw new SemanticException(String.format("partition unit %s is not equal to %s",
                    partitionUnit, parsedPartitionUnit));
        }
    }

    private static void injectPartitionPredicate(DataCacheSelectStatement statement, SelectRelation selectRelation) {
        if (statement.isFullTableCache()) {
            return;
        }

        Type type = transformPartitionFieldType(statement.getPartitionFieldType());
        LiteralExpr lowerLiteral;
        LiteralExpr upperLiteral;
        try {
            String partitionValue = statement.getPartition();
            ChronoUnit chronoUnit = resolveChronoUnit(statement.getPartitionUnit(), partitionValue);
            LocalDateTime base = parsePartitionValue(partitionValue);
            LocalDateTime lowerBound = truncateToUnit(base, chronoUnit);
            LocalDateTime upperBound = lowerBound.plus(1, chronoUnit);
            lowerLiteral = buildPartitionLiteral(type, lowerBound, statement.getPartitionFieldFormat(), chronoUnit);
            upperLiteral = buildPartitionLiteral(type, upperBound, statement.getPartitionFieldFormat(), chronoUnit);
        } catch (AnalysisException e) {
            throw new SemanticException(String.format(
                    "Failed to parse partition value '%s' as %s: %s",
                    statement.getPartition(), statement.getPartitionFieldType(), e.getMessage()));
        }
        SlotRef slotRef = new SlotRef(statement.getTableName(), statement.getPartitionField());
        slotRef.setBackQuoted(true);
        BinaryPredicate lowerPredicate = new BinaryPredicate(BinaryType.GE, slotRef, lowerLiteral);
        BinaryPredicate upperPredicate =
                new BinaryPredicate(BinaryType.LT, (Expr) slotRef.clone(), upperLiteral);
        selectRelation.setPredicate(
                new CompoundPredicate(CompoundPredicate.Operator.AND, lowerPredicate, upperPredicate));
    }

    private static ChronoUnit resolveChronoUnit(String partitionUnit, String partitionValue) throws AnalysisException {
        String unit = partitionUnit;
        if (Strings.isNullOrEmpty(unit)) {
            unit = parsePartitionUnit(partitionValue);
        }
        return toChronoUnit(unit);
    }

    private static ChronoUnit toChronoUnit(String partitionUnit) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionUnit)) {
            throw new AnalysisException("partition unit is not specified");
        }
        switch (partitionUnit.toLowerCase(Locale.ROOT)) {
            case "hour":
                return ChronoUnit.HOURS;
            case "day":
                return ChronoUnit.DAYS;
            case "month":
                return ChronoUnit.MONTHS;
            case "year":
                return ChronoUnit.YEARS;
            default:
                throw new AnalysisException(String.format("Unsupported partition unit '%s'", partitionUnit));
        }
    }

    private static LocalDateTime truncateToUnit(LocalDateTime dt, ChronoUnit unit) {
        if (unit == ChronoUnit.HOURS) {
            return dt.truncatedTo(ChronoUnit.HOURS);
        } else if (unit == ChronoUnit.DAYS) {
            return dt.truncatedTo(ChronoUnit.DAYS);
        } else if (unit == ChronoUnit.MONTHS) {
            return LocalDateTime.of(dt.getYear(), dt.getMonth(), 1, 0, 0);
        } else if (unit == ChronoUnit.YEARS) {
            return LocalDateTime.of(dt.getYear(), 1, 1, 0, 0);
        }
        return dt;
    }

    private static LiteralExpr buildPartitionLiteral(Type type, LocalDateTime dt,
                                                     String stringFormat, ChronoUnit unit)
            throws AnalysisException {
        String literalString;
        if (type == Type.DATE) {
            literalString = dt.format(DateUtils.DATE_FORMATTER_UNIX);
        } else if (type == Type.DATETIME) {
            literalString = dt.format(DateUtils.DATE_TIME_FORMATTER_UNIX);
        } else if (type == Type.STRING) {
            if (Strings.isNullOrEmpty(stringFormat)) {
                throw new AnalysisException("partition_field_format is required for string partition field");
            }
            literalString = dt.format(DateUtils.unixDatetimeFormatter(stringFormat, true));
        } else if (type == Type.INT || type == Type.BIGINT) {
            literalString = formatNumericPartitionLiteral(dt, unit);
        } else {
            throw new AnalysisException(String.format("Unsupported partition field type %s", type));
        }
        return LiteralExpr.create(literalString, type);
    }

    private static String formatNumericPartitionLiteral(LocalDateTime dt, ChronoUnit unit)
            throws AnalysisException {
        DateTimeFormatter formatter;
        if (unit == ChronoUnit.HOURS) {
            formatter = DateTimeFormatter.ofPattern("yyyyMMddHH");
        } else if (unit == ChronoUnit.DAYS) {
            formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        } else if (unit == ChronoUnit.MONTHS) {
            formatter = DateTimeFormatter.ofPattern("yyyyMM");
        } else if (unit == ChronoUnit.YEARS) {
            formatter = DateTimeFormatter.ofPattern("yyyy");
        } else {
            throw new AnalysisException(String.format("Unsupported partition unit '%s'", unit));
        }
        return dt.format(formatter);
    }

    private static Optional<String> buildNormalizedPartitionName(Table table,
                                                                 DataCacheSelectStatement statement,
                                                                 ChronoUnit chronoUnit,
                                                                 LocalDateTime lowerBound,
                                                                 LiteralExpr lowerLiteral)
            throws AnalysisException {
        if (table == null) {
            return Optional.empty();
        }
        if (table.isIcebergTable()) {
            return buildIcebergPartitionName((IcebergTable) table, statement, chronoUnit, lowerBound, lowerLiteral);
        }

        return Optional.empty();
    }

    private static Optional<String> buildIcebergPartitionName(IcebergTable table,
                                                              DataCacheSelectStatement statement,
                                                              ChronoUnit chronoUnit,
                                                              LocalDateTime lowerBound,
                                                              LiteralExpr lowerLiteral) {
        PartitionField field = findIcebergPartitionField(table, statement.getPartitionField());
        if (field == null) {
            LOG.warn("Failed to locate iceberg partition field {} on table {}", statement.getPartitionField(),
                    table.getName());
            return Optional.empty();
        }
        String valueString = formatIcebergPartitionValue(transformPartitionFieldType(statement.getPartitionFieldType()),
                chronoUnit, lowerBound, lowerLiteral);
        return Optional.of(PartitionUtil.toHivePartitionName(
                ImmutableList.of(field.name()), ImmutableList.of(valueString)));
    }

    private static PartitionField findIcebergPartitionField(IcebergTable table, String partitionField) {
        PartitionSpec spec = table.getNativeTable().spec();
        Schema schema = table.getNativeTable().schema();
        for (PartitionField field : spec.fields()) {
            if (field.name().equalsIgnoreCase(partitionField)) {
                return field;
            }
            String sourceName = table.getPartitionSourceName(schema, field);
            if (!Strings.isNullOrEmpty(sourceName) && sourceName.equalsIgnoreCase(partitionField)) {
                return field;
            }
        }
        return null;
    }

    private static String formatIcebergPartitionValue(Type type, ChronoUnit chronoUnit,
                                                      LocalDateTime lowerBound, LiteralExpr literal) {
        if (type == Type.DATETIME || type == Type.DATE) {
            if (chronoUnit == ChronoUnit.HOURS) {
                return lowerBound.format(DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"));
            } else if (chronoUnit == ChronoUnit.DAYS) {
                return lowerBound.format(DateUtils.DATE_FORMATTER_UNIX);
            } else if (chronoUnit == ChronoUnit.MONTHS) {
                return lowerBound.format(DateTimeFormatter.ofPattern("yyyy-MM"));
            } else if (chronoUnit == ChronoUnit.YEARS) {
                return lowerBound.format(DateTimeFormatter.ofPattern("yyyy"));
            }
        }
        return literal.getStringValue();
    }

    private static String parsePartitionUnit(String partitionValue) throws AnalysisException {
        String digitsOnly = partitionValue.replaceAll("\\D", "");
        if (digitsOnly.isEmpty()) {
            throw new AnalysisException(String.format(
                    "Partition value '%s' does not contain numeric date information", partitionValue));
        }

        switch (digitsOnly.length()) {
            case 4: {
                return "year";
            }
            case 6: {
                return "month";
            }
            case 8: {
                return "day";
            }
            case 10: {
                return "hour";
            }
            default: {
                throw new AnalysisException(String.format(
                        "Unsupported partition value format '%s'", partitionValue));
            }
        }
    }

    private static LocalDateTime parsePartitionValue(String partitionValue) throws AnalysisException {
        String digitsOnly = partitionValue.replaceAll("\\D", "");
        if (digitsOnly.isEmpty()) {
            throw new AnalysisException(String.format(
                    "Partition value '%s' does not contain numeric date information", partitionValue));
        }
        try {
            switch (digitsOnly.length()) {
                case 4: {
                    int year = Integer.parseInt(digitsOnly);
                    return LocalDateTime.of(year, 1, 1, 0, 0);
                }
                case 6: {
                    int year = Integer.parseInt(digitsOnly.substring(0, 4));
                    int month = Integer.parseInt(digitsOnly.substring(4, 6));
                    return LocalDateTime.of(year, month, 1, 0, 0);
                }
                case 8: {
                    int year = Integer.parseInt(digitsOnly.substring(0, 4));
                    int month = Integer.parseInt(digitsOnly.substring(4, 6));
                    int day = Integer.parseInt(digitsOnly.substring(6, 8));
                    return LocalDateTime.of(year, month, day, 0, 0);
                }
                case 10: {
                    int year = Integer.parseInt(digitsOnly.substring(0, 4));
                    int month = Integer.parseInt(digitsOnly.substring(4, 6));
                    int day = Integer.parseInt(digitsOnly.substring(6, 8));
                    int hour = Integer.parseInt(digitsOnly.substring(8, 10));
                    return LocalDateTime.of(year, month, day, hour, 0);
                }
                default: {
                    throw new AnalysisException(String.format(
                            "Unsupported partition value format '%s'", partitionValue));
                }
            }
        } catch (NumberFormatException | DateTimeException e) {
            throw new AnalysisException(String.format(
                    "Failed to parse partition value '%s'", partitionValue), e);
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
