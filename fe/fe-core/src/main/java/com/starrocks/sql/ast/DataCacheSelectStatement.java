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

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.datacache.DataCacheMetaManager;
import com.starrocks.qe.OriginStatement;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TCacheSelectMode;

import java.util.Map;

public class DataCacheSelectStatement extends DdlStmt {

    public static final String PARTITION_FULL_TABLE = "__FULL_TABLE__";
    private TCacheSelectMode mode;
    private final InsertStmt insertStmt;

    private final Map<String, String> properties;

    // =================================================================================
    // Below properties will set after DataCacheAnalyzer analyze properties
    private boolean isVerbose = false;
    // real catalog of cache select table
    private String catalog = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
    // TODO: 使用Table.getUUID()更好？
    private TableName tableName;
    private Table table;
    private String partition; // eg. p20251001, if partition is empty, cache latest time unit
    private String partitionName; // eg. dt=2025-10-01, use as key to seach iceberg partition meta
    private long partitionVersion; // newest partition version
    private String hashRingSignature;
    private long ttlSeconds = 0;
    private int priority = 0;
    private boolean createByJob = false;
    private String partitionField;
    private String partitionFieldFormat;
    private String partitionUnit;
    private String partitionFieldType; // be the same with properties
    private DataCacheMetaManager.CacheDeleteMode deleteMode = DataCacheMetaManager.CacheDeleteMode.NORMAL;
    private boolean userPredicatePresent = false;
    private boolean fullTableCache = false;
    // =================================================================================

    public DataCacheSelectStatement(TCacheSelectMode mode, InsertStmt insertStmt,
                                    Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.mode = mode;
        this.insertStmt = insertStmt;
        this.properties = properties;
        Preconditions.checkNotNull(properties, "properties can't be null");
        insertStmt.setOrigStmt(new OriginStatement("CACHE " + AstToSQLBuilder.toSQL(insertStmt.getQueryStatement())));
    }

    public Boolean isCacheSelect() {
        return mode == TCacheSelectMode.DEFAULT;
    }

    public Boolean isCacheDelete() {
        return mode == TCacheSelectMode.DELETE;
    }

    public Boolean isCacheDesc() {
        return mode == TCacheSelectMode.DESC;
    }

    public TCacheSelectMode mode() {
        return mode;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    public boolean isVerbose() {
        return isVerbose;
    }

    public void setFullTableCache(boolean fullTableCache) {
        this.fullTableCache = fullTableCache;
    }

    public boolean isFullTableCache() {
        return fullTableCache;
    }

    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    public String getCatalog() {
        return this.catalog;
    }

    public TableName getTableName() {
        return tableName;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }

    public String getPartition() {
        return partition;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public void setPartitionVersion(long partitionVersion) {
        this.partitionVersion = partitionVersion;
    }

    public String getHashRingSignature() {
        return hashRingSignature;
    }

    public void setHashRingSignature(String hashRingSignature) {
        this.hashRingSignature = hashRingSignature;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

    public String getPartitionFieldType() {
        return partitionFieldType;
    }

    public void setPartitionFieldType(String partitionFieldType) {
        this.partitionFieldType = partitionFieldType;
    }

    public String getPartitionFieldFormat() {
        return this.partitionFieldFormat;
    }

    public String getPartitionUnit() {
        return this.partitionUnit;
    }

    public void setPartitionUnit(String partitionUnit) {
        this.partitionUnit = partitionUnit;
    }

    public void setPartitionFieldFormat(String partitionFieldFormat) {
        this.partitionFieldFormat = partitionFieldFormat;
    }

    public DataCacheMetaManager.CacheDeleteMode getDeleteMode() {
        return deleteMode;
    }

    public void setDeleteMode(DataCacheMetaManager.CacheDeleteMode deleteMode) {
        if (deleteMode == null) {
            this.deleteMode = DataCacheMetaManager.CacheDeleteMode.NORMAL;
        } else {
            this.deleteMode = deleteMode;
        }
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public void setTTLSeconds(long ttlSeconds) {
        this.ttlSeconds = ttlSeconds;
    }

    public long getTTLSeconds() {
        return ttlSeconds;
    }

    public boolean isCreateByJob() {
        return createByJob;
    }

    public void setCreateByJob(boolean createByJob) {
        this.createByJob = createByJob;
    }

    public boolean hasUserPredicate() {
        return userPredicatePresent;
    }

    public void setUserPredicatePresent(boolean userPredicatePresent) {
        this.userPredicatePresent = userPredicatePresent;
    }

    public boolean isDeleteModeGc() {
        return DataCacheMetaManager.CacheDeleteMode.GC == deleteMode;
    }

    public String toSQLStringWithoutProperties() {
        StringBuilder sb = new StringBuilder();
        sb.append("CACHE SELECT * FROM ");
        sb.append(tableName.toSql());

        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDataCacheSelectStatement(this, context);
    }
}
