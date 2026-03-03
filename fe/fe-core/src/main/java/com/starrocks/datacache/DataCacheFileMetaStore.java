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

import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Status;
import com.starrocks.common.UserException;
import com.starrocks.common.util.AutoInferUtil;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.mysql.MysqlProto;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TResultBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Store responsible for persisting and querying file cache metadata in an OLAP table.
 */
public class DataCacheFileMetaStore {
    private static final Logger LOG = LogManager.getLogger(DataCacheFileMetaStore.class);

    private final String dbName;
    private final String tableName;

    public DataCacheFileMetaStore(String dbName, String tableName) {
        this.dbName = dbName;
        this.tableName = tableName;
    }

    // Query expired file metas for a partition: history version files minus current version files.
    public List<DataCacheFileMeta> queryExpiredFileMeta(long partitionUid, long partitionVersion,
                                                        String latestHashRingSignature) {
        // Use CTE to build current and history lists then left join to get expired rows.
        String escapedSig = escapeSqlString(latestHashRingSignature);
        String sql = String.format(
                "WITH current_files AS (\n" +
                        "    SELECT file_id, backend_id\n" +
                        "    FROM %s.%s\n" +
                        "    WHERE partition_uid = %d AND partition_version = %d "
                        + "AND hash_ring_signature = '%s'\n" +
                        "),\n" +
                        "history_files AS (\n" +
                        "    SELECT table_id, partition_uid, file_id, backend_id, file_path, file_size_bytes,\n" +
                        "           offset, length, partition_version, modification_time, file_type, is_relative_path,\n" +
                        "           hash_ring_signature\n" +
                        "    FROM %s.%s\n" +
                        "    WHERE partition_uid = %d AND (partition_version <> %d "
                        + "OR hash_ring_signature <> '%s')\n" +
                        ")\n" +
                        "SELECT h.table_id, h.partition_uid, h.file_id, h.backend_id, h.file_path, h.file_size_bytes,\n" +
                        "       h.offset, h.length, h.partition_version, h.modification_time, h.file_type,\n" +
                        "       h.is_relative_path, h.hash_ring_signature\n" +
                        "FROM history_files h\n" +
                        "LEFT JOIN current_files c ON h.file_id = c.file_id AND h.backend_id = c.backend_id\n" +
                        "WHERE c.file_id IS NULL",
                quoteIdentifier(dbName), quoteIdentifier(tableName), partitionUid, partitionVersion, escapedSig,
                quoteIdentifier(dbName), quoteIdentifier(tableName), partitionUid, partitionVersion, escapedSig);

        List<List<String>> rows = executeInternalSelect(sql, dbName);
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<DataCacheFileMeta> result = new ArrayList<>();
        for (List<String> row : rows) {
            try {
                long tableId = Long.parseLong(row.get(0));
                long parsedPartitionUid = Long.parseLong(row.get(1));
                long fileId = Long.parseLong(row.get(2));
                long backendId = Long.parseLong(row.get(3));
                String filePath = row.get(4);
                long fileSizeBytes = Long.parseLong(row.get(5));
                long offset = Long.parseLong(row.get(6));
                long length = Long.parseLong(row.get(7));
                long version = Long.parseLong(row.get(8));
                long modificationTime = Long.parseLong(row.get(9));
                String fileType = row.get(10);
                String isRelativePathStr = row.get(11);
                String hashRingSignature = row.size() > 12 ? row.get(12) : "";
                boolean isRelativePath = !("0".equals(isRelativePathStr) || "false".equalsIgnoreCase(isRelativePathStr));
                result.add(new DataCacheFileMeta(parsedPartitionUid, fileId, backendId, filePath,
                        fileSizeBytes, offset, length, tableId, version, modificationTime, fileType, isRelativePath,
                        hashRingSignature));
            } catch (Exception e) {
                LOG.warn("Failed to parse expired file_cache_meta row: {}", row, e);
            }
        }
        return result;
    }

    public void ensureTable() throws UserException {
        if (FeConstants.runningUnitTest) {
            return;
        }
        String createDbSql = "CREATE DATABASE IF NOT EXISTS " + quoteIdentifier(dbName);
        executeInternalSql(createDbSql, null);

        int replicationNum = AutoInferUtil.calDefaultReplicationNum();
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" (")
                .append("`table_id` BIGINT NOT NULL,")
                .append("`partition_uid` BIGINT NOT NULL,")
                .append("`file_id` BIGINT NOT NULL,")
                .append("`backend_id` BIGINT NOT NULL,")
                .append("`file_path` STRING NOT NULL,")
                .append("`file_size_bytes` BIGINT NOT NULL,")
                .append("`offset` BIGINT NOT NULL,")
                .append("`length` BIGINT NOT NULL,")
                .append("`partition_version` BIGINT NOT NULL,")
                .append("`modification_time` BIGINT NOT NULL,")
                .append("`file_type` STRING NOT NULL,")
                .append("`is_relative_path` BOOLEAN NOT NULL,")
                .append("`hash_ring_signature` STRING NOT NULL")
                .append(") ENGINE=OLAP ")
                .append("DUPLICATE KEY(`table_id`, `partition_uid`, `file_id`, `backend_id`) ")
                .append("DISTRIBUTED BY HASH(`partition_uid`) BUCKETS 10 ")
                .append("PROPERTIES(\"replication_num\" = \"")
                .append(replicationNum)
                .append("\")");
        executeInternalSql(sb.toString(), dbName);
    }

    public void insertFileMeta(DataCacheFileMeta entry) {
        if (FeConstants.runningUnitTest) {
            return;
        }
        String sql = buildInsertFileMetaSql(entry);
        executeInternalSql(sql, dbName);
    }

    public void deleteForPartition(long partitionUid) {
        if (FeConstants.runningUnitTest) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `partition_uid` = ")
                .append(partitionUid);
        executeInternalSql(sb.toString(), dbName);
    }

    public void deleteExpiredFileMeta(long partitionUid, long version, String hashRingSignature) {
        if (FeConstants.runningUnitTest) {
            return;
        }
        String escapedSignature = escapeSqlString(hashRingSignature);
        // 1) remove rows whose version is different
        StringBuilder sbVersion = new StringBuilder();
        sbVersion.append("DELETE FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `partition_uid` = ")
                .append(partitionUid)
                .append(" AND `partition_version` <> ")
                .append(version);

        // 2) remove rows with same version but different hash ring signature
        StringBuilder sbSignature = new StringBuilder();
        sbSignature.append("DELETE FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `partition_uid` = ")
                .append(partitionUid)
                .append(" AND `hash_ring_signature` <> '")
                .append(escapedSignature)
                .append("'");

        long affectedVersion = executeInternalSqlWithResult(sbVersion.toString(), dbName);
        long affectedSignature = executeInternalSqlWithResult(sbSignature.toString(), dbName);
        LOG.info("Deleted expired file meta for partition {}: version={}, hashRingSignature={}, "
                        + "affectedRowsVersion={}, affectedRowsSignature={}, total={}",
                partitionUid, version, hashRingSignature, affectedVersion, affectedSignature,
                affectedVersion + affectedSignature);
    }

    public List<DataCacheFileMeta> queryFileMeta(long partitionUid) {
        if (FeConstants.runningUnitTest) {
            return Collections.emptyList();
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT `table_id`, `partition_uid`, `file_id`, `backend_id`, ")
                .append("`file_path`, `file_size_bytes`, `offset`, `length`, `partition_version`, ")
                .append("`modification_time`, `file_type`, `is_relative_path`, `hash_ring_signature` ")
                .append("FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `partition_uid` = ")
                .append(partitionUid);
        List<List<String>> rows = executeInternalSelect(sb.toString(), dbName);
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<DataCacheFileMeta> result = new ArrayList<>();
        for (List<String> row : rows) {
            try {
                long parsedTableId = Long.parseLong(row.get(0));
                long parsedPartitionUid = Long.parseLong(row.get(1));
                long fileId = Long.parseLong(row.get(2));
                long backendId = Long.parseLong(row.get(3));
                String filePath = row.get(4);
                long fileSizeBytes = Long.parseLong(row.get(5));
                long offset = Long.parseLong(row.get(6));
                long length = Long.parseLong(row.get(7));
                long partitionVersion = Long.parseLong(row.get(8));
                long modificationTime = Long.parseLong(row.get(9));
                String fileType = row.get(10);
                String isRelativePathStr = row.get(11);
                String hashRingSignature = row.size() > 12 ? row.get(12) : "";
                boolean isRelativePath = !("0".equals(isRelativePathStr) || "false".equalsIgnoreCase(isRelativePathStr));
                result.add(new DataCacheFileMeta(parsedPartitionUid, fileId, backendId, filePath,
                        fileSizeBytes, offset, length, parsedTableId,
                        partitionVersion, modificationTime, fileType, isRelativePath, hashRingSignature));
            } catch (Exception e) {
                LOG.warn("Failed to parse file_cache_meta row: {}", row, e);
            }
        }
        return result;
    }

    public long[] queryExpiredStats(long tableId, long partitionUid, long currentVersion, String hashRingSignature) {
        String escapedSignature = escapeSqlString(hashRingSignature);

        // Version mismatch
        StringBuilder versionSql = new StringBuilder();
        versionSql.append("SELECT COUNT(*), IFNULL(SUM(`file_size_bytes`), 0) FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `table_id` = ")
                .append(tableId)
                .append(" AND `partition_uid` = ")
                .append(partitionUid)
                .append(" AND `partition_version` <> ")
                .append(currentVersion);

        // Same version, different hash ring signature
        StringBuilder signatureSql = new StringBuilder();
        signatureSql.append("SELECT COUNT(*), IFNULL(SUM(`file_size_bytes`), 0) FROM ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" WHERE `table_id` = ")
                .append(tableId)
                .append(" AND `partition_uid` = ")
                .append(partitionUid)
                .append(" AND `partition_version` = ")
                .append(currentVersion)
                .append(" AND `hash_ring_signature` <> '")
                .append(escapedSignature)
                .append("'");

        long[] versionStats = fetchCountAndBytes(versionSql.toString(), partitionUid);
        long[] signatureStats = fetchCountAndBytes(signatureSql.toString(), partitionUid);
        return new long[] {versionStats[0] + signatureStats[0], versionStats[1] + signatureStats[1]};
    }

    private long[] fetchCountAndBytes(String sql, long partitionUid) {
        List<List<String>> rows = executeInternalSelect(sql, dbName);
        if (rows == null || rows.isEmpty()) {
            return new long[] {0L, 0L};
        }
        List<String> row = rows.get(0);
        try {
            long expiredFiles = Long.parseLong(row.get(0));
            long expiredBytes = Long.parseLong(row.get(1));
            return new long[] {expiredFiles, expiredBytes};
        } catch (Exception e) {
            LOG.warn("Failed to parse expired stats from file_cache_meta for partitionUid {} sql {}", partitionUid, sql, e);
            return new long[] {0L, 0L};
        }
    }

    private String buildInsertFileMetaSql(DataCacheFileMeta entry) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(quoteIdentifier(dbName))
                .append(".")
                .append(quoteIdentifier(tableName))
                .append(" (`table_id`, `partition_uid`, `file_id`, `backend_id`, ")
                .append("`file_path`, `file_size_bytes`, `offset`, `length`, `partition_version`, ")
                .append("`modification_time`, `file_type`, `is_relative_path`, `hash_ring_signature`)")
                .append(" VALUES (")
                .append(entry.getTableId()).append(",")
                .append(entry.getPartitionUid()).append(",")
                .append(entry.getFileId()).append(",")
                .append(entry.getBackendId()).append(",")
                .append("'").append(escapeSqlString(entry.getFilePath())).append("',")
                .append(entry.getFileSizeBytes()).append(",")
                .append(entry.getOffset()).append(",")
                .append(entry.getLength()).append(",")
                .append(entry.getPartitionVersion()).append(",")
                .append(entry.getModificationTime()).append(",")
                .append("'").append(escapeSqlString(entry.getFileType())).append("',")
                .append(entry.isRelativePath() ? 1 : 0).append(",")
                .append("'").append(escapeSqlString(entry.getHashRingSignature())).append("'")
                .append(")");
        return sb.toString();
    }

    private String quoteIdentifier(String identifier) {
        StringBuilder sb = new StringBuilder();
        sb.append('`');
        for (int i = 0; i < identifier.length(); i++) {
            char ch = identifier.charAt(i);
            if (ch == '`') {
                sb.append("``");
            } else {
                sb.append(ch);
            }
        }
        sb.append('`');
        return sb.toString();
    }

    private String escapeSqlString(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("'", "''");
    }

    private long executeInternalSqlWithResult(String sql, String dbName) {
        ConnectContext parent = ConnectContext.get();
        ConnectContext context = ConnectContext.buildInner();
        try {
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(UserIdentity.ROOT);
            context.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            if (dbName != null) {
                context.setDatabase(dbName);
            }
            context.setCurrentWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            context.setExecutionId(UUIDUtil.genTUniqueId());
            context.setQueryId(UUIDUtil.genUUID());
            context.setThreadLocalInfo();

            StatementBase parsedStmt = (StatementBase) SqlParser.parse(sql,
                    context.getSessionVariable()).get(0);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            executor.execute();
            return context.getState() == null ? 0L : context.getState().getAffectedRows();
        } catch (Exception e) {
            LOG.warn("Failed to execute internal SQL: {}", sql, e);
            return -1L;
        } finally {
            if (parent != null) {
                parent.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
    }

    private void executeInternalSql(String sql, String dbName) {
        executeInternalSqlWithResult(sql, dbName);
    }

    private List<List<String>> executeInternalSelect(String sql, String dbName) {
        if (FeConstants.runningUnitTest) {
            return Collections.emptyList();
        }
        ConnectContext parent = ConnectContext.get();
        ConnectContext context = ConnectContext.buildInner();
        try {
            context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
            context.setCurrentUserIdentity(UserIdentity.ROOT);
            context.setCurrentRoleIds(UserIdentity.ROOT);
            context.setCurrentCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
            if (dbName != null) {
                context.setDatabase(dbName);
            }
            context.setCurrentWarehouse(WarehouseManager.DEFAULT_WAREHOUSE_NAME);
            context.setExecutionId(UUIDUtil.genTUniqueId());
            context.setQueryId(UUIDUtil.genUUID());
            context.setThreadLocalInfo();

            StatementBase parsedStmt = (StatementBase) SqlParser.parse(sql,
                    context.getSessionVariable()).get(0);
            ExecPlan execPlan = StatementPlanner.plan(parsedStmt, context);
            StmtExecutor executor = StmtExecutor.newInternalExecutor(context, parsedStmt);
            var resultWithStatus = executor.executeStmtWithExecPlan(context, execPlan);
            Status status = resultWithStatus.second;
            if (status == null || !status.ok()) {
                LOG.warn("Internal SELECT failed with status: {}", status);
                return Collections.emptyList();
            }
            return decodeResultBatches(resultWithStatus.first);
        } catch (Exception e) {
            LOG.warn("Failed to execute internal SELECT SQL: {}", sql, e);
            return Collections.emptyList();
        } finally {
            if (parent != null) {
                parent.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
    }

    /**
     * Decode TResultBatch rows into a list of string rows using MySQL length-encoded strings.
     * This is similar to StatisticExecutor/RepoExecutor patterns where result batches are consumed directly.
     */
    private List<List<String>> decodeResultBatches(List<TResultBatch> batches) {
        List<List<String>> result = new ArrayList<>();
        if (batches == null) {
            return result;
        }
        for (TResultBatch batch : batches) {
            if (batch == null || batch.getRows() == null) {
                continue;
            }
            for (ByteBuffer buf : batch.getRows()) {
                if (buf == null) {
                    continue;
                }
                ByteBuffer rowBuf = buf.slice();
                List<String> row = new ArrayList<>();
                while (rowBuf.hasRemaining()) {
                    try {
                        byte[] cell = MysqlProto.readLenEncodedString(rowBuf);
                        row.add(cell == null ? null : new String(cell, StandardCharsets.UTF_8));
                    } catch (NullPointerException npe) {
                        // MySQL protocol encodes NULL as length=251
                        row.add(null);
                    }
                }
                result.add(row);
            }
        }
        return result;
    }
}
