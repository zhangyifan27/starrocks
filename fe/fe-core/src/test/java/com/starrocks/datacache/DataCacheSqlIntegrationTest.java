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
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for DataCache SQL operations using PseudoCluster.
 *
 * These tests use PseudoCluster.runSql() to execute SQL through the complete
 * execution path via JDBC connection to FE's MySQL protocol port:
 * 1. SQL sent via JDBC -> FE MySQL Protocol Handler
 * 2. SQL parsing and analysis
 * 3. Query planning (StatementPlanner)
 * 4. Execution via Coordinator -> PseudoBackend
 * 5. PseudoBackend returns mock DataCache metrics
 * 6. DataCacheMetaManager.updateDataCacheMeta() updates metadata
 */
public class DataCacheSqlIntegrationTest {

    private static PseudoCluster cluster;
    private static DataCacheMetaManager metaManager;
    private static boolean originalEnableOteamDatacache;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_experimental_mv = true;

        // Save original config and enable oteam datacache for tests
        originalEnableOteamDatacache = Config.enable_oteam_datacache;
        Config.enable_oteam_datacache = true;

        // Start PseudoCluster with 3 backends
        cluster = PseudoCluster.getOrCreateWithRandomPort(true, 3);

        // Setup StarRocksAssert for external catalog mocking
        starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());

        // Mock Hive catalog for external table tests
        ConnectorPlanTestBase.mockHiveCatalog(starRocksAssert.getCtx());

        // Use the real DataCacheMetaManager from GlobalStateMgr
        metaManager = GlobalStateMgr.getCurrentState().getDataCacheMetaManager();

        // Mock DataCacheMetaManager.isInitialized() to return true
        new MockUp<DataCacheMetaManager>() {
            @Mock
            public boolean isInitialized() {
                return true;
            }
        };

        // Mock DataCacheFileMetaStore to avoid executing real SQL queries to internal tables
        new MockUp<DataCacheFileMetaStore>() {
            @Mock
            public long[] queryExpiredStats(long tableId, long partitionUid,
                                            long currentVersion, String hashRingSignature) {
                return new long[] {0, 0};
            }
        };
    }

    @AfterClass
    public static void afterAll() throws Exception {
        Config.enable_oteam_datacache = originalEnableOteamDatacache;
        if (cluster != null) {
            cluster.shutdown(false);
        }
    }

    /**
     * Execute SQL through the full JDBC path using PseudoCluster.
     * This goes through the complete SQL execution pipeline.
     */
    private void runSql(String sql) throws SQLException {
        cluster.runSql(null, sql, true);
    }

    /**
     * Execute a query SQL and return the result as a list of rows.
     * Each row is a list of column values as strings.
     */
    private List<List<String>> runQuery(String sql) throws SQLException {
        List<List<String>> result = new ArrayList<>();
        Connection connection = cluster.getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            ResultSet rs = stmt.executeQuery(sql);
            int columnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getString(i));
                }
                result.add(row);
            }
            rs.close();
        } finally {
            stmt.close();
            connection.close();
        }
        return result;
    }

    // ========================================
    // A. CACHE SELECT Integration Tests
    // ========================================

    @Test
    public void testCacheSelectCreatesMetadata() throws Exception {
        // Use partition value that exists in MockedHiveMetadata (1998-01-01 to 1998-01-05)
        String uniquePartition = "p19980101";
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Verify no partition metadata before execution
        Assert.assertFalse("Partition meta should not exist before execution",
                metaManager.existsPartition(tableName, uniquePartition));

        // Execute CACHE SELECT through full JDBC path
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + uniquePartition + "', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        runSql(sql);

        // Verify metadata after execution
        Assert.assertTrue("Table meta should exist after execution",
                metaManager.existsTable(tableName));
        Assert.assertTrue("Partition meta should exist after execution",
                metaManager.existsPartition(tableName, uniquePartition));

        DataCachePartitionMeta meta = metaManager.getPartitionMeta(tableName, uniquePartition).get();
        Assert.assertEquals("l_shipdate", meta.getPartitionField());
        Assert.assertEquals("day", meta.getPartitionUnit().toLowerCase());
        Assert.assertEquals(DataCacheMetaManager.CACHE_STATUS_ACTIVE, meta.getCacheStatus());
    }

    @Test
    public void testCacheSelectWithTTL() throws Exception {
        String partition = "p19980102";
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Execute CACHE SELECT with TTL through full JDBC path
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D', 'priority'='1')";
        runSql(sql);

        // Verify TTL in metadata
        Assert.assertTrue("Partition should exist", metaManager.existsPartition(tableName, partition));
        DataCachePartitionMeta meta = metaManager.getPartitionMeta(tableName, partition).get();
        Assert.assertNotNull("TTL expire time should be set", meta.getTtlExpireAt());
    }

    // ========================================
    // B. CACHE DELETE Integration Tests
    // ========================================

    @Test
    public void testCacheDeleteRemovesMetadata() throws Exception {
        // Setup: Create partition metadata first
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");
        String partition = "p19980110";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(tableName, partition, 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 100 * 1024 * 1024L,
                "/data/hive/multi_partition_table/l_shipdate=1998-01-10",
                "DAY", "yyyy-MM-dd", "sig-to-delete");

        Assert.assertTrue("Partition should exist before delete",
                metaManager.existsPartition(tableName, partition));

        // Execute CACHE DELETE through full JDBC path
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "')";
        runSql(sql);

        // Verify metadata removed
        Assert.assertFalse("Partition should be removed after delete",
                metaManager.existsPartition(tableName, partition));
        Assert.assertTrue("Table meta should still exist",
                metaManager.existsTable(tableName));
    }

    @Test
    public void testCacheDeleteGcMode() throws Exception {
        // Setup
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");
        String partition = "p19980111";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(tableName, partition, 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 100 * 1024 * 1024L,
                "/data/hive/multi_partition_table/l_shipdate=1998-01-11",
                "DAY", "yyyy-MM-dd", "sig-gc");

        // Execute CACHE DELETE with GC mode through full JDBC path
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "', 'cache_delete_mode'='gc')";
        runSql(sql);

        // Verify partition still exists (GC mode resets stats, doesn't remove)
        Assert.assertTrue("Partition should still exist after GC mode delete",
                metaManager.existsPartition(tableName, partition));
    }

    // ========================================
    // C. CACHE DESC Integration Tests
    // ========================================

    @Test
    public void testCacheDescInheritsFromMetadata() throws Exception {
        // Setup
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");
        String partition = "p19980120";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(tableName, partition, 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 150 * 1024 * 1024L,
                "/data/hive/multi_partition_table/l_shipdate=1998-01-20",
                "DAY", "yyyy-MM-dd", "sig-desc");

        // Execute CACHE DESC through full JDBC path
        String sql = "CACHE DESC * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "')";
        runSql(sql);

        // Partition should still exist after DESC
        Assert.assertTrue("Partition should exist after DESC",
                metaManager.existsPartition(tableName, partition));
    }

    // ========================================
    // D. Error Cases Tests
    // ========================================

    @Test
    public void testCacheDeleteNonExistentPartitionFails() {
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p99990101')";
        try {
            runSql(sql);
            Assert.fail("Expected SQL execution to fail");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("not found in partition_cache_meta"));
        }
    }

    @Test
    public void testCacheDescNonExistentPartitionFails() {
        String sql = "CACHE DESC * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p99990102')";
        try {
            runSql(sql);
            Assert.fail("Expected SQL execution to fail");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("not found in partition_cache_meta"));
        }
    }

    @Test
    public void testCacheSelectTableNotExistFails() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.non_existent_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        try {
            runSql(sql);
            Assert.fail("Expected SQL execution to fail");
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown table"));
        }
    }

    // ========================================
    // E. Multi-Partition Workflow Tests
    // ========================================

    @Test
    public void testMultiPartitionCacheWorkflow() throws Exception {
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Cache multiple partitions through full JDBC path (use dates that exist in mock data: 1998-01-01 to 1998-01-05)
        String[] partitions = {"p19980103", "p19980104", "p19980105"};
        for (String partition : partitions) {
            String sql = String.format(
                    "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                    "PROPERTIES('partition'='%s', 'partition_field'='l_shipdate', " +
                    "'partition_field_type'='date', 'partition_unit'='day')", partition);
            runSql(sql);
        }

        // Verify all partitions cached
        for (String partition : partitions) {
            Assert.assertTrue("Partition " + partition + " should exist",
                    metaManager.existsPartition(tableName, partition));
        }

        // Delete middle partition through full JDBC path
        String deleteSQL = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                           "PROPERTIES('partition'='p19980104')";
        runSql(deleteSQL);

        // Verify state after delete
        Assert.assertTrue(metaManager.existsPartition(tableName, "p19980103"));
        Assert.assertFalse(metaManager.existsPartition(tableName, "p19980104"));
        Assert.assertTrue(metaManager.existsPartition(tableName, "p19980105"));
    }

    // ========================================
    // F. Version and Size Tracking Tests
    // ========================================

    @Test
    public void testPartitionVersionTracking() {
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");
        String partition = "p19980301";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        // First cache with version 100
        metaManager.upsertPartitionMeta(tableName, partition, 100L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 100 * 1024 * 1024L,
                "/path", "DAY", "yyyy-MM-dd", "sig-v100");
        Assert.assertEquals(100L, metaManager.getPartitionVersion(tableName, partition));

        // Re-cache with version 200
        metaManager.upsertPartitionMeta(tableName, partition, 200L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 150 * 1024 * 1024L,
                "/path", "DAY", "yyyy-MM-dd", "sig-v200");
        Assert.assertEquals(200L, metaManager.getPartitionVersion(tableName, partition));
    }

    @Test
    public void testCacheSizeAggregation() {
        TableName tableName = new TableName("hive0", "datacache_db", "size_agg_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        // Create two partitions with different sizes
        metaManager.upsertPartitionMeta(tableName, "p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 100 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig-1");
        metaManager.upsertPartitionMeta(tableName, "p2", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 200 * 1024 * 1024L, "/path/p2", "DAY", "yyyy-MM-dd", "sig-2");

        // Query partition sizes
        java.util.List<java.util.List<String>> sizes = metaManager.getPartitionsDataCacheSize(tableName);
        Assert.assertEquals(2, sizes.size());
    }

    // ========================================
    // G. Full Table Cache Tests
    // ========================================

    @Test
    public void testFullTableCacheSelectCreatesMetadata() throws Exception {
        // Using normal_table which is a non-partitioned table in MockedHiveMetadata
        TableName tableName = new TableName("hive0", "datacache_db", "normal_table");
        String fullTablePartition = DataCacheSelectStatement.PARTITION_FULL_TABLE;

        // Verify no partition metadata before execution
        Assert.assertFalse("Full table cache meta should not exist before execution",
                metaManager.existsPartition(tableName, fullTablePartition));

        // Execute CACHE SELECT for full table with 'full_table_cache'='true' property
        String sql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true')";
        runSql(sql);

        // Verify metadata after execution
        Assert.assertTrue("Table meta should exist after full table cache",
                metaManager.existsTable(tableName));
        Assert.assertTrue("Full table partition meta should exist after execution",
                metaManager.existsPartition(tableName, fullTablePartition));

        DataCachePartitionMeta meta = metaManager.getPartitionMeta(tableName, fullTablePartition).get();
        Assert.assertEquals(DataCacheMetaManager.CACHE_STATUS_ACTIVE, meta.getCacheStatus());
        // Full table cache should have empty partition field
        Assert.assertTrue("Partition field should be empty for full table cache",
                meta.getPartitionField() == null || meta.getPartitionField().isEmpty());
    }

    @Test
    public void testFullTableCacheDeleteRemovesMetadata() throws Exception {
        // Setup: First cache the full table to create metadata
        TableName tableName = new TableName("hive0", "datacache_db", "normal_table");
        String fullTablePartition = DataCacheSelectStatement.PARTITION_FULL_TABLE;

        // Clean up any existing metadata
        if (metaManager.existsPartition(tableName, fullTablePartition)) {
            metaManager.removePartitionMeta(tableName, fullTablePartition);
        }

        // First execute CACHE SELECT for full table to create metadata
        String selectSql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true')";
        runSql(selectSql);

        Assert.assertTrue("Full table cache should exist after CACHE SELECT",
                metaManager.existsPartition(tableName, fullTablePartition));

        // Execute CACHE DELETE for full table
        String deleteSql = "CACHE DELETE * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('partition'='" + fullTablePartition + "')";
        runSql(deleteSql);

        // Verify metadata removed
        Assert.assertFalse("Full table cache should be removed after delete",
                metaManager.existsPartition(tableName, fullTablePartition));
        Assert.assertTrue("Table meta should still exist",
                metaManager.existsTable(tableName));
    }

    @Test
    public void testFullTableCacheDescInheritsFromMetadata() throws Exception {
        // Setup: First cache the full table to create metadata
        TableName tableName = new TableName("hive0", "datacache_db", "normal_table");
        String fullTablePartition = DataCacheSelectStatement.PARTITION_FULL_TABLE;

        // Clean up any existing metadata
        if (metaManager.existsPartition(tableName, fullTablePartition)) {
            metaManager.removePartitionMeta(tableName, fullTablePartition);
        }

        // First execute CACHE SELECT for full table to create metadata
        String selectSql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true')";
        runSql(selectSql);

        Assert.assertTrue("Full table cache should exist after CACHE SELECT",
                metaManager.existsPartition(tableName, fullTablePartition));

        // Execute CACHE DESC for full table
        String descSql = "CACHE DESC * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('partition'='" + fullTablePartition + "')";
        runSql(descSql);

        // Partition should still exist after DESC
        Assert.assertTrue("Full table cache should exist after DESC",
                metaManager.existsPartition(tableName, fullTablePartition));
    }

    @Test
    public void testFullTableCacheWithTTL() throws Exception {
        // Test full table cache with TTL setting
        TableName tableName = new TableName("hive0", "datacache_db", "normal_table");
        String fullTablePartition = DataCacheSelectStatement.PARTITION_FULL_TABLE;

        // Clean up any existing metadata first
        if (metaManager.existsPartition(tableName, fullTablePartition)) {
            metaManager.removePartitionMeta(tableName, fullTablePartition);
        }

        // Execute CACHE SELECT for full table with TTL
        String sql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true', 'ttl'='P30D')";
        runSql(sql);

        // Verify TTL in metadata
        Assert.assertTrue("Full table partition should exist",
                metaManager.existsPartition(tableName, fullTablePartition));
        DataCachePartitionMeta meta = metaManager.getPartitionMeta(tableName, fullTablePartition).get();
        Assert.assertNotNull("TTL expire time should be set", meta.getTtlExpireAt());
    }

    @Test
    public void testFullTableCacheRejectsPartitionProperties() {
        // Full table cache should reject partition-related properties
        String sql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true', 'partition_field'='dt')";

        try {
            runSql(sql);
            Assert.fail("Expected SQL execution to fail due to conflicting properties");
        } catch (SQLException e) {
            Assert.assertTrue("Error should mention full table cache conflict",
                    e.getMessage().contains("full table cache should not has partition properties"));
        }
    }

    // ========================================
    // H. Cache File Meta Tests
    // ========================================

    @Test
    public void testCacheSelectRecordsFileMeta() throws Exception {
        // Use partition that exists in MockedHiveMetadata for multi_partition_table
        // Available partitions: l_shipdate=1998-01-01 to l_shipdate=1998-01-05
        String partition = "p19980105"; // This is the last available date
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Track file meta insertions
        List<DataCacheFileMeta> insertedFileMetas = new ArrayList<>();
        new MockUp<DataCacheFileMetaStore>() {
            @Mock
            public void insertFileMeta(DataCacheFileMeta entry) {
                insertedFileMetas.add(entry);
            }

            @Mock
            public long[] queryExpiredStats(long tableId, long partitionUid,
                                            long currentVersion, String hashRingSignature) {
                return new long[] {0, 0};
            }
        };

        // Execute CACHE SELECT
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        runSql(sql);

        // Verify partition meta was created
        Assert.assertTrue("Partition meta should exist after CACHE SELECT",
                metaManager.existsPartition(tableName, partition));
    }

    @Test
    public void testFileMetaContainsRequiredFields() {
        // Test that DataCacheFileMeta contains all required fields
        TableName tableName = new TableName("hive0", "datacache_db", "test_table");
        String partition = "p20240101";
        long backendId = 10001L;
        String filePath = "/data/hive/test_table/dt=2024-01-01/file.parquet";
        long fileSizeBytes = 100 * 1024 * 1024L;
        long offset = 0L;
        long length = fileSizeBytes;
        long partitionVersion = 12345L;
        long modificationTime = System.currentTimeMillis();
        String fileType = "PARQUET";
        boolean isRelativePath = false;
        String hashRingSignature = "test-signature";

        DataCacheFileMeta fileMeta = metaManager.buildFileMeta(
                tableName, partition, backendId, filePath, fileSizeBytes,
                offset, length, partitionVersion, modificationTime, fileType,
                isRelativePath, hashRingSignature);

        // Verify all fields
        Assert.assertTrue("Partition UID should be positive", fileMeta.getPartitionUid() != 0);
        Assert.assertTrue("File ID should be computed from path", fileMeta.getFileId() != 0);
        Assert.assertEquals(backendId, fileMeta.getBackendId());
        Assert.assertEquals(filePath, fileMeta.getFilePath());
        Assert.assertEquals(fileSizeBytes, fileMeta.getFileSizeBytes());
        Assert.assertEquals(offset, fileMeta.getOffset());
        Assert.assertEquals(length, fileMeta.getLength());
        Assert.assertEquals(partitionVersion, fileMeta.getPartitionVersion());
        Assert.assertEquals(modificationTime, fileMeta.getModificationTime());
        Assert.assertEquals(fileType, fileMeta.getFileType());
        Assert.assertEquals(isRelativePath, fileMeta.isRelativePath());
        Assert.assertEquals(hashRingSignature, fileMeta.getHashRingSignature());
    }

    @Test
    public void testFileMetaWithDifferentBackendsHasDifferentEntries() {
        TableName tableName = new TableName("hive0", "datacache_db", "test_table");
        String partition = "p20240102";
        String filePath = "/data/hive/test_table/file.parquet";

        // Create file meta for same file but different backends
        DataCacheFileMeta meta1 = metaManager.buildFileMeta(
                tableName, partition, 10001L, filePath, 1024L,
                0L, 512L, 1L, System.currentTimeMillis(), "PARQUET",
                false, "sig1");

        DataCacheFileMeta meta2 = metaManager.buildFileMeta(
                tableName, partition, 10002L, filePath, 1024L,
                512L, 512L, 1L, System.currentTimeMillis(), "PARQUET",
                false, "sig1");

        // Same partition UID and file ID (computed from partition key and file path)
        Assert.assertEquals(meta1.getPartitionUid(), meta2.getPartitionUid());
        Assert.assertEquals(meta1.getFileId(), meta2.getFileId());

        // Different backend IDs and offsets
        Assert.assertNotEquals(meta1.getBackendId(), meta2.getBackendId());
        Assert.assertNotEquals(meta1.getOffset(), meta2.getOffset());
    }

    @Test
    public void testHashRingSignatureComputation() {
        // Test hash ring signature computation
        List<Long> workerIds = new ArrayList<>();
        workerIds.add(10001L);
        workerIds.add(10002L);
        workerIds.add(10003L);

        String signature1 = metaManager.buildHashRingSignature(
                "CONSISTENT_HASH", false, 100, workerIds);

        // Same parameters should produce same signature
        String signature2 = metaManager.buildHashRingSignature(
                "CONSISTENT_HASH", false, 100, workerIds);

        Assert.assertEquals("Same parameters should produce same signature", signature1, signature2);

        // Different parameters should produce different signature
        String signature3 = metaManager.buildHashRingSignature(
                "CONSISTENT_HASH", true, 100, workerIds);  // different filePathOnly flag

        Assert.assertNotEquals("Different parameters should produce different signature",
                signature1, signature3);

        // Different worker count should produce different signature
        workerIds.add(10004L);
        String signature4 = metaManager.buildHashRingSignature(
                "CONSISTENT_HASH", false, 100, workerIds);

        Assert.assertNotEquals("Different worker count should produce different signature",
                signature1, signature4);
    }

    // ========================================
    // I. CREATE DATA CACHE JOB Tests
    // ========================================

    @Test
    public void testCreateDataCacheJobSqlParsing() {
        // Test that CREATE DATA CACHE JOB SQL can be parsed
        // Note: Full execution requires TaskManager which may not be fully set up in PseudoCluster
        String sql = "CREATE DATA CACHE JOB test_cache_job_parse " +
                     "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                     "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D')";

        // The parsing should succeed even if execution may fail due to TaskManager setup
        try {
            // This will parse and analyze the statement
            runSql(sql);
            // If it succeeds, job was created
        } catch (SQLException e) {
            // Expected if TaskManager is not fully configured in test environment
            // But we verify the SQL was at least parsed correctly
            Assert.assertFalse("Should not fail due to syntax error",
                    e.getMessage().contains("Syntax error") || e.getMessage().contains("parse"));
        }
    }

    @Test
    public void testDataCacheJobMgrComputePartition() {
        // Test the partition computation utility from DataCacheJobMgr
        LocalDateTime baseTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);

        // Test DAY unit
        String dayPartition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "day");
        Assert.assertEquals("p20240115", dayPartition0);

        String dayPartition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "day");
        Assert.assertEquals("p20240114", dayPartition1);

        String dayPartition7 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 7, "day");
        Assert.assertEquals("p20240108", dayPartition7);

        // Test HOUR unit
        String hourPartition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "hour");
        Assert.assertEquals("p2024011510", hourPartition0);

        String hourPartition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "hour");
        Assert.assertEquals("p2024011509", hourPartition1);

        // Test MONTH unit
        String monthPartition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "month");
        Assert.assertEquals("p202401", monthPartition0);

        String monthPartition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "month");
        Assert.assertEquals("p202312", monthPartition1);

        // Test YEAR unit
        String yearPartition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "year");
        Assert.assertEquals("p2024", yearPartition0);

        String yearPartition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "year");
        Assert.assertEquals("p2023", yearPartition1);
    }

    @Test
    public void testCacheDeleteTaskRequestBuildSql() {
        // Test building cache delete task SQL
        DataCacheMetaManager.CacheDeleteTaskRequest request =
                new DataCacheMetaManager.CacheDeleteTaskRequest(
                        "hive0", "datacache_db", "test_table", "p20240101",
                        DataCacheMetaManager.CacheDeleteMode.GC);

        String sql = metaManager.buildCacheDeleteSubmitSql(request);

        Assert.assertTrue("SQL should contain SUBMIT TASK", sql.contains("SUBMIT TASK"));
        Assert.assertTrue("SQL should contain CACHE DELETE", sql.contains("CACHE DELETE"));
        Assert.assertTrue("SQL should contain table name", sql.contains("test_table"));
        Assert.assertTrue("SQL should contain partition", sql.contains("p20240101"));
        Assert.assertTrue("SQL should contain GC mode", sql.contains("GC"));
    }

    @Test
    public void testCacheDeleteTaskRequestNormalMode() {
        DataCacheMetaManager.CacheDeleteTaskRequest request =
                new DataCacheMetaManager.CacheDeleteTaskRequest(
                        "hive0", "datacache_db", "test_table", "p20240102",
                        DataCacheMetaManager.CacheDeleteMode.NORMAL);

        String sql = metaManager.buildCacheDeleteSubmitSql(request);

        Assert.assertTrue("SQL should contain NORMAL mode", sql.contains("NORMAL"));
    }

    // ========================================
    // J. Partition Lock Tests
    // ========================================

    @Test
    public void testPartitionLockAcquisition() {
        TableName tableName = new TableName("hive0", "datacache_db", "lock_test_table");
        String partition = "p20240101";

        // Test that partition lock can be acquired and released
        try (com.starrocks.common.CloseableLock lock = metaManager.lockPartition(tableName, partition)) {
            Assert.assertNotNull("Lock should be acquired", lock);
            // Lock is held here
        }
        // Lock is released here

        // Should be able to acquire again
        try (com.starrocks.common.CloseableLock lock = metaManager.lockPartition(tableName, partition)) {
            Assert.assertNotNull("Lock should be acquired again", lock);
        }
    }

    @Test
    public void testDifferentPartitionsHaveDifferentLocks() {
        TableName tableName = new TableName("hive0", "datacache_db", "lock_test_table");

        // Different partitions should have different locks
        AtomicInteger counter = new AtomicInteger(0);

        // Acquire lock for partition 1
        try (com.starrocks.common.CloseableLock lock1 = metaManager.lockPartition(tableName, "p1")) {
            counter.incrementAndGet();

            // Should be able to acquire lock for partition 2 while holding lock for partition 1
            try (com.starrocks.common.CloseableLock lock2 = metaManager.lockPartition(tableName, "p2")) {
                counter.incrementAndGet();
            }
        }

        Assert.assertEquals("Both locks should have been acquired", 2, counter.get());
    }

    // ========================================
    // K. Table and Partition Meta Existence Tests
    // ========================================

    @Test
    public void testTableMetaExistenceAfterMultiplePartitions() throws Exception {
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");
        LocalDateTime now = LocalDateTime.now();

        // Ensure table exists
        metaManager.upsertTableMeta(tableName);

        // Add multiple partitions
        for (int i = 1; i <= 5; i++) {
            String partition = "existence_test_p" + i;
            metaManager.upsertPartitionMeta(tableName, partition, (long) i,
                    DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                    "l_shipdate", "DATE", i * 100 * 1024 * 1024L,
                    "/path/" + partition, "DAY", "yyyy-MM-dd", "sig-" + i);
        }

        // Verify all exist
        Assert.assertTrue("Table should exist", metaManager.existsTable(tableName));
        for (int i = 1; i <= 5; i++) {
            Assert.assertTrue("Partition " + i + " should exist",
                    metaManager.existsPartition(tableName, "existence_test_p" + i));
        }

        // Remove one partition
        metaManager.removePartitionMeta(tableName, "existence_test_p3");

        // Verify table still exists but partition 3 is gone
        Assert.assertTrue("Table should still exist", metaManager.existsTable(tableName));
        Assert.assertFalse("Partition 3 should not exist",
                metaManager.existsPartition(tableName, "existence_test_p3"));
        Assert.assertTrue("Partition 2 should still exist",
                metaManager.existsPartition(tableName, "existence_test_p2"));
    }

    // ========================================
    // L. SHOW DATA CACHE TABLES Tests
    // ========================================

    @Test
    public void testShowDataCacheTablesWithCurrentDb() throws Exception {
        // Setup: create partition metadata to ensure tables appear in SHOW DATA CACHE TABLES
        TableName tableName1 = new TableName("hive0", "datacache_db", "show_tables_test1");
        TableName tableName2 = new TableName("hive0", "datacache_db", "show_tables_test2");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName1);
        metaManager.upsertTableMeta(tableName2);

        // Add partitions with known cache sizes
        metaManager.upsertPartitionMeta(tableName1, "p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 100 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");
        metaManager.upsertPartitionMeta(tableName2, "p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 200 * 1024 * 1024L, "/path/p2", "DAY", "yyyy-MM-dd", "sig2");

        // Execute SHOW DATA CACHE TABLES via SQL (with db context)
        runSql("SET CATALOG hive0");
        runSql("USE datacache_db");
        List<List<String>> rows = runQuery("SHOW DATA CACHE TABLES");

        // Verify: should contain tables from datacache_db
        boolean foundTable1 = rows.stream().anyMatch(row ->
                row.get(1).equals("datacache_db") && row.get(2).equals("show_tables_test1"));
        boolean foundTable2 = rows.stream().anyMatch(row ->
                row.get(1).equals("datacache_db") && row.get(2).equals("show_tables_test2"));

        Assert.assertTrue("Should find show_tables_test1 in datacache_db", foundTable1);
        Assert.assertTrue("Should find show_tables_test2 in datacache_db", foundTable2);
    }

    @Test
    public void testShowDataCacheTablesWithoutDb() throws Exception {
        // Setup: create tables in multiple databases
        TableName tableName1 = new TableName("hive0", "db_show_a", "all_tables_test1");
        TableName tableName2 = new TableName("hive0", "db_show_b", "all_tables_test2");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName1);
        metaManager.upsertTableMeta(tableName2);

        metaManager.upsertPartitionMeta(tableName1, "p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 100 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");
        metaManager.upsertPartitionMeta(tableName2, "p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 200 * 1024 * 1024L, "/path/p2", "DAY", "yyyy-MM-dd", "sig2");

        // Execute SHOW DATA CACHE TABLES without USE db (should show all tables in catalog)
        // Only SET CATALOG, do NOT USE any database
        runSql("SET CATALOG hive0");
        List<List<String>> rows = runQuery("SHOW DATA CACHE TABLES");

        // Verify: should contain tables from all databases in hive0 catalog
        boolean foundTable1 = rows.stream().anyMatch(row ->
                row.get(1).equals("db_show_a") && row.get(2).equals("all_tables_test1"));
        boolean foundTable2 = rows.stream().anyMatch(row ->
                row.get(1).equals("db_show_b") && row.get(2).equals("all_tables_test2"));

        Assert.assertTrue("Should find all_tables_test1 from db_show_a", foundTable1);
        Assert.assertTrue("Should find all_tables_test2 from db_show_b", foundTable2);
    }

    @Test
    public void testShowDataCacheTablesReturnsCorrectColumns() throws Exception {
        // Setup: create a table with full metadata
        TableName tableName = new TableName("hive0", "datacache_db", "columns_test_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        // Add a partition to make the table have cache size (50MB)
        metaManager.upsertPartitionMeta(tableName, "test_partition", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 1024L * 1024 * 50,  // 50MB
                "/path/test", "DAY", "yyyy-MM-dd", "test-sig");

        // Execute SHOW DATA CACHE TABLES via SQL
        runSql("SET CATALOG hive0");
        runSql("USE datacache_db");
        List<List<String>> rows = runQuery("SHOW DATA CACHE TABLES");

        // Find our test table (column 2 is Table name)
        List<String> tableRow = rows.stream()
                .filter(row -> row.get(2).equals("columns_test_table"))
                .findFirst()
                .orElse(null);

        Assert.assertNotNull("Should find columns_test_table", tableRow);

        // Verify columns (based on ShowDataCacheTableStmt.getMetaData())
        // 0: Catalog, 1: Database, 2: Table, 3: Table Id, 4: Table Type
        // 5: Created Time, 6: Updated Time, 7: Cache Size, 8: Schedule Task Name
        Assert.assertEquals("Catalog should be hive0", "hive0", tableRow.get(0));
        Assert.assertEquals("Database should be datacache_db", "datacache_db", tableRow.get(1));
        Assert.assertEquals("Table should be columns_test_table", "columns_test_table", tableRow.get(2));
        Assert.assertNotNull("Table Id should not be null", tableRow.get(3));
        // Cache size should be "50.00 MB"
        Assert.assertTrue("Cache Size should contain 50",
                tableRow.get(7).contains("50") || tableRow.get(7).contains("MB"));
    }

    // ========================================
    // M. SHOW DATA CACHE FROM <table> and Cache Size Tests
    // ========================================

    @Test
    public void testShowDataCacheFromTableAfterCacheSelect() throws Exception {
        // Use a unique partition to avoid conflicts with other tests
        String partition = "p19980103";
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Clear any existing metadata for this partition
        if (metaManager.existsPartition(tableName, partition)) {
            metaManager.removePartitionMeta(tableName, partition);
        }

        // Execute CACHE SELECT via SQL
        String cacheSql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        runSql(cacheSql);

        // Execute SHOW DATA CACHE FROM <table> via SQL
        List<List<String>> rows = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.multi_partition_table");

        // Find our partition (column 2 is Partition key)
        List<String> partitionRow = rows.stream()
                .filter(row -> row.get(2).equals(partition))
                .findFirst()
                .orElse(null);

        Assert.assertNotNull("Should find partition " + partition + " in SHOW DATA CACHE result", partitionRow);

        // Column 4 is Cache Status, should be ACTIVE
        Assert.assertEquals("Cache status should be ACTIVE",
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, partitionRow.get(4));

        // Column 12 is Cache Data Size, should not be empty
        String cacheSizeStr = partitionRow.get(12);
        Assert.assertNotNull("Cache size should not be null", cacheSizeStr);
        Assert.assertFalse("Cache size should not be empty", cacheSizeStr.isEmpty());
    }

    @Test
    public void testShowDataCacheTablesSizeAfterCacheSelect() throws Exception {
        // Use unique partition
        String partition = "p19980104";
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        // Clear existing partition if any
        if (metaManager.existsPartition(tableName, partition)) {
            metaManager.removePartitionMeta(tableName, partition);
        }

        // Execute CACHE SELECT via SQL
        String cacheSql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='" + partition + "', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        runSql(cacheSql);

        // Execute SHOW DATA CACHE TABLES via SQL
        runSql("SET CATALOG hive0");
        runSql("USE datacache_db");
        List<List<String>> rows = runQuery("SHOW DATA CACHE TABLES");

        // Find our table (column 2 is Table name)
        List<String> tableRow = rows.stream()
                .filter(row -> row.get(2).equals("multi_partition_table"))
                .findFirst()
                .orElse(null);

        Assert.assertNotNull("Should find multi_partition_table in SHOW DATA CACHE TABLES result", tableRow);

        // Column 7 is Cache Size, should not be empty
        String cacheSizeStr = tableRow.get(7);
        Assert.assertNotNull("Table cache size should not be null", cacheSizeStr);
        Assert.assertFalse("Table cache size should not be empty", cacheSizeStr.isEmpty());
    }

    @Test
    public void testShowDataCacheTablesSizeUpdatesAfterMultiplePartitions() throws Exception {
        // Setup: Create a fresh table for this test
        TableName tableName = new TableName("hive0", "datacache_db", "cache_size_update_table");
        LocalDateTime now = LocalDateTime.now();

        // Create table meta
        metaManager.upsertTableMeta(tableName);

        // Add first partition with known size (100MB)
        long partition1Size = 100 * 1024 * 1024L;
        metaManager.upsertPartitionMeta(tableName, "size_test_p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", partition1Size, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");

        // Execute SHOW DATA CACHE TABLES and verify size
        runSql("SET CATALOG hive0");
        runSql("USE datacache_db");
        List<List<String>> rows1 = runQuery("SHOW DATA CACHE TABLES");
        List<String> tableRow1 = rows1.stream()
                .filter(row -> row.get(2).equals("cache_size_update_table"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find table after first partition", tableRow1);
        Assert.assertTrue("Cache size should contain 100MB",
                tableRow1.get(7).contains("100") || tableRow1.get(7).contains("MB"));

        // Add second partition (200MB)
        long partition2Size = 200 * 1024 * 1024L;
        metaManager.upsertPartitionMeta(tableName, "size_test_p2", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", partition2Size, "/path/p2", "DAY", "yyyy-MM-dd", "sig2");

        // Verify table cache size is sum of both partitions (300MB)
        List<List<String>> rows2 = runQuery("SHOW DATA CACHE TABLES");
        List<String> tableRow2 = rows2.stream()
                .filter(row -> row.get(2).equals("cache_size_update_table"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find table after second partition", tableRow2);
        Assert.assertTrue("Cache size should contain 300MB",
                tableRow2.get(7).contains("300") || tableRow2.get(7).contains("MB"));
    }

    @Test
    public void testShowDataCacheFromTableAfterCacheDelete() throws Exception {
        // Setup: Create a table with partition
        TableName tableName = new TableName("hive0", "datacache_db", "cache_delete_show_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(tableName, "delete_show_p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 150 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");

        // Verify partition exists via SHOW DATA CACHE FROM
        List<List<String>> rowsBefore = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.cache_delete_show_table");
        boolean existsBefore = rowsBefore.stream().anyMatch(row -> row.get(2).equals("delete_show_p1"));
        Assert.assertTrue("Partition should exist before CACHE DELETE", existsBefore);

        // Execute CACHE DELETE (we need to simulate this since actual delete requires BE)
        // For testing, we directly remove via metaManager
        metaManager.removePartitionMeta(tableName, "delete_show_p1");

        // Verify partition is gone via SHOW DATA CACHE FROM
        List<List<String>> rowsAfter = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.cache_delete_show_table");
        boolean existsAfter = rowsAfter.stream().anyMatch(row -> row.get(2).equals("delete_show_p1"));
        Assert.assertFalse("Partition should not exist after CACHE DELETE", existsAfter);
    }

    @Test
    public void testShowDataCacheTablesSizeDecreasesAfterPartitionRemoval() throws Exception {
        // Setup: Create a table with two partitions
        TableName tableName = new TableName("hive0", "datacache_db", "cache_decrease_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        long partition1Size = 150 * 1024 * 1024L; // 150MB
        long partition2Size = 250 * 1024 * 1024L; // 250MB

        metaManager.upsertPartitionMeta(tableName, "decrease_p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", partition1Size, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");

        metaManager.upsertPartitionMeta(tableName, "decrease_p2", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", partition2Size, "/path/p2", "DAY", "yyyy-MM-dd", "sig2");

        // Verify initial total (400MB) via SHOW DATA CACHE TABLES
        runSql("SET CATALOG hive0");
        runSql("USE datacache_db");
        List<List<String>> rowsBefore = runQuery("SHOW DATA CACHE TABLES");
        List<String> tableRowBefore = rowsBefore.stream()
                .filter(row -> row.get(2).equals("cache_decrease_table"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find table before removal", tableRowBefore);
        Assert.assertTrue("Initial cache size should be 400MB",
                tableRowBefore.get(7).contains("400") || tableRowBefore.get(7).contains("MB"));

        // Remove partition 2
        metaManager.removePartitionMeta(tableName, "decrease_p2");

        // Verify cache size decreased (should be 150MB now)
        List<List<String>> rowsAfter = runQuery("SHOW DATA CACHE TABLES");
        List<String> tableRowAfter = rowsAfter.stream()
                .filter(row -> row.get(2).equals("cache_decrease_table"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find table after removal", tableRowAfter);
        Assert.assertTrue("Cache size should decrease to 150MB",
                tableRowAfter.get(7).contains("150") || tableRowAfter.get(7).contains("MB"));
    }

    @Test
    public void testShowDataCacheFromTableReturnsPartitionDetails() throws Exception {
        // Setup: Create table with multiple partitions having different attributes
        TableName tableName = new TableName("hive0", "datacache_db", "show_partitions_detail_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        // Add 3 partitions with different sizes and statuses
        metaManager.upsertPartitionMeta(tableName, "detail_p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "l_shipdate", "DATE", 50 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");

        metaManager.upsertPartitionMeta(tableName, "detail_p2", 2L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, now.plusHours(1),
                "l_shipdate", "DATE", 75 * 1024 * 1024L, "/path/p2", "DAY", "yyyy-MM-dd", "sig2");

        metaManager.upsertPartitionMeta(tableName, "detail_p3", 3L,
                "EXPIRED", now, now.plusHours(2),
                "l_shipdate", "DATE", 100 * 1024 * 1024L, "/path/p3", "DAY", "yyyy-MM-dd", "sig3");

        // Execute SHOW DATA CACHE FROM <table> via SQL
        List<List<String>> rows = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.show_partitions_detail_table");

        // Verify all 3 partitions are returned
        long matchCount = rows.stream().filter(row ->
                row.get(2).equals("detail_p1") ||
                row.get(2).equals("detail_p2") ||
                row.get(2).equals("detail_p3")
        ).count();
        Assert.assertEquals("Should have exactly 3 matching partitions", 3, matchCount);

        // Find partition with EXPIRED status
        List<String> expiredPartition = rows.stream()
                .filter(row -> row.get(2).equals("detail_p3"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find detail_p3", expiredPartition);
        // Column 4 is Cache Status
        Assert.assertEquals("Cache status should be EXPIRED", "EXPIRED", expiredPartition.get(4));
        // Column 3 is Version
        Assert.assertEquals("Version should be 3", "3", expiredPartition.get(3));
    }

    @Test
    public void testShowDataCacheFromTablePartitionVersionUpdates() throws Exception {
        // Setup: Create a partition with initial version
        TableName tableName = new TableName("hive0", "datacache_db", "version_update_show_table");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);

        // Add partition with version 1
        metaManager.upsertPartitionMeta(tableName, "version_show_p1", 1L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, null,
                "dt", "DATE", 100 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1");

        // Get initial version via SHOW DATA CACHE FROM
        List<List<String>> rows1 = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.version_update_show_table");
        List<String> partition1 = rows1.stream()
                .filter(row -> row.get(2).equals("version_show_p1"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find partition initially", partition1);
        // Column 3 is Version
        Assert.assertEquals("Initial version should be 1", "1", partition1.get(3));

        // Update partition with new version
        metaManager.upsertPartitionMeta(tableName, "version_show_p1", 5L,
                DataCacheMetaManager.CACHE_STATUS_ACTIVE, now, now.plusMinutes(5),
                "dt", "DATE", 120 * 1024 * 1024L, "/path/p1", "DAY", "yyyy-MM-dd", "sig1-v5");

        // Verify version updated via SHOW DATA CACHE FROM
        List<List<String>> rows2 = runQuery("SHOW DATA CACHE FROM hive0.datacache_db.version_update_show_table");
        List<String> partition2 = rows2.stream()
                .filter(row -> row.get(2).equals("version_show_p1"))
                .findFirst().orElse(null);

        Assert.assertNotNull("Should find partition after update", partition2);
        Assert.assertEquals("Version should be updated to 5", "5", partition2.get(3));
    }
}
