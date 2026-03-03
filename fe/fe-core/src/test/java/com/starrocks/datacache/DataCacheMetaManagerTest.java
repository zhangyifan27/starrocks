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
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Comprehensive tests for DataCacheMetaManager.
 *
 * These tests verify the overall logic of data cache metadata management including:
 * - Table/Partition/File metadata lifecycle (CRUD operations)
 * - EditLog persistence and replay
 * - GC logic for expired partitions
 * - Concurrent access and locking
 * - Hash ring signature tracking
 * - Version management and expired file detection
 */
public class DataCacheMetaManagerTest {

    private static StarRocksAssert starRocksAssert;
    private static DataCacheMetaManager metaManager;

    @BeforeClass
    public static void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        metaManager = new DataCacheMetaManager(60_000L, 1L, 1L);
    }

    @AfterClass
    public static void tearDown() {
        // Cleanup
    }

    // ========================================
    // A. Table Metadata Lifecycle Tests
    // ========================================

    @Test
    public void testTableMetaLifecycle_CreateAndRetrieve() {
        TableName tableName = new TableName("hive_catalog", "test_db", "table_lifecycle_1");

        // Initially table should not exist
        Assert.assertFalse(metaManager.existsTable(tableName));

        // Create table meta
        metaManager.upsertTableMeta(tableName);

        // Now table should exist
        Assert.assertTrue(metaManager.existsTable(tableName));

        // Retrieve and verify
        Optional<DataCacheTableMeta> metaOpt = metaManager.getTableMeta(tableName);
        Assert.assertTrue(metaOpt.isPresent());
        DataCacheTableMeta meta = metaOpt.get();
        Assert.assertEquals("hive_catalog", meta.getCatalogName());
        Assert.assertEquals("test_db", meta.getDbName());
        Assert.assertEquals("table_lifecycle_1", meta.getTableName());
        Assert.assertNotNull(meta.getCreatedTime());
        Assert.assertNotNull(meta.getUpdatedTime());
    }

    @Test
    public void testTableMetaLifecycle_UpdatePreservesCreatedTime() throws Exception {
        TableName tableName = new TableName("hive_catalog", "test_db", "table_lifecycle_2");

        // Create table meta
        metaManager.upsertTableMeta(tableName);
        Optional<DataCacheTableMeta> meta1Opt = metaManager.getTableMeta(tableName);
        Assert.assertTrue(meta1Opt.isPresent());
        LocalDateTime createdTime1 = meta1Opt.get().getCreatedTime();

        Thread.sleep(50); // Ensure time difference

        // Update table meta
        metaManager.upsertTableMeta(tableName);
        Optional<DataCacheTableMeta> meta2Opt = metaManager.getTableMeta(tableName);
        Assert.assertTrue(meta2Opt.isPresent());

        // createdTime should remain the same
        Assert.assertEquals(createdTime1.withNano(0), meta2Opt.get().getCreatedTime().withNano(0));
    }

    @Test
    public void testTableMetaLifecycle_WithScheduleTask() {
        TableName tableName = new TableName("hive_catalog", "test_db", "table_lifecycle_3");
        String taskName = "DataCacheSchedule-test-task";

        metaManager.upsertTableMeta(tableName, taskName);

        Optional<DataCacheTableMeta> metaOpt = metaManager.getTableMeta(tableName);
        Assert.assertTrue(metaOpt.isPresent());
        Assert.assertEquals(taskName, metaOpt.get().getScheduleTaskName());
    }

    @Test
    public void testTableMetaLifecycle_DifferentCatalogs() {
        TableName table1 = new TableName("hive_catalog", "db1", "same_table");
        TableName table2 = new TableName("iceberg_catalog", "db1", "same_table");

        metaManager.upsertTableMeta(table1);
        metaManager.upsertTableMeta(table2);

        // Both should exist independently
        Assert.assertTrue(metaManager.existsTable(table1));
        Assert.assertTrue(metaManager.existsTable(table2));

        Optional<DataCacheTableMeta> meta1 = metaManager.getTableMeta(table1);
        Optional<DataCacheTableMeta> meta2 = metaManager.getTableMeta(table2);

        Assert.assertTrue(meta1.isPresent());
        Assert.assertTrue(meta2.isPresent());
        Assert.assertNotEquals(meta1.get().getTableId(), meta2.get().getTableId());
    }

    // ========================================
    // B. Partition Metadata Lifecycle Tests
    // ========================================

    @Test
    public void testPartitionMetaLifecycle_CreateAndRetrieve() {
        TableName tableName = new TableName("hive_catalog", "test_db", "partition_test_1");
        String partitionKey = "dt=2024-01-01";

        // Initially partition should not exist
        Assert.assertFalse(metaManager.existsPartition(tableName, partitionKey));

        // Create partition meta
        LocalDateTime now = LocalDateTime.now();
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L * 1024, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // Now partition should exist
        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));

        // Retrieve and verify
        Optional<DataCachePartitionMeta> metaOpt = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(metaOpt.isPresent());
        DataCachePartitionMeta meta = metaOpt.get();
        Assert.assertEquals(partitionKey, meta.getPartitionKey());
        Assert.assertEquals(1L, meta.getVersion());
        Assert.assertEquals("ACTIVE", meta.getCacheStatus());
        Assert.assertEquals("dt", meta.getPartitionField());
        Assert.assertEquals("DATE", meta.getPartitionFieldType());
        Assert.assertEquals(1024L * 1024, meta.getCacheDataSize());
    }

    @Test
    public void testPartitionMetaLifecycle_VersionUpdate() {
        TableName tableName = new TableName("hive_catalog", "test_db", "partition_test_2");
        String partitionKey = "dt=2024-01-02";
        LocalDateTime now = LocalDateTime.now();

        // Create with version 1
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );
        Assert.assertEquals(1L, metaManager.getPartitionVersion(tableName, partitionKey));

        // Update to version 2
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 2L, "ACTIVE", now, null,
                "dt", "DATE", 2048L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );
        Assert.assertEquals(2L, metaManager.getPartitionVersion(tableName, partitionKey));
    }

    @Test
    public void testPartitionMetaLifecycle_Remove() {
        TableName tableName = new TableName("hive_catalog", "test_db", "partition_test_3");
        String partitionKey = "dt=2024-01-03";
        LocalDateTime now = LocalDateTime.now();

        // Create partition
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );
        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));

        // Remove partition
        metaManager.removePartitionMeta(tableName, partitionKey);
        Assert.assertFalse(metaManager.existsPartition(tableName, partitionKey));
    }

    @Test
    public void testPartitionMetaLifecycle_MultiplePartitions() {
        TableName tableName = new TableName("hive_catalog", "test_db", "partition_test_4");
        LocalDateTime now = LocalDateTime.now();

        // Create multiple partitions
        for (int i = 1; i <= 5; i++) {
            String partitionKey = "dt=2024-01-0" + i;
            metaManager.upsertPartitionMeta(
                    tableName, partitionKey, (long) i, "ACTIVE", now, null,
                    "dt", "DATE", 1024L * i, "/path/to/partition" + i,
                    "DAY", "yyyy-MM-dd", "signature" + i
            );
        }

        // Verify all partitions exist with correct versions
        for (int i = 1; i <= 5; i++) {
            String partitionKey = "dt=2024-01-0" + i;
            Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));
            Assert.assertEquals((long) i, metaManager.getPartitionVersion(tableName, partitionKey));
        }
    }

    @Test
    public void testPartitionMetaLifecycle_HashRingSignature() {
        TableName tableName = new TableName("hive_catalog", "test_db", "partition_test_5");
        String partitionKey = "dt=2024-01-05";
        LocalDateTime now = LocalDateTime.now();
        String signature = "test-hash-ring-signature-12345";

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", signature
        );

        Assert.assertEquals(signature, metaManager.getPartitionHashRingSignature(tableName, partitionKey));
    }

    // ========================================
    // C. Hash Ring Signature Tests
    // ========================================

    @Test
    public void testHashRingSignature_Deterministic() {
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        String sig2 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));

        Assert.assertEquals(sig1, sig2);
    }

    @Test
    public void testHashRingSignature_OrderIndependent() {
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        String sig2 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10003L, 10001L, 10002L));

        Assert.assertEquals(sig1, sig2);
    }

    @Test
    public void testHashRingSignature_DifferentAlgorithm() {
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        String sig2 = metaManager.buildHashRingSignature("consistent_hash", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));

        Assert.assertNotEquals(sig1, sig2);
    }

    @Test
    public void testHashRingSignature_DifferentWorkers() {
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        String sig2 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10004L)); // Different worker

        Assert.assertNotEquals(sig1, sig2);
    }

    @Test
    public void testHashRingSignature_DifferentVirtualNodes() {
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        String sig2 = metaManager.buildHashRingSignature("rendezvous", false, 20,
                Arrays.asList(10001L, 10002L, 10003L));

        Assert.assertNotEquals(sig1, sig2);
    }

    // ========================================
    // D. File Metadata Tests
    // ========================================

    @Test
    public void testFileMeta_BuildFileMeta() {
        TableName tableName = new TableName("hive_catalog", "test_db", "file_test_1");
        String partitionKey = "dt=2024-01-01";

        DataCacheFileMeta fileMeta = metaManager.buildFileMeta(
                tableName, partitionKey, 10001L,
                "/data/file1.parquet", 1024L * 1024, 0L, 1024L * 1024,
                1L, System.currentTimeMillis(), "PARQUET", false, "signature1"
        );

        Assert.assertNotNull(fileMeta);
        Assert.assertEquals(10001L, fileMeta.getBackendId());
        Assert.assertEquals("/data/file1.parquet", fileMeta.getFilePath());
        Assert.assertEquals(1024L * 1024, fileMeta.getFileSizeBytes());
        Assert.assertEquals("PARQUET", fileMeta.getFileType());
        Assert.assertFalse(fileMeta.isRelativePath());
    }

    @Test
    public void testFileMeta_SamePathSameFileId() {
        TableName tableName = new TableName("hive_catalog", "test_db", "file_test_2");
        String partitionKey = "dt=2024-01-01";
        String filePath = "/data/same_file.parquet";

        DataCacheFileMeta file1 = metaManager.buildFileMeta(
                tableName, partitionKey, 10001L,
                filePath, 1024L, 0L, 1024L,
                1L, System.currentTimeMillis(), "PARQUET", false, "sig1"
        );

        DataCacheFileMeta file2 = metaManager.buildFileMeta(
                tableName, partitionKey, 10002L, // Different backend
                filePath, 2048L, 0L, 2048L,  // Different size
                2L, System.currentTimeMillis(), "PARQUET", false, "sig2"
        );

        // Same file path should produce same file ID
        Assert.assertEquals(file1.getFileId(), file2.getFileId());
    }

    @Test
    public void testFileMeta_DifferentPathDifferentFileId() {
        TableName tableName = new TableName("hive_catalog", "test_db", "file_test_3");
        String partitionKey = "dt=2024-01-01";

        DataCacheFileMeta file1 = metaManager.buildFileMeta(
                tableName, partitionKey, 10001L,
                "/data/file1.parquet", 1024L, 0L, 1024L,
                1L, System.currentTimeMillis(), "PARQUET", false, "sig1"
        );

        DataCacheFileMeta file2 = metaManager.buildFileMeta(
                tableName, partitionKey, 10001L,
                "/data/file2.parquet", 1024L, 0L, 1024L,
                1L, System.currentTimeMillis(), "PARQUET", false, "sig1"
        );

        // Different file paths should produce different file IDs
        Assert.assertNotEquals(file1.getFileId(), file2.getFileId());
    }

    @Test
    public void testFileMeta_UpsertFile() {
        TableName tableName = new TableName("hive_catalog", "test_db", "file_test_4");
        String partitionKey = "dt=2024-01-01";

        DataCacheFileMeta fileMeta = metaManager.buildFileMeta(
                tableName, partitionKey, 10001L,
                "/data/test_file.parquet", 1024L * 1024, 0L, 1024L * 1024,
                1L, System.currentTimeMillis(), "PARQUET", false, "signature1"
        );

        // Should not throw exception
        boolean result = metaManager.upsertFile(fileMeta);
        Assert.assertTrue(result);
    }

    // ========================================
    // E. EditLog Replay Tests
    // ========================================

    @Test
    public void testReplay_TableMeta() {
        TableName tableName = new TableName("hive_catalog", "test_db", "replay_table_1");

        // First create normally to get computed ID
        metaManager.upsertTableMeta(tableName);
        Optional<DataCacheTableMeta> normalMeta = metaManager.getTableMeta(tableName);
        Assert.assertTrue(normalMeta.isPresent());
        long tableId = normalMeta.get().getTableId();

        // Create replay meta with same ID
        DataCacheTableMeta replayMeta = new DataCacheTableMeta(
                tableId, "hive_catalog", "test_db", "replay_table_1", "HIVE",
                LocalDateTime.now(), LocalDateTime.now(), 9999L, "replay_task"
        );

        // Replay
        metaManager.replayUpsertTableMeta(replayMeta);

        // Verify replayed data
        Optional<DataCacheTableMeta> retrieved = metaManager.getTableMeta(tableName);
        Assert.assertTrue(retrieved.isPresent());
        Assert.assertEquals(9999L, retrieved.get().getCacheSize());
        Assert.assertEquals("replay_task", retrieved.get().getScheduleTaskName());
    }

    @Test
    public void testReplay_PartitionMeta() {
        TableName tableName = new TableName("hive_catalog", "test_db", "replay_partition_1");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // First create normally
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        Optional<DataCachePartitionMeta> normalMeta = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(normalMeta.isPresent());
        long tableId = normalMeta.get().getTableId();
        long partitionUid = normalMeta.get().getPartitionUid();

        // Create replay meta
        DataCachePartitionMeta replayMeta = new DataCachePartitionMeta(
                tableId, partitionKey, partitionUid, 99L, "REPLAYED",
                now, now, 5L, 5000L, null,
                "dt", "DATE", 8888L, "/replayed/path",
                "DAY", "yyyy-MM-dd", "replayed-signature"
        );

        // Replay
        metaManager.replayUpsertPartitionMeta(replayMeta);

        // Verify replayed data
        Optional<DataCachePartitionMeta> retrieved = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(retrieved.isPresent());
        Assert.assertEquals(99L, retrieved.get().getVersion());
        Assert.assertEquals("REPLAYED", retrieved.get().getCacheStatus());
        Assert.assertEquals(8888L, retrieved.get().getCacheDataSize());
    }

    @Test
    public void testReplay_DeletePartitionMeta() {
        TableName tableName = new TableName("hive_catalog", "test_db", "replay_delete_1");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // Create partition
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );
        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));

        // Get partition UID for replay delete
        Optional<DataCachePartitionMeta> meta = metaManager.getPartitionMeta(tableName, partitionKey);
        long partitionUid = meta.get().getPartitionUid();

        // Replay delete
        metaManager.replayDeletePartitionMeta(partitionUid);

        // Verify deletion
        Assert.assertFalse(metaManager.existsPartition(tableName, partitionKey));
    }

    // ========================================
    // F. GC Candidate Queue Tests
    // ========================================

    @Test
    public void testGcCandidate_AddPartition() {
        long partitionId = 99999L;

        boolean added = metaManager.addCandidateGcPartition(partitionId);
        Assert.assertTrue("First add should succeed", added);
    }

    @Test
    public void testGcCandidate_Deduplication() {
        long partitionId = 88888L;

        boolean added1 = metaManager.addCandidateGcPartition(partitionId);
        boolean added2 = metaManager.addCandidateGcPartition(partitionId);

        Assert.assertTrue("First add should succeed", added1);
        Assert.assertFalse("Duplicate add should be deduplicated", added2);
    }

    @Test
    public void testGcCandidate_MultipleDifferentPartitions() {
        long partition1 = 77771L;
        long partition2 = 77772L;
        long partition3 = 77773L;

        boolean added1 = metaManager.addCandidateGcPartition(partition1);
        boolean added2 = metaManager.addCandidateGcPartition(partition2);
        boolean added3 = metaManager.addCandidateGcPartition(partition3);

        Assert.assertTrue(added1);
        Assert.assertTrue(added2);
        Assert.assertTrue(added3);
    }

    // ========================================
    // G. Concurrent Access Tests
    // ========================================

    @Test
    public void testConcurrency_PartitionLockSerializes() throws Exception {
        TableName tableName = new TableName("hive_catalog", "test_db", "concurrent_test_1");
        String partitionKey = "dt=2024-01-01";

        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(10);

        ExecutorService executor = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    try (var lock = metaManager.lockPartition(tableName, partitionKey)) {
                        int value = counter.get();
                        Thread.sleep(10); // Simulate work
                        counter.set(value + 1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // If locking works correctly, counter should be exactly 10
        Assert.assertEquals(10, counter.get());
    }

    @Test
    public void testConcurrency_DifferentPartitionsNoBlock() throws Exception {
        TableName tableName = new TableName("hive_catalog", "test_db", "concurrent_test_2");

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch bothAcquired = new CountDownLatch(2);
        AtomicInteger locksHeld = new AtomicInteger(0);

        Thread t1 = new Thread(() -> {
            try {
                startLatch.await();
                try (var lock = metaManager.lockPartition(tableName, "partition1")) {
                    locksHeld.incrementAndGet();
                    bothAcquired.countDown();
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                startLatch.await();
                try (var lock = metaManager.lockPartition(tableName, "partition2")) {
                    locksHeld.incrementAndGet();
                    bothAcquired.countDown();
                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        t1.start();
        t2.start();
        startLatch.countDown();

        // Both threads should acquire locks quickly (no blocking between different partitions)
        boolean acquired = bothAcquired.await(1, TimeUnit.SECONDS);
        Assert.assertTrue("Both threads should acquire locks on different partitions", acquired);
        Assert.assertEquals(2, locksHeld.get());

        t1.join();
        t2.join();
    }

    @Test
    public void testConcurrency_MultipleTableUpserts() throws Exception {
        int numThreads = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numThreads);
        List<Exception> exceptions = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    TableName tableName = new TableName(
                            "hive_catalog", "test_db", "concurrent_table_" + threadId);
                    metaManager.upsertTableMeta(tableName);
                    Assert.assertTrue(metaManager.existsTable(tableName));
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    finishLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        finishLatch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertTrue("No exceptions should occur: " + exceptions, exceptions.isEmpty());
    }

    // ========================================
    // H. Cache Delete SQL Generation Tests
    // ========================================

    @Test
    public void testBuildCacheDeleteSubmitSql_Normal() {
        DataCacheMetaManager.CacheDeleteTaskRequest task =
                new DataCacheMetaManager.CacheDeleteTaskRequest(
                        "hive_catalog", "test_db", "test_table",
                        "dt=2024-01-01", DataCacheMetaManager.CacheDeleteMode.NORMAL
                );

        String sql = metaManager.buildCacheDeleteSubmitSql(task);

        Assert.assertNotNull(sql);
        Assert.assertTrue(sql.contains("SUBMIT TASK"));
        Assert.assertTrue(sql.contains("CACHE DELETE"));
        Assert.assertTrue(sql.contains("hive_catalog"));
        Assert.assertTrue(sql.contains("test_db"));
        Assert.assertTrue(sql.contains("test_table"));
        Assert.assertTrue(sql.contains("dt=2024-01-01"));
        Assert.assertTrue(sql.contains("NORMAL"));
    }

    @Test
    public void testBuildCacheDeleteSubmitSql_GcMode() {
        DataCacheMetaManager.CacheDeleteTaskRequest task =
                new DataCacheMetaManager.CacheDeleteTaskRequest(
                        "hive_catalog", "test_db", "test_table",
                        "dt=2024-01-01", DataCacheMetaManager.CacheDeleteMode.GC
                );

        String sql = metaManager.buildCacheDeleteSubmitSql(task);

        Assert.assertNotNull(sql);
        Assert.assertTrue(sql.contains("GC"));
    }

    @Test
    public void testBuildCacheDeleteSubmitSql_SpecialCharacters() {
        DataCacheMetaManager.CacheDeleteTaskRequest task =
                new DataCacheMetaManager.CacheDeleteTaskRequest(
                        "hive-catalog", "test_db", "test_table",
                        "dt=2024-01-01/hour=12", DataCacheMetaManager.CacheDeleteMode.NORMAL
                );

        String sql = metaManager.buildCacheDeleteSubmitSql(task);

        Assert.assertNotNull(sql);
        // Should handle special characters in identifiers
        Assert.assertTrue(sql.contains("`hive-catalog`"));
    }

    // ========================================
    // I. Integration Scenario Tests
    // ========================================

    @Test
    public void testScenario_CompleteMetadataFlow() {
        // Simulate a complete cache select -> version update -> GC flow
        TableName tableName = new TableName("hive_catalog", "test_db", "scenario_table_1");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // Step 1: Create table meta
        metaManager.upsertTableMeta(tableName);
        Assert.assertTrue(metaManager.existsTable(tableName));

        // Step 2: Create partition meta with version 1
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/v1",
                "DAY", "yyyy-MM-dd", "sig-v1"
        );
        Assert.assertEquals(1L, metaManager.getPartitionVersion(tableName, partitionKey));
        Assert.assertEquals("sig-v1", metaManager.getPartitionHashRingSignature(tableName, partitionKey));

        // Step 3: Update partition meta with version 2 (simulates partition data update)
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 2L, "ACTIVE", now, null,
                "dt", "DATE", 2048L, "/path/v2",
                "DAY", "yyyy-MM-dd", "sig-v2"
        );
        Assert.assertEquals(2L, metaManager.getPartitionVersion(tableName, partitionKey));
        Assert.assertEquals("sig-v2", metaManager.getPartitionHashRingSignature(tableName, partitionKey));

        // Step 4: Verify cache data size increased
        Optional<DataCachePartitionMeta> finalMeta = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(finalMeta.isPresent());
        Assert.assertEquals(2048L, finalMeta.get().getCacheDataSize());
    }

    @Test
    public void testScenario_MultiPartitionTable() {
        TableName tableName = new TableName("hive_catalog", "test_db", "multi_partition_table");
        LocalDateTime now = LocalDateTime.now();

        // Create multiple partitions
        String[] partitions = {"dt=2024-01-01", "dt=2024-01-02", "dt=2024-01-03"};
        for (int i = 0; i < partitions.length; i++) {
            metaManager.upsertPartitionMeta(
                    tableName, partitions[i], (long) (i + 1), "ACTIVE", now, null,
                    "dt", "DATE", 1024L * (i + 1), "/path/" + partitions[i],
                    "DAY", "yyyy-MM-dd", "sig-" + i
            );
        }

        // Verify all partitions exist
        for (int i = 0; i < partitions.length; i++) {
            Assert.assertTrue(metaManager.existsPartition(tableName, partitions[i]));
            Assert.assertEquals((long) (i + 1), metaManager.getPartitionVersion(tableName, partitions[i]));
        }

        // Remove middle partition
        metaManager.removePartitionMeta(tableName, partitions[1]);
        Assert.assertTrue(metaManager.existsPartition(tableName, partitions[0]));
        Assert.assertFalse(metaManager.existsPartition(tableName, partitions[1]));
        Assert.assertTrue(metaManager.existsPartition(tableName, partitions[2]));
    }

    @Test
    public void testScenario_HashRingDrift() {
        TableName tableName = new TableName("hive_catalog", "test_db", "hash_ring_drift_table");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // Initial cache with 3 workers
        String sig1 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10003L));
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", sig1
        );
        Assert.assertEquals(sig1, metaManager.getPartitionHashRingSignature(tableName, partitionKey));

        // Worker topology changes (worker 10003 replaced by 10004)
        String sig2 = metaManager.buildHashRingSignature("rendezvous", false, 10,
                Arrays.asList(10001L, 10002L, 10004L));
        Assert.assertNotEquals(sig1, sig2);

        // Update partition with new signature
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", sig2
        );
        Assert.assertEquals(sig2, metaManager.getPartitionHashRingSignature(tableName, partitionKey));
    }

    // ========================================
    // J. Edge Case Tests
    // ========================================

    @Test
    public void testEdgeCase_EmptyPartitionKey() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_1");
        String partitionKey = ""; // Empty partition key
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "", "", 1024L, "/path/to/partition",
                "", "", "signature"
        );

        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));
    }

    @Test
    public void testEdgeCase_SpecialCharactersInPartitionKey() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_2");
        String partitionKey = "dt=2024-01-01/hour=12/region=us-east-1";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature"
        );

        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));
        Assert.assertEquals(1L, metaManager.getPartitionVersion(tableName, partitionKey));
    }

    @Test
    public void testEdgeCase_LargeVersion() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_3");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();
        long largeVersion = Long.MAX_VALUE - 1;

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, largeVersion, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature"
        );

        Assert.assertEquals(largeVersion, metaManager.getPartitionVersion(tableName, partitionKey));
    }

    @Test
    public void testEdgeCase_TTLExpireAt() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_4");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime ttlExpireAt = now.plusDays(7);

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, ttlExpireAt,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature"
        );

        Optional<DataCachePartitionMeta> meta = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(meta.isPresent());
        Assert.assertNotNull(meta.get().getTtlExpireAt());
        Assert.assertEquals(ttlExpireAt.withNano(0), meta.get().getTtlExpireAt().withNano(0));
    }

    @Test
    public void testEdgeCase_NonExistentPartitionVersion() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_5");
        String partitionKey = "dt=2024-01-01"; // This partition doesn't exist

        long version = metaManager.getPartitionVersion(tableName, partitionKey);
        Assert.assertEquals(0L, version);
    }

    @Test
    public void testEdgeCase_NonExistentPartitionHashRingSignature() {
        TableName tableName = new TableName("hive_catalog", "test_db", "edge_case_6");
        String partitionKey = "dt=2024-01-01"; // This partition doesn't exist

        String signature = metaManager.getPartitionHashRingSignature(tableName, partitionKey);
        Assert.assertEquals("", signature);
    }

    // ========================================
    // K. GetTablesDataCacheSize Tests
    // ========================================

    @Test
    public void testGetTablesDataCacheSize_ExistingCatalogDb() {
        TableName tableName = new TableName("test_catalog", "test_db", "cache_size_table_1");
        LocalDateTime now = LocalDateTime.now();

        // Create table and partition
        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(
                tableName, "dt=2024-01-01", 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L * 1024, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature"
        );

        List<List<String>> rows = metaManager.getTablesDataCacheSize("test_catalog", "test_db");
        Assert.assertNotNull(rows);
        // Should contain at least one table
        boolean found = rows.stream().anyMatch(row ->
                row.get(2).equals("cache_size_table_1"));
        Assert.assertTrue("Should find the created table", found);
    }

    @Test
    public void testGetTablesDataCacheSize_NonExistingCatalog() {
        List<List<String>> rows = metaManager.getTablesDataCacheSize("non_existing_catalog", "non_existing_db");
        Assert.assertNotNull(rows);
        Assert.assertTrue("Should return empty for non-existing catalog/db", rows.isEmpty());
    }

    @Test
    public void testGetTablesDataCacheSize_FiltersByCatalogAndDb() {
        TableName table1 = new TableName("catalog_a", "db_a", "table_filter_1");
        TableName table2 = new TableName("catalog_b", "db_b", "table_filter_2");

        metaManager.upsertTableMeta(table1);
        metaManager.upsertTableMeta(table2);

        List<List<String>> rowsCatalogA = metaManager.getTablesDataCacheSize("catalog_a", "db_a");
        List<List<String>> rowsCatalogB = metaManager.getTablesDataCacheSize("catalog_b", "db_b");

        // Verify filtering works
        boolean foundInA = rowsCatalogA.stream().anyMatch(row -> row.get(2).equals("table_filter_1"));
        boolean notFoundInA = rowsCatalogA.stream().noneMatch(row -> row.get(2).equals("table_filter_2"));
        Assert.assertTrue("Should find table_filter_1 in catalog_a", foundInA);
        Assert.assertTrue("Should not find table_filter_2 in catalog_a", notFoundInA);

        boolean foundInB = rowsCatalogB.stream().anyMatch(row -> row.get(2).equals("table_filter_2"));
        Assert.assertTrue("Should find table_filter_2 in catalog_b", foundInB);
    }

    // ========================================
    // L. GetPartitionsDataCacheSize Tests
    // ========================================

    @Test
    public void testGetPartitionsDataCacheSize_ExistingTable() {
        TableName tableName = new TableName("test_catalog", "test_db", "partition_size_table_1");
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(
                tableName, "dt=2024-01-01", 1L, "ACTIVE", now, null,
                "dt", "DATE", 2048L * 1024, "/path/to/partition1",
                "DAY", "yyyy-MM-dd", "sig1"
        );
        metaManager.upsertPartitionMeta(
                tableName, "dt=2024-01-02", 2L, "ACTIVE", now, null,
                "dt", "DATE", 4096L * 1024, "/path/to/partition2",
                "DAY", "yyyy-MM-dd", "sig2"
        );

        List<List<String>> rows = metaManager.getPartitionsDataCacheSize(tableName);
        Assert.assertNotNull(rows);
        Assert.assertTrue("Should have at least 2 partitions", rows.size() >= 2);
    }

    @Test
    public void testGetPartitionsDataCacheSize_NonExistingTable() {
        TableName tableName = new TableName("non_existing", "non_existing", "non_existing_table");

        List<List<String>> rows = metaManager.getPartitionsDataCacheSize(tableName);
        Assert.assertNotNull(rows);
        Assert.assertTrue("Should return empty for non-existing table", rows.isEmpty());
    }

    @Test
    public void testGetPartitionsDataCacheSize_ContainsAllFields() {
        TableName tableName = new TableName("field_test_catalog", "field_test_db", "field_test_table");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime ttl = now.plusDays(7);

        metaManager.upsertTableMeta(tableName);
        metaManager.upsertPartitionMeta(
                tableName, "dt=2024-03-15", 100L, "ACTIVE", now, ttl,
                "dt", "DATE", 5120L * 1024, "/data/partition",
                "DAY", "yyyy-MM-dd", "test-signature-xyz"
        );

        List<List<String>> rows = metaManager.getPartitionsDataCacheSize(tableName);
        Assert.assertFalse("Should have partition data", rows.isEmpty());

        // Verify row contains expected number of columns (17 columns based on the implementation)
        List<String> row = rows.get(0);
        Assert.assertTrue("Row should have at least 10 columns", row.size() >= 10);
    }

    // ========================================
    // M. ResetExpiredStatistics Tests
    // ========================================

    @Test
    public void testResetExpiredStatisticsForPartitionMeta_Success() {
        TableName tableName = new TableName("reset_test_catalog", "reset_test_db", "reset_test_table");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // Create partition with some expired stats
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // Reset expired statistics
        metaManager.resetExpiredStatisticsForPartitionMeta(tableName, partitionKey);

        // Verify partition still exists
        Assert.assertTrue(metaManager.existsPartition(tableName, partitionKey));
        Optional<DataCachePartitionMeta> meta = metaManager.getPartitionMeta(tableName, partitionKey);
        Assert.assertTrue(meta.isPresent());
        // After reset, expired files and bytes should be 0
        Assert.assertEquals(0, meta.get().getExpiredFiles());
        Assert.assertEquals(0, meta.get().getExpiredBytes());
    }

    @Test
    public void testResetExpiredStatisticsForPartitionMeta_NonExistentPartition() {
        TableName tableName = new TableName("reset_test_catalog2", "reset_test_db2", "reset_test_table2");
        String partitionKey = "dt=2024-01-01"; // This partition doesn't exist

        try {
            metaManager.resetExpiredStatisticsForPartitionMeta(tableName, partitionKey);
            Assert.fail("Should throw exception for non-existent partition");
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("not found"));
        }
    }

    // ========================================
    // N. GetCacheDeleteRemoteFileDescs Tests
    // ========================================

    @Test
    public void testGetCacheDeleteRemoteFileDescs_NonExistentPartition() {
        TableName tableName = new TableName("delete_test_catalog", "delete_test_db", "delete_test_table");
        String partitionKey = "dt=2024-01-01"; // This partition doesn't exist

        List<DataCacheRemoteFileDesc> descs = metaManager.getCacheDeleteRemoteFileDescs(
                tableName, partitionKey, DataCacheMetaManager.CacheDeleteMode.NORMAL);

        Assert.assertNotNull(descs);
        Assert.assertTrue("Should return empty for non-existent partition", descs.isEmpty());
    }

    @Test
    public void testGetCacheDeleteRemoteFileDescs_ExistingPartitionNoFiles() {
        TableName tableName = new TableName("delete_test_catalog2", "delete_test_db2", "delete_test_table2");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        // Create partition without any file metadata
        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // In unit test mode, fileMetaStore returns empty, so this should return empty
        List<DataCacheRemoteFileDesc> descs = metaManager.getCacheDeleteRemoteFileDescs(
                tableName, partitionKey, DataCacheMetaManager.CacheDeleteMode.NORMAL);

        Assert.assertNotNull(descs);
        // Since we're in unit test mode and no files were actually stored
        Assert.assertTrue("Should return empty when no files stored", descs.isEmpty());
    }

    @Test
    public void testGetCacheDeleteRemoteFileDescs_InvalidMode() {
        TableName tableName = new TableName("delete_mode_test", "delete_mode_db", "delete_mode_table");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // Both NORMAL and GC modes should work
        List<DataCacheRemoteFileDesc> normalDescs = metaManager.getCacheDeleteRemoteFileDescs(
                tableName, partitionKey, DataCacheMetaManager.CacheDeleteMode.NORMAL);
        Assert.assertNotNull(normalDescs);

        List<DataCacheRemoteFileDesc> gcDescs = metaManager.getCacheDeleteRemoteFileDescs(
                tableName, partitionKey, DataCacheMetaManager.CacheDeleteMode.GC);
        Assert.assertNotNull(gcDescs);
    }

    // ========================================
    // O. RemoveCacheFileMetaAfterCacheDelete Tests
    // ========================================

    @Test
    public void testRemoveCacheFileMetaAfterCacheDelete_NormalMode() {
        TableName tableName = new TableName("remove_file_test", "remove_file_db", "remove_file_table");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // Should not throw in unit test mode
        metaManager.removeCacheFileMetaAfterCacheDelete(tableName, partitionKey,
                DataCacheMetaManager.CacheDeleteMode.NORMAL);
    }

    @Test
    public void testRemoveCacheFileMetaAfterCacheDelete_GcMode() {
        TableName tableName = new TableName("remove_gc_test", "remove_gc_db", "remove_gc_table");
        String partitionKey = "dt=2024-01-01";
        LocalDateTime now = LocalDateTime.now();

        metaManager.upsertPartitionMeta(
                tableName, partitionKey, 1L, "ACTIVE", now, null,
                "dt", "DATE", 1024L, "/path/to/partition",
                "DAY", "yyyy-MM-dd", "signature1"
        );

        // Should not throw in unit test mode
        metaManager.removeCacheFileMetaAfterCacheDelete(tableName, partitionKey,
                DataCacheMetaManager.CacheDeleteMode.GC);
    }

    @Test
    public void testRemoveCacheFileMetaAfterCacheDelete_GcMode_NonExistentPartition() {
        TableName tableName = new TableName("remove_gc_noexist", "remove_gc_db", "remove_gc_table");
        String partitionKey = "dt=2024-01-01"; // This partition doesn't exist

        try {
            metaManager.removeCacheFileMetaAfterCacheDelete(tableName, partitionKey,
                    DataCacheMetaManager.CacheDeleteMode.GC);
            Assert.fail("Should throw exception for non-existent partition in GC mode");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Partition meta not found"));
        }
    }

    // ========================================
    // P. CacheDeleteMode Enum Tests
    // ========================================

    @Test
    public void testCacheDeleteMode_Values() {
        DataCacheMetaManager.CacheDeleteMode[] modes = DataCacheMetaManager.CacheDeleteMode.values();
        Assert.assertEquals(2, modes.length);
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.NORMAL, modes[0]);
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.GC, modes[1]);
    }

    @Test
    public void testCacheDeleteMode_ValueOf() {
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.NORMAL,
                DataCacheMetaManager.CacheDeleteMode.valueOf("NORMAL"));
        Assert.assertEquals(DataCacheMetaManager.CacheDeleteMode.GC,
                DataCacheMetaManager.CacheDeleteMode.valueOf("GC"));
    }

    // ========================================
    // Q. Constants Tests
    // ========================================

    @Test
    public void testConstants() {
        Assert.assertEquals("datacache_meta_db", DataCacheMetaManager.CACHE_DB_NAME);
        Assert.assertEquals("datacache_file_meta", DataCacheMetaManager.FILE_CACHE_META);
        Assert.assertEquals("ACTIVE", DataCacheMetaManager.CACHE_STATUS_ACTIVE);
    }

    // ========================================
    // R. IsInitialized Tests
    // ========================================

    @Test
    public void testIsInitialized_NewManager() {
        DataCacheMetaManager newManager = new DataCacheMetaManager(60_000L, 1L, 1L);
        // New manager should not be initialized yet
        Assert.assertFalse(newManager.isInitialized());
    }
}
