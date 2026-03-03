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
import com.starrocks.persist.EditLog;
import org.junit.Assert;
import org.mockito.Mockito;

import java.time.LocalDateTime;

public class DataCacheTestUtils {

    public static final String TEST_CATALOG = "hive0";
    public static final String TEST_DB = "test_db";
    public static final String TEST_TABLE = "test_table";
    public static final String TEST_PARTITION_KEY = "dt=2024-01-01";
    public static final long TEST_TABLE_ID = 12345L;
    public static final long TEST_PARTITION_UID = 67890L;
    public static final long TEST_FILE_ID = 111L;
    public static final long TEST_BACKEND_ID = 10001L;

    /**
     * Creates a test DataCacheFileMeta with sensible defaults.
     */
    public static DataCacheFileMeta createTestFileMeta(long partitionUid, long fileId, long backendId) {
        return new DataCacheFileMeta(
                partitionUid,
                fileId,
                backendId,
                "/test/path/file_" + fileId + ".parquet",
                1024L * 1024,  // 1MB
                0L,
                1024L * 1024,
                TEST_TABLE_ID,
                1L,  // partition version
                System.currentTimeMillis(),
                "PARQUET",
                false,
                "worker1,worker2"
        );
    }

    /**
     * Creates a test DataCacheFileMeta with custom path.
     */
    public static DataCacheFileMeta createTestFileMeta(long partitionUid, long fileId, long backendId, String filePath) {
        return new DataCacheFileMeta(
                partitionUid,
                fileId,
                backendId,
                filePath,
                1024L * 1024,
                0L,
                1024L * 1024,
                TEST_TABLE_ID,
                1L,
                System.currentTimeMillis(),
                "PARQUET",
                false,
                "worker1,worker2"
        );
    }

    /**
     * Creates a test DataCacheFileMeta with all custom parameters.
     */
    public static DataCacheFileMeta createTestFileMeta(
            long partitionUid, long fileId, long backendId, String filePath,
            long fileSizeBytes, long offset, long length, long tableId,
            long partitionVersion, long modificationTime, String fileType,
            boolean isRelativePath, String hashRingSignature) {
        return new DataCacheFileMeta(
                partitionUid, fileId, backendId, filePath,
                fileSizeBytes, offset, length, tableId,
                partitionVersion, modificationTime, fileType,
                isRelativePath, hashRingSignature
        );
    }

    /**
     * Creates a test DataCacheTableMeta with sensible defaults.
     */
    public static DataCacheTableMeta createTestTableMeta(long tableId) {
        return new DataCacheTableMeta(
                tableId,
                TEST_CATALOG,
                TEST_DB,
                TEST_TABLE,
                "HIVE",
                LocalDateTime.now(),
                LocalDateTime.now(),
                0L,
                null
        );
    }

    /**
     * Creates a test DataCacheTableMeta with custom catalog/db/table names.
     */
    public static DataCacheTableMeta createTestTableMeta(
            long tableId, String catalogName, String dbName, String tableName) {
        return new DataCacheTableMeta(
                tableId,
                catalogName,
                dbName,
                tableName,
                "HIVE",
                LocalDateTime.now(),
                LocalDateTime.now(),
                0L,
                null
        );
    }

    /**
     * Creates a test DataCachePartitionMeta with sensible defaults.
     */
    public static DataCachePartitionMeta createTestPartitionMeta(
            long tableId, long partitionUid, String partitionKey) {
        return new DataCachePartitionMeta(
                tableId,
                partitionKey,
                partitionUid,
                1L,  // version
                "CACHED",
                LocalDateTime.now(),
                LocalDateTime.now(),
                0L,  // expired files
                0L,  // expired bytes
                null,  // ttl expire at
                "dt",  // partition field
                "DATE",  // partition field type
                1024L * 1024,  // cache data size
                null,  // partition abs prefix path
                "DAY",  // partition unit
                "yyyy-MM-dd",  // partition field format
                "worker1,worker2"  // hash ring signature
        );
    }

    /**
     * Creates a test TableName with default values.
     */
    public static TableName createTestTableName() {
        return new TableName(TEST_CATALOG, TEST_DB, TEST_TABLE);
    }

    /**
     * Creates a test TableName with custom values.
     */
    public static TableName createTestTableName(String catalog, String db, String table) {
        return new TableName(catalog, db, table);
    }

    /**
     * Creates a mock EditLog.
     */
    public static EditLog mockEditLog() {
        return Mockito.mock(EditLog.class);
    }

    /**
     * Asserts that two DataCacheFileMeta objects are equal field-by-field.
     */
    public static void assertFileMetaEquals(DataCacheFileMeta expected, DataCacheFileMeta actual) {
        Assert.assertEquals("partitionUid mismatch", expected.getPartitionUid(), actual.getPartitionUid());
        Assert.assertEquals("fileId mismatch", expected.getFileId(), actual.getFileId());
        Assert.assertEquals("backendId mismatch", expected.getBackendId(), actual.getBackendId());
        Assert.assertEquals("filePath mismatch", expected.getFilePath(), actual.getFilePath());
        Assert.assertEquals("fileSizeBytes mismatch", expected.getFileSizeBytes(), actual.getFileSizeBytes());
        Assert.assertEquals("offset mismatch", expected.getOffset(), actual.getOffset());
        Assert.assertEquals("length mismatch", expected.getLength(), actual.getLength());
        Assert.assertEquals("tableId mismatch", expected.getTableId(), actual.getTableId());
        Assert.assertEquals("partitionVersion mismatch", expected.getPartitionVersion(), actual.getPartitionVersion());
        Assert.assertEquals("modificationTime mismatch", expected.getModificationTime(), actual.getModificationTime());
        Assert.assertEquals("fileType mismatch", expected.getFileType(), actual.getFileType());
        Assert.assertEquals("isRelativePath mismatch", expected.isRelativePath(), actual.isRelativePath());
        Assert.assertEquals("hashRingSignature mismatch",
                expected.getHashRingSignature(), actual.getHashRingSignature());
    }

    /**
     * Asserts that two DataCacheTableMeta objects are equal field-by-field.
     */
    public static void assertTableMetaEquals(DataCacheTableMeta expected, DataCacheTableMeta actual) {
        Assert.assertEquals("tableId mismatch", expected.getTableId(), actual.getTableId());
        Assert.assertEquals("catalogName mismatch", expected.getCatalogName(), actual.getCatalogName());
        Assert.assertEquals("dbName mismatch", expected.getDbName(), actual.getDbName());
        Assert.assertEquals("tableName mismatch", expected.getTableName(), actual.getTableName());
        Assert.assertEquals("tableType mismatch", expected.getTableType(), actual.getTableType());
        assertLocalDateTimeEquals("createdTime", expected.getCreatedTime(), actual.getCreatedTime());
        assertLocalDateTimeEquals("updatedTime", expected.getUpdatedTime(), actual.getUpdatedTime());
        Assert.assertEquals("cacheSize mismatch", expected.getCacheSize(), actual.getCacheSize());
        Assert.assertEquals("scheduleTaskName mismatch",
                expected.getScheduleTaskName(), actual.getScheduleTaskName());
    }

    /**
     * Asserts that two DataCachePartitionMeta objects are equal field-by-field.
     */
    public static void assertPartitionMetaEquals(DataCachePartitionMeta expected, DataCachePartitionMeta actual) {
        Assert.assertEquals("tableId mismatch", expected.getTableId(), actual.getTableId());
        Assert.assertEquals("partitionKey mismatch", expected.getPartitionKey(), actual.getPartitionKey());
        Assert.assertEquals("partitionUid mismatch", expected.getPartitionUid(), actual.getPartitionUid());
        Assert.assertEquals("version mismatch", expected.getVersion(), actual.getVersion());
        Assert.assertEquals("cacheStatus mismatch", expected.getCacheStatus(), actual.getCacheStatus());
        assertLocalDateTimeEquals("createdTime", expected.getCreatedTime(), actual.getCreatedTime());
        assertLocalDateTimeEquals("lastRefreshTime", expected.getLastRefreshTime(), actual.getLastRefreshTime());
        Assert.assertEquals("expiredFiles mismatch", expected.getExpiredFiles(), actual.getExpiredFiles());
        Assert.assertEquals("expiredBytes mismatch", expected.getExpiredBytes(), actual.getExpiredBytes());
        assertLocalDateTimeEquals("ttlExpireAt", expected.getTtlExpireAt(), actual.getTtlExpireAt());
        Assert.assertEquals("partitionField mismatch", expected.getPartitionField(), actual.getPartitionField());
        Assert.assertEquals("partitionFieldType mismatch",
                expected.getPartitionFieldType(), actual.getPartitionFieldType());
        Assert.assertEquals("cacheDataSize mismatch", expected.getCacheDataSize(), actual.getCacheDataSize());
        Assert.assertEquals("partitionAbsPrefixPath mismatch",
                expected.getPartitionAbsPrefixPath(), actual.getPartitionAbsPrefixPath());
        Assert.assertEquals("partitionUnit mismatch", expected.getPartitionUnit(), actual.getPartitionUnit());
        Assert.assertEquals("partitionFieldFormat mismatch",
                expected.getPartitionFieldFormat(), actual.getPartitionFieldFormat());
        Assert.assertEquals("hashRingSignature mismatch",
                expected.getHashRingSignature(), actual.getHashRingSignature());
    }

    /**
     * Asserts that two LocalDateTime objects are equal with second precision.
     * This is needed because serialization to epoch seconds loses nanoseconds.
     */
    private static void assertLocalDateTimeEquals(String fieldName, LocalDateTime expected, LocalDateTime actual) {
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || actual == null) {
            Assert.fail(fieldName + " mismatch: one is null, the other is not. expected=" + expected + ", actual=" + actual);
        }
        // Compare with second precision (serialization loses nanoseconds)
        Assert.assertEquals(fieldName + " mismatch", expected.withNano(0), actual.withNano(0));
    }

    // Convenience aliases for shorter test code
    public static DataCacheFileMeta createFileMeta(
            long tableId, long partitionUid, long fileId, long backendId,
            String filePath, long fileSizeBytes, long offset, long length,
            long partitionVersion, long modificationTime, String fileType,
            boolean isRelativePath, String hashRingSignature) {
        return createTestFileMeta(partitionUid, fileId, backendId, filePath,
                fileSizeBytes, offset, length, tableId, partitionVersion,
                modificationTime, fileType, isRelativePath, hashRingSignature);
    }

    public static DataCacheTableMeta createTableMeta(long tableId, String catalogName,
                                                      String dbName, String tableName) {
        return createTestTableMeta(tableId, catalogName, dbName, tableName);
    }

    public static DataCachePartitionMeta createPartitionMeta(long tableId, String partitionKey,
                                                              long partitionUid, long version) {
        DataCachePartitionMeta meta = createTestPartitionMeta(tableId, partitionUid, partitionKey);
        meta.setVersion(version);
        return meta;
    }
}
