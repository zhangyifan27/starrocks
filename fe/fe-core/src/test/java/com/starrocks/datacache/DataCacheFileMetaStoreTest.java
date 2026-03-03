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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * Test suite for DataCacheFileMetaStore using the mock implementation.
 */
public class DataCacheFileMetaStoreTest {

    private MockDataCacheFileMetaStore store;

    @Before
    public void setUp() {
        store = new MockDataCacheFileMetaStore();
    }

    @Test
    public void testInsertAndQueryFileMeta() {
        long partitionUid = 1000L;
        DataCacheFileMeta file1 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 10L, 10001L, "file1.parquet", 1024L, 0L, 1024L, 1L,
                System.currentTimeMillis(), "PARQUET", true, "sig1"
        );

        store.insertFileMeta(file1);

        List<DataCacheFileMeta> queried = store.queryFileMeta(partitionUid);
        Assert.assertEquals(1, queried.size());
        Assert.assertEquals(file1.getFileId(), queried.get(0).getFileId());
        Assert.assertEquals(file1.getBackendId(), queried.get(0).getBackendId());
    }

    @Test
    public void testDeleteForPartition() {
        long partitionUid = 4000L;

        DataCacheFileMeta file1 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 10L, 10001L, "file1.parquet", 1024L, 0L, 1024L, 1L,
                System.currentTimeMillis(), "PARQUET", true, "sig1"
        );
        DataCacheFileMeta file2 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 11L, 10002L, "file2.parquet", 2048L, 0L, 2048L, 1L,
                System.currentTimeMillis(), "PARQUET", true, "sig1"
        );

        store.insertFileMeta(file1);
        store.insertFileMeta(file2);

        Assert.assertEquals(2, store.queryFileMeta(partitionUid).size());

        // Delete partition
        store.deleteForPartition(partitionUid);

        Assert.assertTrue(store.queryFileMeta(partitionUid).isEmpty());
    }

    @Test
    public void testQueryExpiredStats_WithExpiredFiles() {
        long tableId = 1L;
        long partitionUid = 6000L;
        long currentVersion = 10L;
        String currentHashRing = "sig-current";

        // Current files
        DataCacheFileMeta currentFile = DataCacheTestUtils.createFileMeta(
                tableId, partitionUid, 100L, 10001L, "file1.parquet", 1024L, 0L, 1024L,
                currentVersion, System.currentTimeMillis(), "PARQUET", true, currentHashRing
        );

        // Expired by version
        DataCacheFileMeta expiredByVersion = DataCacheTestUtils.createFileMeta(
                tableId, partitionUid, 101L, 10002L, "file2.parquet", 2048L, 0L, 2048L,
                9L, System.currentTimeMillis(), "PARQUET", true, currentHashRing
        );

        // Expired by hash ring
        DataCacheFileMeta expiredByHashRing = DataCacheTestUtils.createFileMeta(
                tableId, partitionUid, 102L, 10003L, "file3.parquet", 4096L, 0L, 4096L,
                currentVersion, System.currentTimeMillis(), "PARQUET", true, "sig-old"
        );

        store.insertFileMeta(currentFile);
        store.insertFileMeta(expiredByVersion);
        store.insertFileMeta(expiredByHashRing);

        long[] stats = store.queryExpiredStats(tableId, partitionUid, currentVersion, currentHashRing);

        Assert.assertEquals(2L, stats[0]); // 2 expired files
        Assert.assertEquals(2048L + 4096L, stats[1]); // sum of expired bytes
    }

    @Test
    public void testDeleteExpiredFileMeta() {
        long partitionUid = 10000L;
        long currentVersion = 10L;
        String currentHashRing = "sig-current";

        // Current files
        DataCacheFileMeta current1 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 100L, 10001L, "file1.parquet", 1024L, 0L, 1024L,
                currentVersion, System.currentTimeMillis(), "PARQUET", true, currentHashRing
        );
        DataCacheFileMeta current2 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 101L, 10002L, "file2.parquet", 2048L, 0L, 2048L,
                currentVersion, System.currentTimeMillis(), "PARQUET", true, currentHashRing
        );

        // Expired files
        DataCacheFileMeta expired1 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 102L, 10003L, "file3.parquet", 4096L, 0L, 4096L,
                9L, System.currentTimeMillis(), "PARQUET", true, currentHashRing
        );
        DataCacheFileMeta expired2 = DataCacheTestUtils.createFileMeta(
                1L, partitionUid, 103L, 10004L, "file4.parquet", 8192L, 0L, 8192L,
                currentVersion, System.currentTimeMillis(), "PARQUET", true, "sig-old"
        );

        store.insertFileMeta(current1);
        store.insertFileMeta(current2);
        store.insertFileMeta(expired1);
        store.insertFileMeta(expired2);

        Assert.assertEquals(4, store.queryFileMeta(partitionUid).size());

        // Delete expired
        store.deleteExpiredFileMeta(partitionUid, currentVersion, currentHashRing);

        // Only current files should remain
        List<DataCacheFileMeta> remaining = store.queryFileMeta(partitionUid);
        Assert.assertEquals(2, remaining.size());
    }
}
