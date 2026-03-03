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
import org.junit.Test;

/**
 * Test suite for DataCacheRemoteFileDesc - the remote file descriptor synthesized from file meta cache.
 */
public class DataCacheRemoteFileDescTest {

    @Test
    public void testNotSplittable() {
        DataCacheRemoteFileDesc desc = new DataCacheRemoteFileDesc(
                1L, 2L, 3L, "file.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 1024L, 1024L
        );

        // DataCacheRemoteFileDesc sets splittable to false
        Assert.assertFalse(desc.isSplittable());
    }

    @Test
    public void testBackendAffinity() {
        long backend1 = 10001L;
        long backend2 = 10002L;
        long backend3 = 10003L;

        DataCacheRemoteFileDesc desc1 = new DataCacheRemoteFileDesc(
                1L, 100L, backend1, "file1.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 1024L, 1024L
        );
        DataCacheRemoteFileDesc desc2 = new DataCacheRemoteFileDesc(
                1L, 101L, backend2, "file2.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 1024L, 1024L
        );
        DataCacheRemoteFileDesc desc3 = new DataCacheRemoteFileDesc(
                1L, 102L, backend3, "file3.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 1024L, 1024L
        );

        // Each file descriptor maintains its backend affinity
        Assert.assertEquals(backend1, desc1.getBackendId());
        Assert.assertEquals(backend2, desc2.getBackendId());
        Assert.assertEquals(backend3, desc3.getBackendId());
    }

    @Test
    public void testSamePartitionDifferentFiles() {
        long partitionUid = 5000L;

        DataCacheRemoteFileDesc file1 = new DataCacheRemoteFileDesc(
                partitionUid, 1L, 10001L, "file1.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 1024L, 1024L
        );
        DataCacheRemoteFileDesc file2 = new DataCacheRemoteFileDesc(
                partitionUid, 2L, 10001L, "file2.parquet", System.currentTimeMillis(),
                "PARQUET", true, 0L, 2048L, 2048L
        );

        // Both belong to same partition
        Assert.assertEquals(partitionUid, file1.getPartitionUid());
        Assert.assertEquals(partitionUid, file2.getPartitionUid());

        // But different file IDs
        Assert.assertNotEquals(file1.getFileId(), file2.getFileId());
    }

    @Test
    public void testPartialFileScan() {
        long fileSize = 1024L * 1024 * 100; // 100MB file
        long offset = 1024L * 1024 * 20; // Start at 20MB
        long length = 1024L * 1024 * 30; // Read 30MB

        DataCacheRemoteFileDesc desc = new DataCacheRemoteFileDesc(
                1L, 2L, 3L, "file.parquet", System.currentTimeMillis(),
                "PARQUET", true, offset, length, fileSize
        );

        Assert.assertEquals(offset, desc.getOffset());
        Assert.assertEquals(length, desc.getLength());
        Assert.assertEquals(fileSize, desc.getFileSize());

        // Verify offset + length <= fileSize
        Assert.assertTrue(offset + length <= fileSize);
    }
}
