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

import java.time.LocalDateTime;

/**
 * Test suite for DataCachePartitionMeta - the metadata object for cached partitions.
 */
public class DataCachePartitionMetaTest {

    @Test
    public void testCopy_CreatesSeparateInstance() {
        DataCachePartitionMeta original = new DataCachePartitionMeta(
                100L, "dt=2024-06-15", 5000L, 10L, "ACTIVE",
                LocalDateTime.now(), LocalDateTime.now(), 3L, 1024L,
                LocalDateTime.now().plusDays(7), "dt", "DATE", 2048L,
                "/path/to/partition", "DAY", "yyyy-MM-dd", "sig-abc"
        );

        DataCachePartitionMeta copy = original.copy();

        // Different instances
        Assert.assertNotSame(original, copy);

        // But same content
        Assert.assertEquals(original.getTableId(), copy.getTableId());
        Assert.assertEquals(original.getPartitionKey(), copy.getPartitionKey());
        Assert.assertEquals(original.getPartitionUid(), copy.getPartitionUid());
        Assert.assertEquals(original.getVersion(), copy.getVersion());
        Assert.assertEquals(original.getCacheStatus(), copy.getCacheStatus());
        Assert.assertEquals(original.getPartitionField(), copy.getPartitionField());
        Assert.assertEquals(original.getHashRingSignature(), copy.getHashRingSignature());
    }

    @Test
    public void testCopy_MutatingCopyDoesNotAffectOriginal() {
        DataCachePartitionMeta original = DataCacheTestUtils.createPartitionMeta(
                1L, "dt=2024-01-01", 1000L, 5L
        );
        DataCachePartitionMeta copy = original.copy();

        // Mutate the copy
        copy.setVersion(999L);
        copy.setCacheStatus("MUTATED");
        copy.setExpiredFiles(777L);
        copy.setExpiredBytes(888888L);
        copy.setCacheDataSize(123456789L);
        copy.setHashRingSignature("mutated-sig");

        // Original should be unchanged
        Assert.assertNotEquals(999L, original.getVersion());
        Assert.assertNotEquals("MUTATED", original.getCacheStatus());
        Assert.assertNotEquals(777L, original.getExpiredFiles());
        Assert.assertNotEquals(888888L, original.getExpiredBytes());
        Assert.assertNotEquals(123456789L, original.getCacheDataSize());
        Assert.assertNotEquals("mutated-sig", original.getHashRingSignature());
    }

    @Test
    public void testVariousPartitionKeyFormats() {
        // Hive-style partition key
        DataCachePartitionMeta hiveMeta = DataCacheTestUtils.createPartitionMeta(
                1L, "year=2024/month=01/day=15", 100L, 1L
        );
        Assert.assertEquals("year=2024/month=01/day=15", hiveMeta.getPartitionKey());

        // Simple date partition
        DataCachePartitionMeta dateMeta = DataCacheTestUtils.createPartitionMeta(
                1L, "20240115", 200L, 1L
        );
        Assert.assertEquals("20240115", dateMeta.getPartitionKey());

        // Full table (no partition)
        DataCachePartitionMeta fullTableMeta = DataCacheTestUtils.createPartitionMeta(
                1L, "__FULL_TABLE__", 300L, 1L
        );
        Assert.assertEquals("__FULL_TABLE__", fullTableMeta.getPartitionKey());
    }
}
