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
 * Test suite for DataCacheFileMeta.
 */
public class DataCacheFileMetaTest {

    @Test
    public void testCopy_DeepCopy() {
        DataCacheFileMeta original = DataCacheTestUtils.createTestFileMeta(1L, 2L, 3L);
        DataCacheFileMeta copy = original.copy();

        Assert.assertNotSame(original, copy);
        DataCacheTestUtils.assertFileMetaEquals(original, copy);

        // Modify copy's mutable fields
        copy.setFileSizeBytes(2048L);
        copy.setPartitionVersion(99L);
        copy.setHashRingSignature("worker3");

        // Original should remain unchanged
        Assert.assertNotEquals(copy.getFileSizeBytes(), original.getFileSizeBytes());
        Assert.assertNotEquals(copy.getPartitionVersion(), original.getPartitionVersion());
        Assert.assertNotEquals(copy.getHashRingSignature(), original.getHashRingSignature());
    }

    @Test
    public void testEquals_SameValues() {
        DataCacheFileMeta meta1 = DataCacheTestUtils.createTestFileMeta(1L, 2L, 3L);
        DataCacheFileMeta meta2 = DataCacheTestUtils.createTestFileMeta(1L, 2L, 3L);

        Assert.assertEquals(meta1, meta2);
        Assert.assertEquals(meta1.hashCode(), meta2.hashCode());
    }

    @Test
    public void testEquals_DifferentValues() {
        DataCacheFileMeta meta1 = DataCacheTestUtils.createTestFileMeta(1L, 2L, 3L);
        DataCacheFileMeta meta2 = DataCacheTestUtils.createTestFileMeta(1L, 2L, 4L);  // Different backend ID

        Assert.assertNotEquals(meta1, meta2);
    }

    @Test
    public void testNullHashRingSignature_ConvertedToEmpty() {
        DataCacheFileMeta meta = new DataCacheFileMeta(
                1L, 2L, 3L, "/test/file.parquet", 1024L, 0L, 1024L,
                4L, 5L, System.currentTimeMillis(), "PARQUET", false, null);

        Assert.assertEquals("", meta.getHashRingSignature());
    }
}
