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

import java.util.Map;

/**
 * Test suite for DataCacheMetaCache.
 */
public class DataCacheMetaCacheTest {

    private DataCacheMetaCache cache;

    @Before
    public void setUp() {
        cache = new DataCacheMetaCache();
    }

    @Test
    public void testTableMeta_PutAndGet() {
        long tableId = 12345L;
        DataCacheTableMeta meta = DataCacheTestUtils.createTestTableMeta(tableId);

        cache.putTableMeta(meta);

        DataCacheTableMeta retrieved = cache.getTableMeta(tableId);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(tableId, retrieved.getTableId());
        Assert.assertEquals("hive0", retrieved.getCatalogName());
        Assert.assertEquals("test_db", retrieved.getDbName());
        Assert.assertEquals("test_table", retrieved.getTableName());
    }

    @Test
    public void testTableMeta_GetAll() {
        DataCacheTableMeta meta1 = DataCacheTestUtils.createTestTableMeta(1L);
        DataCacheTableMeta meta2 = DataCacheTestUtils.createTestTableMeta(2L);
        DataCacheTableMeta meta3 = DataCacheTestUtils.createTestTableMeta(3L);

        cache.putTableMeta(meta1);
        cache.putTableMeta(meta2);
        cache.putTableMeta(meta3);

        Map<Long, DataCacheTableMeta> allTables = cache.getTableMetaById();
        Assert.assertEquals(3, allTables.size());
        Assert.assertTrue(allTables.containsKey(1L));
        Assert.assertTrue(allTables.containsKey(2L));
        Assert.assertTrue(allTables.containsKey(3L));
    }

    @Test
    public void testPartitionMeta_PutGetRemove() {
        long partitionUid = 67890L;
        DataCachePartitionMeta meta = DataCacheTestUtils.createTestPartitionMeta(
                12345L, partitionUid, "dt=2024-01-01");

        cache.putPartitionMeta(meta);
        Assert.assertTrue(cache.containsPartition(partitionUid));

        DataCachePartitionMeta retrieved = cache.getPartitionMeta(partitionUid);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(partitionUid, retrieved.getPartitionUid());
        Assert.assertEquals("dt=2024-01-01", retrieved.getPartitionKey());

        cache.removePartitionMeta(partitionUid);
        Assert.assertFalse(cache.containsPartition(partitionUid));
        Assert.assertNull(cache.getPartitionMeta(partitionUid));
    }

    @Test
    public void testClear() {
        DataCacheTableMeta tableMeta = DataCacheTestUtils.createTestTableMeta(1L);
        DataCachePartitionMeta partitionMeta = DataCacheTestUtils.createTestPartitionMeta(
                1L, 101L, "dt=2024-01-01");

        cache.putTableMeta(tableMeta);
        cache.putPartitionMeta(partitionMeta);

        Assert.assertTrue(cache.containsTable(1L));
        Assert.assertTrue(cache.containsPartition(101L));

        cache.clear();

        Assert.assertFalse(cache.containsTable(1L));
        Assert.assertFalse(cache.containsPartition(101L));
        Assert.assertEquals(0, cache.getTableMetaById().size());
        Assert.assertEquals(0, cache.getPartitionMetaByUid().size());
    }
}
