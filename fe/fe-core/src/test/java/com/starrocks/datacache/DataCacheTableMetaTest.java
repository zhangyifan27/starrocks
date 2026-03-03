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
 * Test suite for DataCacheTableMeta - the metadata object for cached tables.
 */
public class DataCacheTableMetaTest {

    @Test
    public void testCopy_CreatesSeparateInstance() {
        DataCacheTableMeta original = new DataCacheTableMeta(
                100L, "catalog1", "db1", "table1", "EXTERNAL",
                LocalDateTime.now(), LocalDateTime.now(), 5000L, "task1"
        );

        DataCacheTableMeta copy = original.copy();

        // Different instances
        Assert.assertNotSame(original, copy);

        // But same content
        Assert.assertEquals(original.getTableId(), copy.getTableId());
        Assert.assertEquals(original.getCatalogName(), copy.getCatalogName());
        Assert.assertEquals(original.getDbName(), copy.getDbName());
        Assert.assertEquals(original.getTableName(), copy.getTableName());
        Assert.assertEquals(original.getTableType(), copy.getTableType());
        Assert.assertEquals(original.getCreatedTime(), copy.getCreatedTime());
        Assert.assertEquals(original.getUpdatedTime(), copy.getUpdatedTime());
        Assert.assertEquals(original.getCacheSize(), copy.getCacheSize());
        Assert.assertEquals(original.getScheduleTaskName(), copy.getScheduleTaskName());
    }

    @Test
    public void testCopy_MutatingCopyDoesNotAffectOriginal() {
        DataCacheTableMeta original = DataCacheTestUtils.createTableMeta(1L, "cat", "db", "tbl");
        DataCacheTableMeta copy = original.copy();

        // Mutate the copy
        copy.setCacheSize(999999L);
        copy.setScheduleTaskName("mutated_task");
        copy.setUpdatedTime(LocalDateTime.of(2099, 12, 31, 23, 59, 59));

        // Original should be unchanged
        Assert.assertNotEquals(999999L, original.getCacheSize());
        Assert.assertNotEquals("mutated_task", original.getScheduleTaskName());
        Assert.assertNotEquals(LocalDateTime.of(2099, 12, 31, 23, 59, 59), original.getUpdatedTime());
    }
}
