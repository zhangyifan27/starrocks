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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Test suite for DataCacheTableMetaLog serialization.
 */
public class DataCacheTableMetaLogTest {

    @Test
    public void testWriteAndRead_AllFields() throws Exception {
        DataCacheTableMeta original = DataCacheTestUtils.createTestTableMeta(12345L);
        original.setCacheSize(1024L * 1024);
        original.setScheduleTaskName("test_task");

        DataCacheTableMetaLog log = new DataCacheTableMetaLog(original);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        log.write(out);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCacheTableMetaLog readLog = new DataCacheTableMetaLog();
        readLog.readFields(in);

        // Verify
        DataCacheTableMeta deserialized = readLog.getTableMeta();
        DataCacheTestUtils.assertTableMetaEquals(original, deserialized);
    }

    @Test
    public void testWriteAndRead_NullLocalDateTime() throws Exception {
        DataCacheTableMeta meta = new DataCacheTableMeta(
                12345L, "hive0", "test_db", "test_table", "HIVE",
                null, null, 0L, null);

        DataCacheTableMetaLog log = new DataCacheTableMetaLog(meta);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        log.write(out);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCacheTableMetaLog readLog = new DataCacheTableMetaLog();
        readLog.readFields(in);

        // Verify null timestamps
        DataCacheTableMeta deserialized = readLog.getTableMeta();
        Assert.assertNull(deserialized.getCreatedTime());
        Assert.assertNull(deserialized.getUpdatedTime());
    }

    @Test
    public void testBackwardCompatibility_MissingScheduleTaskName() throws Exception {
        DataCacheTableMeta original = DataCacheTestUtils.createTestTableMeta(12345L);

        // Serialize without schedule task name (simulate old version)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeLong(original.getTableId());
        out.writeUTF(original.getCatalogName());
        out.writeUTF(original.getDbName());
        out.writeUTF(original.getTableName());
        out.writeUTF(original.getTableType());
        out.writeLong(original.getCreatedTime().toEpochSecond(java.time.ZoneOffset.UTC));
        out.writeLong(original.getUpdatedTime().toEpochSecond(java.time.ZoneOffset.UTC));
        out.writeLong(original.getCacheSize());
        // Don't write scheduleTaskName (simulating old format)

        // Deserialize with new code
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCacheTableMetaLog readLog = new DataCacheTableMetaLog();
        readLog.readFields(in);

        // Verify schedule task name is null (backward compatible)
        DataCacheTableMeta deserialized = readLog.getTableMeta();
        Assert.assertNull(deserialized.getScheduleTaskName());
        Assert.assertEquals(original.getTableId(), deserialized.getTableId());
    }
}
