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
import java.time.LocalDateTime;

/**
 * Test suite for DataCachePartitionMetaLog serialization.
 */
public class DataCachePartitionMetaLogTest {

    @Test
    public void testWriteAndRead_AllFields() throws Exception {
        DataCachePartitionMeta original = DataCacheTestUtils.createTestPartitionMeta(
                12345L, 67890L, "dt=2024-01-01");
        original.setExpiredFiles(10L);
        original.setExpiredBytes(1024L * 1024);
        original.setTtlExpireAt(LocalDateTime.now().plusDays(7));

        DataCachePartitionMetaLog log = new DataCachePartitionMetaLog(original);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        log.write(out);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCachePartitionMetaLog readLog = new DataCachePartitionMetaLog();
        readLog.readFields(in);

        // Verify
        DataCachePartitionMeta deserialized = readLog.getPartitionMeta();
        DataCacheTestUtils.assertPartitionMetaEquals(original, deserialized);
    }

    @Test
    public void testWriteAndRead_NullLocalDateTime() throws Exception {
        DataCachePartitionMeta meta = new DataCachePartitionMeta(
                12345L, "dt=2024-01-01", 67890L, 1L, "CACHED",
                null, null, 0L, 0L, null,
                "dt", "DATE", 1024L, null, "DAY", "yyyy-MM-dd", "worker1");

        DataCachePartitionMetaLog log = new DataCachePartitionMetaLog(meta);

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        log.write(out);

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCachePartitionMetaLog readLog = new DataCachePartitionMetaLog();
        readLog.readFields(in);

        // Verify null timestamps
        DataCachePartitionMeta deserialized = readLog.getPartitionMeta();
        Assert.assertNull(deserialized.getCreatedTime());
        Assert.assertNull(deserialized.getLastRefreshTime());
        Assert.assertNull(deserialized.getTtlExpireAt());
    }

    @Test
    public void testBackwardCompatibility_MissingHashRingSignature() throws Exception {
        DataCachePartitionMeta original = DataCacheTestUtils.createTestPartitionMeta(
                12345L, 67890L, "dt=2024-01-01");

        // Serialize without hash ring signature (simulate old version)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        out.writeLong(original.getTableId());
        out.writeUTF(original.getPartitionKey());
        out.writeLong(original.getPartitionUid());
        out.writeLong(original.getVersion());
        out.writeUTF(original.getCacheStatus());
        out.writeLong(original.getCreatedTime().toEpochSecond(java.time.ZoneOffset.UTC));
        out.writeLong(original.getLastRefreshTime().toEpochSecond(java.time.ZoneOffset.UTC));
        out.writeLong(original.getExpiredFiles());
        out.writeLong(original.getExpiredBytes());
        out.writeLong(-1L); // ttlExpireAt null
        out.writeUTF(original.getPartitionField());
        out.writeUTF(original.getPartitionFieldType());
        out.writeLong(original.getCacheDataSize());
        out.writeUTF(""); // partitionAbsPrefixPath
        out.writeUTF(original.getPartitionUnit());
        out.writeUTF(original.getPartitionFieldFormat());
        // Don't write hashRingSignature (simulating old format)

        // Deserialize with new code
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        DataInputStream in = new DataInputStream(bais);
        DataCachePartitionMetaLog readLog = new DataCachePartitionMetaLog();
        readLog.readFields(in);

        // Verify hash ring signature defaults to empty string (backward compatible)
        DataCachePartitionMeta deserialized = readLog.getPartitionMeta();
        Assert.assertEquals("", deserialized.getHashRingSignature());
        Assert.assertEquals(original.getTableId(), deserialized.getTableId());
    }
}
