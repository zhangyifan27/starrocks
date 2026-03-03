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

import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Lightweight log entity for data cache partition meta, used in edit log.
 */
public class DataCachePartitionMetaLog implements Writable {
    private DataCachePartitionMeta partitionMeta;

    public DataCachePartitionMetaLog() {
    }

    public DataCachePartitionMetaLog(DataCachePartitionMeta partitionMeta) {
        this.partitionMeta = partitionMeta;
    }

    public DataCachePartitionMeta getPartitionMeta() {
        return partitionMeta;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(partitionMeta.getTableId());
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionKey()));
        out.writeLong(partitionMeta.getPartitionUid());
        out.writeLong(partitionMeta.getVersion());
        out.writeUTF(nullToEmpty(partitionMeta.getCacheStatus()));
        out.writeLong(partitionMeta.getCreatedTime() == null
                ? -1L : partitionMeta.getCreatedTime().toEpochSecond(ZoneOffset.UTC));
        out.writeLong(partitionMeta.getLastRefreshTime() == null
                ? -1L : partitionMeta.getLastRefreshTime().toEpochSecond(ZoneOffset.UTC));
        out.writeLong(partitionMeta.getExpiredFiles());
        out.writeLong(partitionMeta.getExpiredBytes());
        out.writeLong(partitionMeta.getTtlExpireAt() == null
                ? -1L : partitionMeta.getTtlExpireAt().toEpochSecond(ZoneOffset.UTC));
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionField()));
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionFieldType()));
        out.writeLong(partitionMeta.getCacheDataSize());
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionAbsPrefixPath()));
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionUnit()));
        out.writeUTF(nullToEmpty(partitionMeta.getPartitionFieldFormat()));
        out.writeUTF(nullToEmpty(partitionMeta.getHashRingSignature()));
    }

    public void readFields(DataInput in) throws IOException {
        long tableId = in.readLong();
        String partitionKey = emptyToNull(in.readUTF());
        long partitionUid = in.readLong();
        long version = in.readLong();
        String cacheStatus = emptyToNull(in.readUTF());
        long createdEpoch = in.readLong();
        long lastRefreshEpoch = in.readLong();
        long expiredFiles = in.readLong();
        long expiredBytes = in.readLong();
        long ttlEpoch = in.readLong();
        String partitionField = emptyToNull(in.readUTF());
        String partitionFieldType = emptyToNull(in.readUTF());
        long cacheDataSize = in.readLong();
        String partitionAbsPrefixPath = emptyToNull(in.readUTF());
        String partitionUnit = emptyToNull(in.readUTF());
        String partitionFormat = emptyToNull(in.readUTF());
        String hashRingSignature;
        try {
            hashRingSignature = in.readUTF();
        } catch (EOFException eof) {
            hashRingSignature = "";
        }

        LocalDateTime createdTime = createdEpoch < 0 ? null
                : LocalDateTime.ofEpochSecond(createdEpoch, 0, ZoneOffset.UTC);
        LocalDateTime lastRefreshTime = lastRefreshEpoch < 0 ? null
                : LocalDateTime.ofEpochSecond(lastRefreshEpoch, 0, ZoneOffset.UTC);
        LocalDateTime ttlExpireAt = ttlEpoch < 0 ? null
                : LocalDateTime.ofEpochSecond(ttlEpoch, 0, ZoneOffset.UTC);

        partitionMeta = new DataCachePartitionMeta(tableId, partitionKey, partitionUid, version,
                cacheStatus, createdTime, lastRefreshTime, expiredFiles, expiredBytes, ttlExpireAt, partitionField,
                partitionFieldType, cacheDataSize, partitionAbsPrefixPath, partitionUnit, partitionFormat,
                hashRingSignature);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    private static String emptyToNull(String s) {
        return s == null || s.isEmpty() ? null : s;
    }
}
