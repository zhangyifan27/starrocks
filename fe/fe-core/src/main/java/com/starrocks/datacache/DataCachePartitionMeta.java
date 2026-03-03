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

import java.time.LocalDateTime;

public final class DataCachePartitionMeta {
    private final long tableId;
    private final String partitionKey;
    private long partitionUid;
    private long version;
    private String cacheStatus;
    private LocalDateTime createdTime;
    private LocalDateTime lastRefreshTime;
    private long expiredFiles;
    private long expiredBytes;
    private LocalDateTime ttlExpireAt;
    private String partitionField;
    private String partitionFieldType;
    // Note: cache data size表示分区中最新数据的cache data size
    // 如果需要查询所有cache file的大小，可以对DataCacheFileMeta的table做聚合查询
    private long cacheDataSize;
    private String partitionAbsPrefixPath;
    private String partitionUnit;
    private String partitionFieldFormat;
    private String hashRingSignature;

    public DataCachePartitionMeta(long tableId, String partitionKey, long partitionUid, long version,
                                  String cacheStatus, LocalDateTime createdTime, LocalDateTime lastRefreshTime,
                                  long expiredFiles, long expiredBytes, LocalDateTime ttlExpireAt,
                                  String partitionField, String partitionFieldType, long cacheDataSize,
                                  String partitionAbsPrefixPath, String partitionUnit, String partitionFieldFormat,
                                  String hashRingSignature) {
        this.tableId = tableId;
        this.partitionKey = partitionKey;
        this.partitionUid = partitionUid;
        this.version = version;
        this.cacheStatus = cacheStatus;
        this.createdTime = createdTime;
        this.lastRefreshTime = lastRefreshTime;
        this.expiredFiles = expiredFiles;
        this.expiredBytes = expiredBytes;
        this.ttlExpireAt = ttlExpireAt;
        this.partitionField = partitionField;
        this.partitionFieldType = partitionFieldType;
        this.cacheDataSize = cacheDataSize;
        this.partitionAbsPrefixPath = partitionAbsPrefixPath;
        this.partitionUnit = partitionUnit;
        this.partitionFieldFormat = partitionFieldFormat;
        this.hashRingSignature = hashRingSignature;
    }

    public long getTableId() {
        return tableId;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public long getPartitionUid() {
        return partitionUid;
    }

    public void setPartitionUid(long partitionUid) {
        this.partitionUid = partitionUid;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public String getCacheStatus() {
        return cacheStatus;
    }

    public void setCacheStatus(String cacheStatus) {
        this.cacheStatus = cacheStatus;
    }

    public LocalDateTime getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(LocalDateTime createdTime) {
        this.createdTime = createdTime;
    }

    public LocalDateTime getLastRefreshTime() {
        return lastRefreshTime;
    }

    public void setLastRefreshTime(LocalDateTime lastRefreshTime) {
        this.lastRefreshTime = lastRefreshTime;
    }

    public long getExpiredFiles() {
        return expiredFiles;
    }

    public void setExpiredFiles(long expiredFiles) {
        this.expiredFiles = expiredFiles;
    }

    public long getExpiredBytes() {
        return expiredBytes;
    }

    public void setExpiredBytes(long expiredBytes) {
        this.expiredBytes = expiredBytes;
    }

    public LocalDateTime getTtlExpireAt() {
        return ttlExpireAt;
    }

    public void setTtlExpireAt(LocalDateTime ttlExpireAt) {
        this.ttlExpireAt = ttlExpireAt;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

    public String getPartitionFieldType() {
        return partitionFieldType;
    }

    public void setPartitionFieldType(String partitionFieldType) {
        this.partitionFieldType = partitionFieldType;
    }

    public long getCacheDataSize() {
        return cacheDataSize;
    }

    public void setCacheDataSize(long cacheDataSize) {
        this.cacheDataSize = cacheDataSize;
    }

    public String getPartitionAbsPrefixPath() {
        return partitionAbsPrefixPath;
    }

    public void setPartitionAbsPrefixPath(String partitionAbsPrefixPath) {
        this.partitionAbsPrefixPath = partitionAbsPrefixPath;
    }

    public String getPartitionUnit() {
        return partitionUnit;
    }

    public void setPartitionUnit(String partitionUnit) {
        this.partitionUnit = partitionUnit;
    }

    public String getPartitionFieldFormat() {
        return partitionFieldFormat;
    }

    public void setPartitionFieldFormat(String partitionFieldFormat) {
        this.partitionFieldFormat = partitionFieldFormat;
    }

    public String getHashRingSignature() {
        return hashRingSignature;
    }

    public void setHashRingSignature(String hashRingSignature) {
        this.hashRingSignature = hashRingSignature;
    }

    public DataCachePartitionMeta copy() {
        return new DataCachePartitionMeta(tableId, partitionKey, partitionUid, version, cacheStatus,
                createdTime, lastRefreshTime, expiredFiles, expiredBytes, ttlExpireAt,
                partitionField, partitionFieldType, cacheDataSize, partitionAbsPrefixPath, partitionUnit,
                partitionFieldFormat, hashRingSignature);
    }
}
