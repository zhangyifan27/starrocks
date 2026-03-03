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

import java.util.Objects;

public final class DataCacheFileMeta {
    private final long partitionUid;
    private final long fileId;
    private final long backendId;
    private final String filePath;
    private long fileSizeBytes;
    private final long offset;
    private final long length;
    private final long tableId;
    private long partitionVersion;
    private long modificationTime;
    private String fileType;
    private boolean isRelativePath;
    private String hashRingSignature;

    public DataCacheFileMeta(long partitionUid, long fileId, long backendId, String filePath,
                             long fileSizeBytes, long offset, long length, long tableId,
                             long partitionVersion, long modificationTime, String fileType,
                             boolean isRelativePath, String hashRingSignature) {
        this.partitionUid = partitionUid;
        this.fileId = fileId;
        this.backendId = backendId;
        this.filePath = filePath;
        this.fileSizeBytes = fileSizeBytes;
        this.offset = offset;
        this.length = length;
        this.tableId = tableId;
        this.partitionVersion = partitionVersion;
        this.modificationTime = modificationTime;
        this.fileType = fileType;
        this.isRelativePath = isRelativePath;
        this.hashRingSignature = hashRingSignature == null ? "" : hashRingSignature;
    }

    public boolean isRelativePath() {
        return this.isRelativePath;
    }

    public String getHashRingSignature() {
        return hashRingSignature;
    }

    public void setHashRingSignature(String hashRingSignature) {
        this.hashRingSignature = hashRingSignature;
    }

    public long getPartitionUid() {
        return partitionUid;
    }

    public long getFileId() {
        return fileId;
    }

    public long getBackendId() {
        return backendId;
    }

    public String getFilePath() {
        return filePath;
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }

    public long getOffset() {
        return offset;
    }

    public long getLength() {
        return length;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionVersion() {
        return partitionVersion;
    }

    public void setPartitionVersion(long partitionVersion) {
        this.partitionVersion = partitionVersion;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public String getFileType() {
        return fileType;
    }

    public DataCacheFileMeta copy() {
        return new DataCacheFileMeta(partitionUid, fileId, backendId, filePath, fileSizeBytes,
                offset, length,
                tableId, partitionVersion, modificationTime, fileType, isRelativePath, hashRingSignature);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataCacheFileMeta)) {
            return false;
        }
        DataCacheFileMeta that = (DataCacheFileMeta) o;
        return partitionUid == that.partitionUid && fileId == that.fileId
                && backendId == that.backendId && fileSizeBytes == that.fileSizeBytes
                && offset == that.offset && length == that.length
                && tableId == that.tableId && partitionVersion == that.partitionVersion
                && Objects.equals(filePath, that.filePath)
                && Objects.equals(fileType, that.fileType)
                && Objects.equals(isRelativePath, that.isRelativePath)
                && Objects.equals(hashRingSignature, that.hashRingSignature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionUid, fileId, backendId, filePath, fileSizeBytes, offset, length, tableId,
                partitionVersion, fileType, isRelativePath, hashRingSignature);
    }
}
