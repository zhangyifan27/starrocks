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

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.RemoteFileDesc;

/**
 * Remote file descriptor synthesized from the file meta cache.
 * It keeps the file-to-backend affinity so the scheduler can place fragments accordingly.
 */
public class DataCacheRemoteFileDesc extends RemoteFileDesc {
    private final long backendId;
    private final long partitionUid;
    private final long fileId;
    private final String format;
    private final boolean isRelativePath;
    private final long offset;
    private final long length;
    private final long fileSize;


    public DataCacheRemoteFileDesc(long partitionUid, long fileId, long backendId,
                                   String filePath, long modificationTime, String format,
                                   boolean isRelativePath, long offset, long length, long fileSize) {
        super(filePath, null, 0, modificationTime, ImmutableList.of());
        this.backendId = backendId;
        this.partitionUid = partitionUid;
        this.fileId = fileId;
        this.format = format;
        this.isRelativePath = isRelativePath;
        this.offset = offset;
        this.length = length;
        this.fileSize = fileSize;
        setFullPath(filePath);
        setSplittable(false);
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getOffset() {
        return this.offset;
    }

    public long getLength() {
        return this.length;
    }

    public boolean isRelativePath() {
        return this.isRelativePath;
    }

    public long getBackendId() {
        return backendId;
    }

    public long getPartitionUid() {
        return partitionUid;
    }

    public long getFileId() {
        return fileId;
    }

    public String getFormat() {
        return format;
    }
}
