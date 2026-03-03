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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In-memory mock implementation of DataCacheFileMetaStore for unit testing.
 * Provides fast, deterministic storage without requiring actual SQL execution.
 */
public class MockDataCacheFileMetaStore extends DataCacheFileMetaStore {

    private final Map<Long, List<DataCacheFileMeta>> storage = new ConcurrentHashMap<>();

    public MockDataCacheFileMetaStore() {
        super("datacache_meta_db", "datacache_file_meta");
    }

    @Override
    public void ensureTable() {
        // No-op for mock - table always "exists"
    }

    @Override
    public void insertFileMeta(DataCacheFileMeta entry) {
        storage.computeIfAbsent(entry.getPartitionUid(), k -> new ArrayList<>())
                .add(entry.copy());
    }

    @Override
    public List<DataCacheFileMeta> queryFileMeta(long partitionUid) {
        return storage.getOrDefault(partitionUid, Collections.emptyList())
                .stream()
                .map(DataCacheFileMeta::copy)
                .collect(Collectors.toList());
    }

    @Override
    public long[] queryExpiredStats(long tableId, long partitionUid, long currentVersion,
                                    String currentHashRingSignature) {
        List<DataCacheFileMeta> files = storage.getOrDefault(partitionUid, Collections.emptyList());
        long expiredFiles = 0;
        long expiredBytes = 0;

        for (DataCacheFileMeta file : files) {
            if (file.getPartitionVersion() != currentVersion ||
                    !Objects.equals(file.getHashRingSignature(), currentHashRingSignature)) {
                expiredFiles++;
                expiredBytes += file.getFileSizeBytes();
            }
        }

        return new long[] {expiredFiles, expiredBytes};
    }

    @Override
    public List<DataCacheFileMeta> queryExpiredFileMeta(long partitionUid, long partitionVersion,
                                                        String latestHashRingSignature) {
        List<DataCacheFileMeta> files = storage.getOrDefault(partitionUid, Collections.emptyList());

        // Current files: version and hash ring match
        List<DataCacheFileMeta> currentFiles = files.stream()
                .filter(f -> f.getPartitionVersion() == partitionVersion &&
                        Objects.equals(f.getHashRingSignature(), latestHashRingSignature))
                .collect(Collectors.toList());

        // History files: version or hash ring don't match
        List<DataCacheFileMeta> historyFiles = files.stream()
                .filter(f -> f.getPartitionVersion() != partitionVersion ||
                        !Objects.equals(f.getHashRingSignature(), latestHashRingSignature))
                .collect(Collectors.toList());

        // Expired files: in history but not in current (based on fileId + backendId)
        return historyFiles.stream()
                .filter(h -> currentFiles.stream()
                        .noneMatch(c -> c.getFileId() == h.getFileId() &&
                                       c.getBackendId() == h.getBackendId()))
                .map(DataCacheFileMeta::copy)
                .collect(Collectors.toList());
    }

    @Override
    public void deleteForPartition(long partitionUid) {
        storage.remove(partitionUid);
    }

    @Override
    public void deleteExpiredFileMeta(long partitionUid, long currentVersion, String currentHashRingSignature) {
        List<DataCacheFileMeta> files = storage.get(partitionUid);
        if (files == null) {
            return;
        }

        List<DataCacheFileMeta> toKeep = files.stream()
                .filter(f -> f.getPartitionVersion() == currentVersion &&
                        Objects.equals(f.getHashRingSignature(), currentHashRingSignature))
                .collect(Collectors.toList());

        if (toKeep.isEmpty()) {
            storage.remove(partitionUid);
        } else {
            storage.put(partitionUid, toKeep);
        }
    }

    /**
     * Clears all stored file metadata. Useful for test cleanup.
     */
    public void clear() {
        storage.clear();
    }

    /**
     * Returns the total number of files stored across all partitions.
     */
    public int getTotalFileCount() {
        return storage.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Returns the number of partitions stored.
     */
    public int getPartitionCount() {
        return storage.size();
    }

    /**
     * Returns whether the store contains any files for the given partition.
     */
    public boolean hasPartition(long partitionUid) {
        return storage.containsKey(partitionUid);
    }
}
