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

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory cache for data cache metadata (table / partition / job).
 *
 * <p>This class owns the maps that represent the current metadata view in FE
 * and is intentionally kept independent from the persistence layer (BDB /
 * OLAP table). {@link DataCacheMetaManager} delegates metadata access and
 * modification to this cache and is responsible for triggering persistence.
 */
final class DataCacheMetaCache {

    private final Map<Long, DataCacheTableMeta> tableMetaById = new HashMap<>();
    private final Map<Long, DataCachePartitionMeta> partitionMetaByUid = new HashMap<>();

    // -------- Table meta --------

    DataCacheTableMeta getTableMeta(long tableId) {
        return tableMetaById.get(tableId);
    }

    void putTableMeta(DataCacheTableMeta meta) {
        tableMetaById.put(meta.getTableId(), meta);
    }

    boolean containsTable(long tableId) {
        return tableMetaById.containsKey(tableId);
    }

    Map<Long, DataCacheTableMeta> getTableMetaById() {
        return tableMetaById;
    }

    // -------- Partition meta --------

    DataCachePartitionMeta getPartitionMeta(long partitionUid) {
        return partitionMetaByUid.get(partitionUid);
    }

    void putPartitionMeta(DataCachePartitionMeta meta) {
        partitionMetaByUid.put(meta.getPartitionUid(), meta);
    }

    void removePartitionMeta(long partitionUid) {
        partitionMetaByUid.remove(partitionUid);
    }

    boolean containsPartition(long partitionUid) {
        return partitionMetaByUid.containsKey(partitionUid);
    }

    Map<Long, DataCachePartitionMeta> getPartitionMetaByUid() {
        return partitionMetaByUid;
    }

    void clear() {
        tableMetaById.clear();
        partitionMetaByUid.clear();
    }
}
