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

import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Store responsible for persisting non-file data cache metadata via edit log.
 */
public class DataCacheMetaStore {
    private static final Logger LOG = LogManager.getLogger(DataCacheMetaStore.class);

    public void initialize(DataCacheMetaCache metaCache) {
        // No-op. All persistence is handled via edit log replay.
        LOG.info("DataCacheMetaStore initialized with edit log backend");
    }

    public void persistTableMeta(DataCacheTableMeta row) {
        GlobalStateMgr.getCurrentState().getEditLog().logDataCacheTableMeta(new DataCacheTableMetaLog(row));
    }

    public void persistPartitionMeta(DataCachePartitionMeta row) {
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDataCachePartitionMeta(new DataCachePartitionMetaLog(row));
    }

    public void deletePartitionMeta(long partitionUid) {
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDeleteDataCachePartitionMeta(partitionUid);
    }
}
