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

import com.starrocks.persist.EditLog;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Test;

public class DataCacheMetaStoreTest {

    @Test
    public void testInitialize(@Mocked DataCacheMetaCache metaCache) {
        DataCacheMetaStore store = new DataCacheMetaStore();

        // Should not throw exception
        store.initialize(metaCache);
    }

    @Test
    public void testPersistTableMeta_CallsEditLog(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked EditLog editLog) {

        DataCacheTableMeta tableMeta = DataCacheTestUtils.createTestTableMeta(12345L);

        new Expectations() {{
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getEditLog();
                result = editLog;

                editLog.logDataCacheTableMeta((DataCacheTableMetaLog) any);
                times = 1;
            }};

        DataCacheMetaStore store = new DataCacheMetaStore();
        store.persistTableMeta(tableMeta);
    }

    @Test
    public void testPersistPartitionMeta_CallsEditLog(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked EditLog editLog) {

        DataCachePartitionMeta partitionMeta = DataCacheTestUtils.createTestPartitionMeta(
                12345L, 67890L, "dt=2024-01-01");

        new Expectations() {{
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getEditLog();
                result = editLog;

                editLog.logDataCachePartitionMeta((DataCachePartitionMetaLog) any);
                times = 1;
            }};

        DataCacheMetaStore store = new DataCacheMetaStore();
        store.persistPartitionMeta(partitionMeta);
    }

    @Test
    public void testDeletePartitionMeta_CallsEditLog(
            @Mocked GlobalStateMgr globalStateMgr,
            @Mocked EditLog editLog) {

        long partitionUid = 67890L;

        new Expectations() {{
                GlobalStateMgr.getCurrentState();
                result = globalStateMgr;

                globalStateMgr.getEditLog();
                result = editLog;

                editLog.logDeleteDataCachePartitionMeta(partitionUid);
                times = 1;
            }};

        DataCacheMetaStore store = new DataCacheMetaStore();
        store.deletePartitionMeta(partitionUid);
    }
}
