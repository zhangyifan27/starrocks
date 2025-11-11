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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/system/SystemInfoService.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableMap;
import com.starrocks.common.Config;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Tests snapshot selection logic for previousCacheComputeNodes inside and outside the retention window.
 * This test uses a lightweight fake SystemInfoService to avoid dependency on GlobalStateMgr initialization.
 */
public class DefaultWorkerProviderCacheSnapshotTest {

    private String originalCacheNodeMode;

    @Before
    public void setUp() {
        originalCacheNodeMode = Config.cache_node_mode;
        Config.cache_node_mode = "cn";
    }

    @After
    public void tearDown() {
        Config.cache_node_mode = originalCacheNodeMode;
    }

    private static class FakeSystemInfoService extends SystemInfoService {
        private ImmutableMap<Long, ComputeNode> current;
        private ImmutableMap<Long, ComputeNode> previous;
        private AtomicLong cacheTime = new AtomicLong(System.currentTimeMillis());

        public void init(long firstId, int datacacheCount, int otherCount) {
            ImmutableMap.Builder<Long, ComputeNode> b = ImmutableMap.builder();
            long id = firstId;
            for (int i = 0; i < datacacheCount; i++) {
                ComputeNode n = new ComputeNode(id++, "dc" + i, 10000 + i);
                n.setWarehouseId(WarehouseManager.DATACACHE_WAREHOUSE_ID);
                b.put(n.getId(), n);
            }
            for (int i = 0; i < otherCount; i++) {
                ComputeNode n = new ComputeNode(id++, "ow" + i, 11000 + i);
                n.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
                b.put(n.getId(), n);
            }
            current = b.build();
            previous = current;
        }

        public void addDatacacheNode(long newId) {
            Map<Long, ComputeNode> datacacheSnapshot = current.values().stream()
                    .filter(n -> n.getWarehouseId() == WarehouseManager.DATACACHE_WAREHOUSE_ID)
                    .collect(Collectors.toMap(ComputeNode::getId, n -> n));
            previous = ImmutableMap.copyOf(datacacheSnapshot);
            ComputeNode n = new ComputeNode(newId, "dc_new", 12000);
            n.setWarehouseId(WarehouseManager.DATACACHE_WAREHOUSE_ID);
            ImmutableMap.Builder<Long, ComputeNode> b = ImmutableMap.builder();
            current.forEach(b::put);
            b.put(n.getId(), n);
            current = b.build();
            cacheTime.set(System.currentTimeMillis());
        }

        public void addOtherNode(long newId) {
            ComputeNode n = new ComputeNode(newId, "ow_new", 13000);
            n.setWarehouseId(WarehouseManager.DEFAULT_WAREHOUSE_ID);
            ImmutableMap.Builder<Long, ComputeNode> b = ImmutableMap.builder();
            current.forEach(b::put);
            b.put(n.getId(), n);
            current = b.build();
        }

        public void removeOtherNode(long id) {
            if (!current.containsKey(id)) {
                return;
            }
            if (current.get(id).getWarehouseId() == WarehouseManager.DATACACHE_WAREHOUSE_ID) {
                return;
            }
            ImmutableMap.Builder<Long, ComputeNode> b = ImmutableMap.builder();
            current.entrySet().stream().filter(e -> e.getKey() != id).forEach(e -> b.put(e.getKey(), e.getValue()));
            current = b.build();
        }

        @Override
        public ImmutableMap<Long, ComputeNode> getIdComputeNode() {
            return current;
        }

        @Override
        public ImmutableMap<Long, ComputeNode> getIdComputeNodeRefBefore() {
            return previous;
        }

        @Override
        public AtomicLong getCacheNodeChangeTime() {
            return cacheTime;
        }

        @Override
        public ImmutableMap<Long, com.starrocks.system.Backend> getIdToBackend() {
            return ImmutableMap.of();
        }
    }

    @Test
    public void testPreviousSnapshotWithinWindow() {
        Config.previous_backend_cache_keep_time = 60000;
        FakeSystemInfoService sis = new FakeSystemInfoService();
        sis.init(1L, 1, 0);
        sis.addDatacacheNode(100L);
        DefaultWorkerProvider provider = new DefaultWorkerProvider.Factory().captureAvailableWorkers(
                sis, true, -1, com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, provider.getAllPreviousCacheNodes().size());
    }

    @Test
    public void testPreviousSnapshotOutsideWindowFallsBackToCurrent() {
        Config.previous_backend_cache_keep_time = 10;
        FakeSystemInfoService sis = new FakeSystemInfoService();
        sis.init(1L, 1, 0);
        sis.addDatacacheNode(100L);
        sis.getCacheNodeChangeTime().set(System.currentTimeMillis() - 1000L);
        DefaultWorkerProvider provider = new DefaultWorkerProvider.Factory().captureAvailableWorkers(
                sis, true, -1, com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(2, provider.getAllPreviousCacheNodes().size());
    }

    @Test
    public void testNonDatacacheNodeChangeDoesNotAffectPreviousSnapshot() {
        Config.previous_backend_cache_keep_time = 60000;
        FakeSystemInfoService sis = new FakeSystemInfoService();
        sis.init(1L, 1, 1);
        DefaultWorkerProvider provider1 = new DefaultWorkerProvider.Factory().captureAvailableWorkers(
                sis, true, -1, com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, provider1.getAllPreviousCacheNodes().size());

        sis.addOtherNode(500L);
        DefaultWorkerProvider provider2 = new DefaultWorkerProvider.Factory().captureAvailableWorkers(
                sis, true, -1, com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, provider2.getAllPreviousCacheNodes().size());

        sis.removeOtherNode(2L);
        DefaultWorkerProvider provider3 = new DefaultWorkerProvider.Factory().captureAvailableWorkers(
                sis, true, -1, com.starrocks.qe.SessionVariableConstants.ComputationFragmentSchedulingPolicy.ALL_NODES,
                WarehouseManager.DEFAULT_WAREHOUSE_ID);
        Assert.assertEquals(1, provider3.getAllPreviousCacheNodes().size());
        Assert.assertTrue(provider3.getAllPreviousCacheNodes().stream()
                .allMatch(n -> n.getWarehouseId() == WarehouseManager.DATACACHE_WAREHOUSE_ID));
    }
}
