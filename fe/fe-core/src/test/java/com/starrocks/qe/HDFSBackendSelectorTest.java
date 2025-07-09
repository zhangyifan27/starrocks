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

package com.starrocks.qe;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.Pair;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.qe.scheduler.DefaultWorkerProvider;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HDFSBackendSelectorTest {
    @Mocked
    private HdfsScanNode hdfsScanNode;
    @Mocked
    private HiveTable hiveTable;
    @Mocked
    private ConnectContext context;
    final int scanNodeId = 0;
    final int computeNodePort = 9030;
    final String hostFormat = "192.168.1.%02d";

    private List<TScanRangeLocations> createScanRanges(long number, long size) {
        List<TScanRangeLocations> ans = new ArrayList<>();

        for (int i = 0; i < number; i++) {
            TScanRangeLocations scanRangeLocations = new TScanRangeLocations();
            ans.add(scanRangeLocations);

            TScanRange scanRange = new TScanRange();
            scanRangeLocations.scan_range = scanRange;
            THdfsScanRange hdfsScanRange = new THdfsScanRange();
            scanRange.hdfs_scan_range = hdfsScanRange;
            hdfsScanRange.setRelative_path(String.format("%06d", i));
            hdfsScanRange.setOffset(0);
            hdfsScanRange.setLength(size);

            List<TScanRangeLocation> locations = new ArrayList<>();
            TScanRangeLocation location = new TScanRangeLocation();
            location.setServer(new TNetworkAddress("localhost", -1));
            locations.add(location);
            scanRangeLocations.setLocations(locations);
        }
        return ans;
    }

    private ImmutableMap<Long, ComputeNode> createComputeNodes(int number) {
        Map<Long, ComputeNode> ans = new HashMap<>();
        for (int i = 0; i < number; i++) {
            ComputeNode node = new ComputeNode(i, String.format(hostFormat, i), computeNodePort);
            node.setBePort(computeNodePort);
            node.setAlive(true);
            ans.put((long) i, node);
        }
        return ImmutableMap.copyOf(ans);
    }

    private Map<Long, Long> computeWorkerIdToReadBytes(FragmentScanRangeAssignment assignment, int scanNodeId) {
        Map<Long, Long> stats = new HashMap<>();
        for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            List<TScanRangeParams> scanRangeParams = entry.getValue().get(scanNodeId);
            for (TScanRangeParams params : scanRangeParams) {
                THdfsScanRange scanRange = params.scan_range.hdfs_scan_range;
                stats.put(entry.getKey(), stats.getOrDefault(entry.getKey(), 0L) + scanRange.getLength());
            }
        }
        return stats;
    }

    private Map<Long, Pair<Long, Long>> computeWorkerIdToReadBytesAndNum(FragmentScanRangeAssignment assignment, int scanNodeId) {
        Map<Long, Pair<Long, Long>> stats = new HashMap<>();
        for (Map.Entry<Long, Map<Integer, List<TScanRangeParams>>> entry : assignment.entrySet()) {
            List<TScanRangeParams> scanRangeParams = entry.getValue().get(scanNodeId);
            for (TScanRangeParams params : scanRangeParams) {
                THdfsScanRange scanRange = params.scan_range.hdfs_scan_range;
                if (stats.containsKey(entry.getKey())) {
                    Pair<Long, Long> pair = stats.get(entry.getKey());
                    pair.first += scanRange.getLength();
                    pair.second += 1;
                } else {
                    stats.put(entry.getKey(), Pair.create(scanRange.getLength(), 1L));
                }
            }
        }
        return stats;
    }

    String toHumanReadableByteCount(double bytes) {
        String[] dictionary = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
        int index;
        for (index = 0; index < dictionary.length; index++) {
            if (bytes < 1024) {
                break;
            }
            bytes = bytes / 1024.0;
        }
        return String.format("%.2f %s", bytes, dictionary[index]);
    }

    @Ignore
    @Test
    public void testHdfsScanNodeHashRingMockOnline() throws Exception {
        String hashRingAlgorithm = "plain";
        int virtualNodeNum = 256;
        long scanRangeNumber = 100000;
        int hostNumber = 100;
        testHdfsScanNodeHashRingMockOnline(hashRingAlgorithm, virtualNodeNum, scanRangeNumber, hostNumber);
    }

    public void testHdfsScanNodeHashRingMockOnline(String hashRingAlgorithm, int virtualNodeNum, long scanRangeNumber,
                                                   int hostNumber) throws Exception {
        // 12M
        long scanRangeSize = 12000000;
        SessionVariable sessionVariable = new SessionVariable();
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider =
                new DefaultWorkerProvider(ImmutableMap.of(), computeNodes, ImmutableMap.of(), computeNodes, true);

        sessionVariable.setHdfsBackendSelectorHashAlgorithm(hashRingAlgorithm);
        sessionVariable.setConsistentHashVirtualNodeNum(virtualNodeNum);
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        long startTime = System.currentTimeMillis();
        selector.computeScanRangeAssignment();
        long endTime = System.currentTimeMillis();

        long avg = (scanRangeNumber * scanRangeSize) / hostNumber;

        Map<Long, Pair<Long, Long>> stats = computeWorkerIdToReadBytesAndNum(assignment, scanNodeId);
        long minSize = Long.MAX_VALUE;
        long maxSize = Long.MIN_VALUE;
        long minNum = Long.MAX_VALUE;
        long maxNum = Long.MIN_VALUE;
        if (stats.size() < hostNumber) {
            minSize = 0;
            minNum = 0;
        }
        double maxVariance = 0;
        for (Map.Entry<Long, Pair<Long, Long>> entry : stats.entrySet()) {
            minSize = Math.min(minSize, entry.getValue().first);
            maxSize = Math.max(maxSize, entry.getValue().first);
            minNum = Math.min(minNum, entry.getValue().second);
            maxNum = Math.max(maxNum, entry.getValue().second);

            maxVariance = Math.max(maxVariance, 1.0 * Math.abs(entry.getValue().first - avg) / avg);
        }

        System.out.println("HashRingAlgorithm: " + hashRingAlgorithm + ", virtualNodeNum: " + virtualNodeNum);
        System.out.println("ScanRangeSize: " + toHumanReadableByteCount(scanRangeSize) + ", ScanRangeNumber: " + scanRangeNumber +
                ", hostNumber: " + hostNumber);
        System.out.println("minNum: " + minNum + ", maxNum: " + maxNum);
        System.out.println("minSize: " + toHumanReadableByteCount(minSize) + ", maxSize: " + toHumanReadableByteCount(maxSize));
        System.out.printf("avgSize: " + toHumanReadableByteCount(avg) + ", maxVariance: %.2f%%\n", maxVariance * 100);
        System.out.println("Schedule time: " + (endTime - startTime) + "ms");

        double variance = 0.2 * avg;
        for (Map.Entry<Long, Pair<Long, Long>> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue().first);
            Assert.assertTrue(entry.getValue().first - avg < variance);
        }

        // test empty compute nodes
        workerProvider =
                new DefaultWorkerProvider(ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), true);
        selector = new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        try {
            selector.computeScanRangeAssignment();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Failed to find backend to execute", e.getMessage());
        }
    }

    @Test
    public void testHdfsScanNodeConsistentHashRing() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        int scanRangeNumber = 10000;
        int scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        selector.computeScanRangeAssignment();

        int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        double variance = 0.2 * avg;
        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(entry.getValue() - avg < variance);
        }

        // test empty compute nodes
        workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                true
        );
        selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        try {
            selector.computeScanRangeAssignment();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Failed to find backend to execute", e.getMessage());
        }
    }

    @Test
    public void testHdfsScanNodeRoundRobin() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setHdfsBackendSelectorHashAlgorithm("roundRobin");
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        {
            // Under the Round Robin algorithm, the distribution must be extremely uniform.
            int scanRangeNumber = 30000;
            int scanRangeSize = 10000;
            int hostNumber = 3;
            List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
            DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                    ImmutableMap.of(),
                    computeNodes,
                    ImmutableMap.of(),
                    computeNodes,
                    true
            );

            HDFSBackendSelector selector =
                    new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                            context);
            selector.computeScanRangeAssignment();

            int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
            // Extremely uniform
            double variance = 0.00001 * avg;
            Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
            for (Map.Entry<Long, Long> entry : stats.entrySet()) {
                System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
                Assert.assertTrue(entry.getValue() - avg < variance);
            }
        }

        {
            // Detail checks
            int scanRangeNumber = 200;
            int scanRangeSize = 10000;
            int hostNumber = 100;
            List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
            DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                    ImmutableMap.of(),
                    computeNodes,
                    ImmutableMap.of(),
                    computeNodes,
                    true
            );

            HDFSBackendSelector selector =
                    new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                            context);
            selector.computeScanRangeAssignment();

            Assert.assertEquals(hostNumber, assignment.size());
            for (int i = 0; i < hostNumber; i++) {
                long nodeIdx = i % hostNumber;
                // Every node has 2 scan range
                Assert.assertEquals(2, assignment.get(nodeIdx).get(0).size());
                String assignmentPath = assignment.get(nodeIdx).get(0).get(0).scan_range.hdfs_scan_range.relative_path;
                Assert.assertEquals(locations.get(i).scan_range.hdfs_scan_range.relative_path, assignmentPath);
            }
        }

        {
            // test empty compute nodes
            int scanRangeNumber = 30000;
            int scanRangeSize = 10000;
            int hostNumber = 3;
            List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
            FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
            ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
            DefaultWorkerProvider workerProvider = workerProvider = new DefaultWorkerProvider(
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    true
            );
            HDFSBackendSelector selector =
                    new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                            context);
            try {
                selector.computeScanRangeAssignment();
                Assert.fail();
            } catch (Exception e) {
                Assert.assertEquals("Failed to find backend to execute", e.getMessage());
            }
        }
    }

    @Test
    public void testHdfsScanNodePlainHashRing() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setHdfsBackendSelectorHashAlgorithm("plain");
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        int scanRangeNumber = 10000;
        int scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                        context);
        selector.computeScanRangeAssignment();

        int avg = (scanRangeNumber * scanRangeSize) / hostNumber;
        double variance = 0.05 * avg;
        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue(entry.getValue() - avg < variance);
        }

        // test empty compute nodes
        workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                true
        );
        selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider,
                        context);
        try {
            selector.computeScanRangeAssignment();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Failed to find backend to execute", e.getMessage());
        }
    }

    @Test
    public void testHdfsScanNodeScanRangeReBalance() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;

                hdfsScanNode.getTableName();
                result = "hive_tbl";

                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        long scanRangeNumber = 10000;
        long scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        selector.computeScanRangeAssignment();

        long avg = (scanRangeNumber * scanRangeSize) / hostNumber + 1;
        double variance = 0.2 * avg;
        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
            Assert.assertTrue((entry.getValue() - avg) < variance);
        }

        variance = 0.4 / 100 * scanRangeNumber * scanRangeSize;
        double actual = 0;
        for (Map.Entry<ComputeNode, Long> entry : selector.reBalanceBytesPerComputeNode.entrySet()) {
            System.out.printf("%s -> %d bytes re-balance\n", entry.getKey(), entry.getValue());
            actual = actual + entry.getValue();
        }
        Assert.assertTrue(actual < variance);
    }

    @Test
    public void testHashRingAlgorithm() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceScheduleLocal(false);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        new Expectations() {
            {
                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        int scanRangeNumber = 100;
        int scanRangeSize = 10000;
        int hostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );
        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        HashRing hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals(SessionVariable.BackendSelectorHashAlgorithm.CONSISTENT));
        ConsistentHashRing consistentHashRing = (ConsistentHashRing) hashRing;
        Assert.assertTrue(consistentHashRing.getVirtualNumber() ==
                HDFSBackendSelector.CONSISTENT_HASH_RING_VIRTUAL_NUMBER);

        sessionVariable.setHdfsBackendSelectorHashAlgorithm(SessionVariable.BackendSelectorHashAlgorithm.RENDEZVOUS);
        hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals(SessionVariable.BackendSelectorHashAlgorithm.RENDEZVOUS));

        sessionVariable.setHdfsBackendSelectorHashAlgorithm(SessionVariable.BackendSelectorHashAlgorithm.CONSISTENT);
        sessionVariable.setConsistentHashVirtualNodeNum(64);
        hashRing = selector.makeHashRing();
        Assert.assertTrue(hashRing.policy().equals(SessionVariable.BackendSelectorHashAlgorithm.CONSISTENT));
        consistentHashRing = (ConsistentHashRing) hashRing;
        Assert.assertTrue(consistentHashRing.getVirtualNumber() == 64);
    }

    @Test
    public void testHdfsScanNodeForceScheduleLocal() throws Exception {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setForceScheduleLocal(true);
        sessionVariable.setHdfsBackendSelectorScanRangeShuffle(false);
        sessionVariable.setEnableScanDataCache(false);
        new Expectations() {
            {
                hdfsScanNode.getId();
                result = scanNodeId;
                hiveTable.getTableLocation();
                result = "hdfs://dfs00/dataset/";

                context.getSessionVariable();
                result = sessionVariable;
            }
        };

        int scanRangeNumber = 100;
        int scanRangeSize = 10000;
        int hostNumber = 100;

        // rewrite scan ranges locations to only 3 hosts.
        // so with `forceScheduleLocal` only 3 nodes will get scan ranges.
        int localHostNumber = 3;
        List<TScanRangeLocations> locations = createScanRanges(scanRangeNumber, scanRangeSize);
        for (TScanRangeLocations location : locations) {
            List<TScanRangeLocation> servers = location.locations;
            servers.clear();
            for (int i = 0; i < localHostNumber; i++) {
                TScanRangeLocation loc = new TScanRangeLocation();
                loc.setServer(new TNetworkAddress(String.format(hostFormat, i), computeNodePort));
                servers.add(loc);
            }
        }

        FragmentScanRangeAssignment assignment = new FragmentScanRangeAssignment();
        ImmutableMap<Long, ComputeNode> computeNodes = createComputeNodes(hostNumber);
        DefaultWorkerProvider workerProvider = new DefaultWorkerProvider(
                ImmutableMap.of(),
                computeNodes,
                ImmutableMap.of(),
                computeNodes,
                true
        );

        HDFSBackendSelector selector =
                new HDFSBackendSelector(hdfsScanNode, locations, assignment, workerProvider, context);
        selector.computeScanRangeAssignment();

        Map<Long, Long> stats = computeWorkerIdToReadBytes(assignment, scanNodeId);
        Assert.assertEquals(stats.size(), localHostNumber);
        for (Map.Entry<Long, Long> entry : stats.entrySet()) {
            System.out.printf("%s -> %d bytes\n", entry.getKey(), entry.getValue());
        }
    }
}
