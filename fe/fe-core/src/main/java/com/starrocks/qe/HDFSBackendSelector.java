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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.hash.Funnel;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.common.Config;
import com.starrocks.common.UserException;
import com.starrocks.common.profile.Tracers;
import com.starrocks.common.util.ConsistentHashRing;
import com.starrocks.common.util.HashRing;
import com.starrocks.common.util.PlainHashRing;
import com.starrocks.common.util.RendezvousHashRing;
import com.starrocks.common.util.RoundRobin;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.FileTableScanNode;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.HudiScanNode;
import com.starrocks.planner.IcebergMetadataScanNode;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.OdpsScanNode;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.scheduler.NonRecoverableException;
import com.starrocks.qe.scheduler.WorkerProvider;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocation;
import com.starrocks.thrift.TScanRangeLocations;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
/**
 * Hybrid backend selector for hive table.
 * Support hybrid and independent deployment with datanode.
 * <p>
 * Assign scan ranges to backend:
 * 1. local backend first,
 * 2. and smallest assigned scan ranges num or scan bytes.
 * <p>
 * If force_schedule_local variable is set, HybridBackendSelector will force to
 * assign scan ranges to local backend if there has one.
 */

public class HDFSBackendSelector implements BackendSelector {
    public static final Logger LOG = LogManager.getLogger(HDFSBackendSelector.class);
    // be -> assigned scans
    Map<ComputeNode, Long> assignedScansPerComputeNode = Maps.newHashMap();
    Map<ComputeNode, Long> assignedScanNumPerComputeNode = Maps.newTreeMap();
    // be -> re-balance bytes
    Map<ComputeNode, Long> reBalanceBytesPerComputeNode = Maps.newHashMap();
    // be host -> bes
    Multimap<String, ComputeNode> hostToBackends = HashMultimap.create();
    Map<Long, ComputeNode> idToBackend = Maps.newHashMap();
    private final ScanNode scanNode;
    private final List<TScanRangeLocations> locations;
    private final FragmentScanRangeAssignment assignment;
    private final WorkerProvider workerProvider;
    private final ConnectContext connectContext;
    private final int kCandidateNumber = 3;
    // After testing, this value can ensure that the scan range size assigned to each BE is as uniform as possible,
    // and the largest scan data is not more than 1.1 times of the average value
    private final double kMaxImbalanceRatio = 1.1;
    public static final int CONSISTENT_HASH_RING_VIRTUAL_NUMBER = 256;

    class HdfsScanRangeHasher {
        String basePath;
        HDFSScanNodePredicates predicates;

        public HdfsScanRangeHasher() {
            if (scanNode instanceof HdfsScanNode) {
                HdfsScanNode node = (HdfsScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getHiveTable().getTableLocation();
            } else if (scanNode instanceof IcebergScanNode) {
                IcebergScanNode node = (IcebergScanNode) scanNode;
                predicates = node.getScanNodePredicates();
            } else if (scanNode instanceof HudiScanNode) {
                HudiScanNode node = (HudiScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getHudiTable().getTableLocation();
            } else if (scanNode instanceof DeltaLakeScanNode) {
                DeltaLakeScanNode node = (DeltaLakeScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getDeltaLakeTable().getTableLocation();
            } else if (scanNode instanceof FileTableScanNode) {
                FileTableScanNode node = (FileTableScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getFileTable().getTableLocation();
            } else if (scanNode instanceof PaimonScanNode) {
                PaimonScanNode node = (PaimonScanNode) scanNode;
                predicates = node.getScanNodePredicates();
                basePath = node.getPaimonTable().getTableLocation();
            } else if (scanNode instanceof OdpsScanNode) {
                OdpsScanNode node = (OdpsScanNode) scanNode;
                predicates = node.getScanNodePredicates();
            } else if (scanNode instanceof IcebergMetadataScanNode) {
                // ignored
            } else {
                Preconditions.checkState(false);
            }
        }

        public void acceptScanRangeLocations(TScanRangeLocations tScanRangeLocations, PrimitiveSink primitiveSink) {
            THdfsScanRange hdfsScanRange = tScanRangeLocations.scan_range.hdfs_scan_range;
            if (hdfsScanRange.isSetFull_path()) {
                primitiveSink.putString(hdfsScanRange.full_path, StandardCharsets.UTF_8);
            } else {
                if (hdfsScanRange.isSetPartition_id() &&
                        predicates.getIdToPartitionKey().containsKey(hdfsScanRange.getPartition_id())) {
                    PartitionKey partitionKey = predicates.getIdToPartitionKey().get(hdfsScanRange.getPartition_id());
                    primitiveSink.putInt(partitionKey.toString().hashCode());
                }
                if (hdfsScanRange.isSetRelative_path()) {
                    primitiveSink.putString(hdfsScanRange.relative_path, StandardCharsets.UTF_8);
                }
            }
            // Always hash offset in the default hasher (normal ring). The "file-path-only" feature
            // is now restricted to hashRingBefore (previous backend view) via a dedicated funnel.
            if (hdfsScanRange.isSetOffset()) {
                primitiveSink.putLong(hdfsScanRange.getOffset());
            }
        }
    }

    private final HdfsScanRangeHasher hdfsScanRangeHasher;
    // dedicated funnel for normal hashing (with offset)
    private final TScanRangeLocationsFunnel normalFunnel = new TScanRangeLocationsFunnel();
    // funnel for file-path-only hashing (skip offset) used ONLY for hashRingBefore when enabled
    private final Funnel<TScanRangeLocations> filePathOnlyFunnel = new Funnel<TScanRangeLocations>() {
        @Override
        public void funnel(@Nonnull TScanRangeLocations tScanRangeLocations, @Nonnull PrimitiveSink primitiveSink) {
            THdfsScanRange hdfsScanRange = tScanRangeLocations.scan_range.hdfs_scan_range;
            if (hdfsScanRange == null) {
                return;
            }
            if (hdfsScanRange.isSetFull_path()) {
                primitiveSink.putString(hdfsScanRange.full_path, StandardCharsets.UTF_8);
                return;
            }
            // Mirror the path/partition portion of HdfsScanRangeHasher.acceptScanRangeLocations (without offset)
            if (hdfsScanRange.isSetPartition_id() &&
                    hdfsScanRangeHasher.predicates != null &&
                    hdfsScanRangeHasher.predicates.getIdToPartitionKey().containsKey(hdfsScanRange.getPartition_id())) {
                PartitionKey partitionKey = hdfsScanRangeHasher.predicates.getIdToPartitionKey().get(
                        hdfsScanRange.getPartition_id());
                primitiveSink.putInt(partitionKey.hashCode());
            }
            if (hdfsScanRange.isSetRelative_path() && hdfsScanRange.relative_path != null) {
                primitiveSink.putString(hdfsScanRange.relative_path, StandardCharsets.UTF_8);
            }
            // DO NOT hash offset here.
        }
    };

    public HDFSBackendSelector(ScanNode scanNode, List<TScanRangeLocations> locations,
                               FragmentScanRangeAssignment assignment, WorkerProvider workerProvider,
                               ConnectContext connectContext) {
        this.scanNode = scanNode;
        this.locations = locations;
        this.assignment = assignment;
        this.workerProvider = workerProvider;
        this.connectContext = connectContext;
        this.hdfsScanRangeHasher = new HdfsScanRangeHasher();
    }

    private boolean needRebalance() {
        if (connectContext == null) {
            return true;
        }
        SessionVariable vars = connectContext.getSessionVariable();
        if (vars.isEnableCacheSelect()) {
            return false;
        }
        if (vars.getHdfsBackendSelectorForceRebalance()) {
            return true;
        }
        return !vars.isEnableScanDataCache();
    }

    // re-balance scan ranges for compute node if needed, return the compute node which scan range is assigned to
    private ComputeNode reBalanceScanRangeForComputeNode(List<ComputeNode> backends, long avgNodeScanRangeBytes,
                                                         TScanRangeLocations scanRangeLocations, boolean needRebalance) {
        if (backends == null || backends.isEmpty()) {
            return null;
        }

        // If force-rebalancing is not specified and cache is used, skip the rebalancing directly.
        if (!needRebalance) {
            return backends.get(0);
        }

        ComputeNode node = null;
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        for (ComputeNode backend : backends) {
            long assignedScanRanges = assignedScansPerComputeNode.get(backend);
            if (assignedScanRanges + addedScans < avgNodeScanRangeBytes * kMaxImbalanceRatio) {
                node = backend;
                break;
            }
        }
        if (node == null) {
            node = backends.get(0);
        }
        return node;
    }

    class ComputeNodeFunnel implements Funnel<ComputeNode> {
        @Override
        public void funnel(@Nonnull ComputeNode computeNode, @Nonnull PrimitiveSink primitiveSink) {
            primitiveSink.putString(computeNode.getHost(), StandardCharsets.UTF_8);
            primitiveSink.putInt(computeNode.getBePort());
        }
    }

    class TScanRangeLocationsFunnel implements Funnel<TScanRangeLocations> {
        @Override
        public void funnel(@Nonnull TScanRangeLocations tScanRangeLocations, @Nonnull PrimitiveSink primitiveSink) {
            hdfsScanRangeHasher.acceptScanRangeLocations(tScanRangeLocations, primitiveSink);
        }
    }

    @VisibleForTesting
    public HashRing<TScanRangeLocations, ComputeNode> makeHashRing(Set<ComputeNode> nodes, boolean filePathOnly) {
        HashRing<TScanRangeLocations, ComputeNode> hashRing = null;
        String hashAlgorithm = getSelectAlgorithm();
        int virtualNodeNum = connectContext != null ? connectContext.getSessionVariable().
                getConsistentHashVirtualNodeNum() : CONSISTENT_HASH_RING_VIRTUAL_NUMBER;
        Funnel<TScanRangeLocations> funnelToUse = filePathOnly ? filePathOnlyFunnel : normalFunnel;
        if (hashAlgorithm.equalsIgnoreCase(SessionVariable.BackendSelectorHashAlgorithm.RENDEZVOUS)) {
            hashRing = new RendezvousHashRing<>(Hashing.murmur3_128(), funnelToUse,
                    new ComputeNodeFunnel(), nodes);
        } else if (hashAlgorithm.equalsIgnoreCase(SessionVariable.BackendSelectorHashAlgorithm.ROUNDROBIN)) {
            hashRing = new RoundRobin<>(nodes, scanNode.getStartRandomScanRangeOffset(), scanNode.getDeployedScanRangeOffset());
        } else if (hashAlgorithm.equalsIgnoreCase(SessionVariable.BackendSelectorHashAlgorithm.PLAIN)) {
            hashRing = new PlainHashRing<>(Hashing.murmur3_128(), funnelToUse, nodes);
        } else {
            hashRing = new ConsistentHashRing<>(Hashing.murmur3_128(), funnelToUse,
                    new ComputeNodeFunnel(), nodes, virtualNodeNum);
        }
        return hashRing;
    }

    private String getSelectAlgorithm() {
        if (connectContext == null) {
            // default use consistent hash
            return SessionVariable.BackendSelectorHashAlgorithm.CONSISTENT;
        }

        if (connectContext.getSessionVariable().getEnableAdaptiveBackendSelectorHashAlgorithm() &&
                !connectContext.getSessionVariable().isEnableScanDataCache()) {
            // in adaptive mode, if disable data cache, use round robin
            return SessionVariable.BackendSelectorHashAlgorithm.ROUNDROBIN;
        }

        // if config select algorithm, use it
        return connectContext.getSessionVariable().
                getHdfsBackendSelectorHashAlgorithm();
    }

    private long computeTotalSize() {
        long size = 0;
        for (TScanRangeLocations scanRangeLocations : locations) {
            size += scanRangeLocations.scan_range.hdfs_scan_range.getLength();
        }
        return size;
    }

    @Override
    public void computeScanRangeAssignment() throws UserException {
        if (locations.size() == 0) {
            return;
        }

        long totalSize = computeTotalSize();
        long avgNodeScanRangeBytes = totalSize / Math.max(workerProvider.getAllWorkers().size(), 1) + 1;

        for (ComputeNode computeNode : workerProvider.getAllWorkers()) {
            assignedScansPerComputeNode.put(computeNode, 0L);
            assignedScanNumPerComputeNode.put(computeNode, 0L);
            reBalanceBytesPerComputeNode.put(computeNode, 0L);
            hostToBackends.put(computeNode.getHost(), computeNode);
            idToBackend.put(computeNode.getId(), computeNode);
        }

        // schedule scan ranges to co-located backends.
        // and put rest scan ranges into remote scan ranges.
        List<TScanRangeLocations> remoteScanRangeLocations = Lists.newArrayList();
        boolean needRebalance = needRebalance();
        if (connectContext.getSessionVariable().getForceScheduleLocal()) {
            for (int i = 0; i < locations.size(); ++i) {
                TScanRangeLocations scanRangeLocations = locations.get(i);
                List<ComputeNode> backends = new ArrayList<>();
                // select all backends that are co-located with this scan range.
                boolean ignoreScanRange = false;
                for (final TScanRangeLocation location : scanRangeLocations.getLocations()) {
                    if (location.getBackend_id() != 0) {
                        ComputeNode node = idToBackend.get(location.getBackend_id());
                        if (node != null) {
                            backends.add(node);
                        } else {
                            // cache delete: data cache file meta中记录的be id目前不活跃，跳过，防止走入hashring逻辑,覆盖file meta中的be id
                            ignoreScanRange = true;
                        }
                        break;
                    }
                    Collection<ComputeNode> servers = hostToBackends.get(location.getServer().getHostname());
                    if (servers == null || servers.isEmpty()) {
                        continue;
                    }
                    backends.addAll(servers);
                }

                if (ignoreScanRange) {
                    continue;
                }

                ComputeNode node =
                        reBalanceScanRangeForComputeNode(backends, avgNodeScanRangeBytes, scanRangeLocations, needRebalance);
                if (node == null) {
                    remoteScanRangeLocations.add(scanRangeLocations);
                } else {
                    recordScanRangeAssignment(node, backends, scanRangeLocations);
                }
            }
        } else {
            remoteScanRangeLocations = locations;
        }
        if (remoteScanRangeLocations.isEmpty()) {
            return;
        }

        // force enable needRebalance when enable_remote_node_cache && using non-default warehouse as executors
        if (Config.enable_remote_node_cache && 
                assignedScansPerComputeNode.keySet().stream().anyMatch(
                    node -> node.getWarehouseId() != WarehouseManager.DATACACHE_WAREHOUSE_ID)) {
            needRebalance = true;
        }

        // use consistent hashing to schedule remote scan ranges
        // Decide whether file-path-only hashing should apply to the main ring as well:
        //  - Session variable hdfsScanRangeHashFilePathOnly must be true
        //  - Cache select (isEnableCacheSelect) must be enabled (we favor stable mapping for cache locality)
        boolean filePathOnlyFlag = connectContext != null && connectContext.getSessionVariable() != null
                && connectContext.getSessionVariable().getHdfsScanRangeHashFilePathOnly();
        boolean mainRingFilePathOnly = filePathOnlyFlag && connectContext.getSessionVariable().isEnableCacheSelect();
        // Main ring (current backends). May use file-path-only hashing if conditions satisfied above.
        HashRing<TScanRangeLocations, ComputeNode> hashRing = makeHashRing(
                assignedScansPerComputeNode.keySet(), mainRingFilePathOnly);
        // cache previous backends list to avoid multiple defensive copies
        // previous cache nodes (datacache warehouse compute nodes snapshot)
        Collection<ComputeNode> previousCacheNodesSnapshot = workerProvider.getAllPreviousCacheNodes();
        // Previous ring still uses the raw flag (it was the original scope of this feature)
        HashRing<TScanRangeLocations, ComputeNode> hashRingBefore = makeHashRing(
                new HashSet<>(previousCacheNodesSnapshot), filePathOnlyFlag);
        // previous Backends should be fixed, no need to rebalance
        // long avgNodeScanRangeBytesBefore = totalSize / Math.max(previousCacheNodesSnapshot.size(), 1) + 1;
        if (connectContext.getSessionVariable().getHDFSBackendSelectorScanRangeShuffle()) {
            Collections.shuffle(remoteScanRangeLocations);
        }
        // assign scan ranges.

        for (int i = 0; i < remoteScanRangeLocations.size(); ++i) {
            TScanRangeLocations scanRangeLocations = remoteScanRangeLocations.get(i);
            List<ComputeNode> backends = hashRing.get(scanRangeLocations, needRebalance ? kCandidateNumber : 1);
            ComputeNode node =
                    reBalanceScanRangeForComputeNode(backends, avgNodeScanRangeBytes, scanRangeLocations, needRebalance);
            if (node == null) {
                throw new RuntimeException("Failed to find backend to execute");
            }
            if (Config.enable_remote_node_cache && previousCacheNodesSnapshot != null &&
                    !previousCacheNodesSnapshot.isEmpty()) {
                List<ComputeNode> backendsBefore = hashRingBefore.get(scanRangeLocations, 1);
                if (!backendsBefore.isEmpty()) {
                    ComputeNode nodeBefore = backendsBefore.get(0);
                    if (!nodeBefore.getBrpcAddress().equals(node.getBrpcAddress())) {
                        scanRangeLocations.scan_range.hdfs_scan_range.setPrevious_cache_node(nodeBefore.getBrpcAddress());
                        scanRangeLocations.scan_range.hdfs_scan_range.setPrevious_cache_nodeIsSet(true);
                    }
                }
            }
            recordScanRangeAssignment(node, backends, scanRangeLocations);
        }
        scanNode.updateScanRangeOffset(remoteScanRangeLocations.size());
        recordScanRangeStatistic(hashRing.policy());
    }

    private void recordScanRangeAssignment(ComputeNode worker, List<ComputeNode> backends,
                                           TScanRangeLocations scanRangeLocations)
            throws NonRecoverableException {
        workerProvider.selectWorker(worker.getId());

        // update statistic
        long addedScans = scanRangeLocations.scan_range.hdfs_scan_range.length;
        assignedScansPerComputeNode.put(worker, assignedScansPerComputeNode.get(worker) + addedScans);
        assignedScanNumPerComputeNode.put(worker, assignedScanNumPerComputeNode.get(worker) + 1L);
        // the fist item in backends will be assigned if there is no re-balance, we compute re-balance bytes
        // if the worker is not the first item in backends.
        if (worker != backends.get(0)) {
            reBalanceBytesPerComputeNode.put(worker, reBalanceBytesPerComputeNode.get(worker) + addedScans);
        }

        // add scan range params
        TScanRangeParams scanRangeParams = new TScanRangeParams();
        scanRangeParams.scan_range = scanRangeLocations.scan_range;
        assignment.put(worker.getId(), scanNode.getId().asInt(), scanRangeParams);
    }

    private void recordScanRangeStatistic(String selectPolicy) {
        // record scan range size for each backend
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<ComputeNode, Long> entry : assignedScansPerComputeNode.entrySet()) {
            String host = entry.getKey().getAddress().hostname.replace('.', '_');
            long value = entry.getValue();
            String key =
                    String.format("Placement.%s.assign[%s].%s(scanRangeNum: %s)", scanNode.getTableName(), selectPolicy, host,
                            assignedScanNumPerComputeNode.get(entry.getKey()));
            Tracers.count(Tracers.Module.EXTERNAL, key, value);
            sb.append(entry.getKey().getAddress().hostname).append(":").append(value).append(",");
        }
        Tracers.record(Tracers.Module.EXTERNAL, scanNode.getTableName() + " scan_range_bytes", sb.toString());
        // record re-balance bytes for each backend
        sb = new StringBuilder();
        for (Map.Entry<ComputeNode, Long> entry : reBalanceBytesPerComputeNode.entrySet()) {
            sb.append(entry.getKey().getAddress().hostname).append(":").append(entry.getValue()).append(",");
        }
        Tracers.record(Tracers.Module.EXTERNAL, scanNode.getTableName() + " rebalance_bytes", sb.toString());
    }
}