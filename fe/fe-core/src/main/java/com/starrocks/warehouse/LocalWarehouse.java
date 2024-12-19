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

package com.starrocks.warehouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// on-premise
public class LocalWarehouse extends DefaultWarehouse {
    private static final Logger LOG = LogManager.getLogger(LocalWarehouse.class);

    public enum WarehouseState {
        AVAILABLE,
        SUSPENDED,
    }

    @SerializedName(value = "cluster")
    protected final com.starrocks.warehouse.Cluster cluster;

    @SerializedName(value = "state")
    protected WarehouseState state = WarehouseState.AVAILABLE;

    @SerializedName(value = "ctime")
    private volatile long createdTime;

    @SerializedName(value = "rtime")
    private volatile long resumedTime;

    @SerializedName(value = "mtime")
    private volatile long updatedTime;

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public static final ImmutableList<String> CLUSTER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("ClusterId")
            .add("WorkerGroupId")
            .add("ComputeNodeIds")
            .add("Pending")
            .add("Running")
            .build();

    public LocalWarehouse() {
        super(WarehouseManager.DEFAULT_WAREHOUSE_ID, WarehouseManager.DEFAULT_WAREHOUSE_NAME);
        cluster = new Cluster(WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public LocalWarehouse(long id, String name, long clusterId, String comment) {
        super(id, name);
        this.comment = comment;
        if (RunMode.isSharedNothingMode()) {
            cluster = new Cluster(id);
        } else {
            cluster = new Cluster(clusterId);
        }
    }

    public void init() throws DdlException {
        try {
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            long workerGroupId = starOSAgent.createWorkerGroup("x0");
            cluster.setWorkerGroupId(workerGroupId);
        } catch (DdlException e) {
            LOG.warn("create Cluster failed, because : " + e);
            throw e;
        }
    }

    public WarehouseState getState() {
        return state;
    }

    public Map<Long, Cluster> getClusters() {
        return ImmutableMap.of(cluster.getId(), cluster);
    }

    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    public List<String> getWarehouseInfo() {
        return Lists.newArrayList(
                String.valueOf(getId()),
                getName(),
                state.toString(),
                String.valueOf(cluster.getComputeNodeIds().size()),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(0L),   //TODO: need to be filled after
                String.valueOf(0L),   //TODO: need to be filled after
                TimeUtils.longToTimeString(createdTime),
                TimeUtils.longToTimeString(resumedTime),
                TimeUtils.longToTimeString(updatedTime),
                comment);
    }

    public void dropSelf() throws DdlException {
        deleteWorkerFromStarMgr();
        dropNodeFromSystem();
    }

    public void suspendSelf() {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            this.state = WarehouseState.SUSPENDED;
            this.updatedTime = System.currentTimeMillis();
        }
    }

    public void resumeSelf() {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            this.state = WarehouseState.AVAILABLE;
            this.resumedTime = System.currentTimeMillis();
        }
    }

    private void deleteWorkerFromStarMgr() throws DdlException {
        if (RunMode.isSharedNothingMode()) {
            return;
        }
        long workerGroupId = cluster.getWorkerGroupId();
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        starOSAgent.deleteWorkerGroup(workerGroupId);
    }

    @Override
    public List<List<String>> getWarehouseNodesInfo() {
        List<List<String>> rows = Lists.newArrayList();
        for (Cluster cluster : getClusters().values()) {
            List<Long> computeNodes = cluster.getComputeNodeIds();
            for (Long computeNodeId : computeNodes) {
                ComputeNode node = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                        .getComputeNode(computeNodeId);

                List<String> computeNodeInfo = Lists.newArrayList();
                long warehouseId = node.getWarehouseId();
                Warehouse warehouse = GlobalStateMgr.getCurrentState().getWarehouseMgr().getWarehouse(warehouseId);
                computeNodeInfo.add(warehouse.getName());

                computeNodeInfo.add(String.valueOf(cluster.getId()));
                computeNodeInfo.add(String.valueOf(cluster.getWorkerGroupId()));
                long nodeId = node.getId();
                computeNodeInfo.add(String.valueOf(nodeId));
                if (RunMode.isSharedDataMode()) {
                    long workerId = GlobalStateMgr.getCurrentState().getStarOSAgent().getWorkerIdByNodeId(nodeId);
                    computeNodeInfo.add(String.valueOf(workerId));
                } else {
                    computeNodeInfo.add("0");
                }

                computeNodeInfo.add(node.getHost());

                computeNodeInfo.add(String.valueOf(node.getHeartbeatPort()));
                computeNodeInfo.add(String.valueOf(node.getBePort()));
                computeNodeInfo.add(String.valueOf(node.getHttpPort()));
                computeNodeInfo.add(String.valueOf(node.getBrpcPort()));
                computeNodeInfo.add(String.valueOf(node.getStarletPort()));

                computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastStartTime()));
                computeNodeInfo.add(TimeUtils.longToTimeString(node.getLastUpdateMs()));
                computeNodeInfo.add(String.valueOf(node.isAlive()));

                computeNodeInfo.add(node.getHeartbeatErrMsg());
                computeNodeInfo.add(String.valueOf(node.getVersion()));

                computeNodeInfo.add(String.valueOf(node.getNumRunningQueries()));
                computeNodeInfo.add(String.valueOf(node.getCpuCores()));
                double memUsedPct = node.getMemUsedPct();
                computeNodeInfo.add(String.format("%.2f", memUsedPct * 100) + " %");
                computeNodeInfo.add(String.format("%.1f", node.getCpuUsedPermille() / 10.0) + " %");

                rows.add(computeNodeInfo);
            }
        }
        return rows;
    }

    private void dropNodeFromSystem() throws DdlException {
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<ComputeNode> nodes = systemInfoService.backendAndComputeNodeStream().
                filter(cn -> cn.getWarehouseId() == getId()).collect(Collectors.toList());

        for (ComputeNode node : nodes) {
            if (node instanceof Backend) {
                if (systemInfoService.getBackendWithHeartbeatPort(node.getHost(), node.getHeartbeatPort()) == null) {
                    continue;
                }

                systemInfoService.dropBackend((Backend) node);
            } else {
                if (systemInfoService.getComputeNodeWithHeartbeatPort(node.getHost(), node.getHeartbeatPort()) == null) {
                    continue;
                }

                systemInfoService.dropComputeNode(node);
            }
        }
    }
}
