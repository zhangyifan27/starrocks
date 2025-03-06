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
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.system.ComputeNode;

import java.util.List;
import java.util.Map;

public class DefaultWarehouse extends Warehouse {

    private static final List<Long> WORKER_GROUP_ID_LIST;

    @SerializedName(value = "cluster")
    protected final com.starrocks.warehouse.Cluster cluster;

    public DefaultWarehouse(long id, String name) {
        super(id, name, "An internal warehouse init after FE is ready");
        this.cluster = new Cluster(WarehouseManager.DEFAULT_WAREHOUSE_ID);
    }

    public DefaultWarehouse(long id, String name, Cluster cluster) {
        super(id, name, "An internal warehouse init after FE is ready");
        this.cluster = cluster;
    }

    static {
        WORKER_GROUP_ID_LIST = ImmutableList.of(StarOSAgent.DEFAULT_WORKER_GROUP_ID);
    }

    @Override
    public List<Long> getWorkerGroupIds() {
        return WORKER_GROUP_ID_LIST;
    }

    @Override
    public Long getAnyWorkerGroupId() {
        return StarOSAgent.DEFAULT_WORKER_GROUP_ID;
    }

    public Map<Long, Cluster> getClusters() {
        return ImmutableMap.of(cluster.getId(), cluster);
    }

    public Cluster getAnyAvailableCluster() {
        return cluster;
    }

    @Override
    public List<String> getWarehouseInfo() {
        return Lists.newArrayList(
                String.valueOf(getId()),
                getName(),
                "AVAILABLE",
                String.valueOf(cluster.getComputeNodeIds().size()),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(1L),
                String.valueOf(0L),   //TODO: need to be filled after
                String.valueOf(0L),   //TODO: need to be filled after
                "",
                "",
                "",
                comment);
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

    @Override
    public ProcResult fetchResult() {
        return new BaseProcResult();
    }
}
