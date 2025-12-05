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

package com.starrocks.sql.optimizer.statistics.hbo;

import com.starrocks.catalog.Column;
import com.starrocks.common.AnalysisException;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PlanStatistics {
    protected final int nodeId;
    protected final long pushRows;
    protected final long pullRows;
    protected final long predFilterRows;
    protected final long indexFilterRows;
    protected final long rfFilterRows;

    public static final PlanStatistics EMPTY = new PlanStatistics(
            -1, -1, -1, -1, -1, -1);

    public PlanStatistics(int nodeId, long pushRows, long pullRows,
                        long predFilterRows, long indexFilterRows,
                        long rfFilterRows) {
        this.nodeId = nodeId;
        this.pushRows = pushRows;
        this.pullRows = pullRows;
        this.predFilterRows = predFilterRows;
        this.indexFilterRows = indexFilterRows;
        this.rfFilterRows = rfFilterRows;
    }

    public static PlanStatistics buildFromStatsItem(NodeExecStatsItemPB item,
                                                    PhysicalOperator planNode, Map<String, ScalarOperator> scanToFilterMap) {
        if (planNode instanceof PhysicalScanOperator) {
            boolean isPartitionedTable = ((PhysicalScanOperator) planNode).getTable().isPartitioned();
            List<Column> partitionColumns = ((PhysicalScanOperator) planNode).getTable().getPartitionColumns();
            try {
                List<Long> selectedPartitionIds;
                if (((PhysicalScanOperator) planNode) instanceof PhysicalOlapScanOperator) {
                    selectedPartitionIds = ((PhysicalOlapScanOperator) ((PhysicalScanOperator) planNode))
                            .getSelectedPartitionId();
                } else {
                    selectedPartitionIds = ((PhysicalScanOperator) planNode)
                            .getScanOperatorPredicates().getSelectedPartitionIds().stream().collect(Collectors.toList());
                }
                String tableKey = Utils.getQualifiedTableKey(((PhysicalScanOperator) planNode).getTable());
                ScalarOperator scanToFilterSet = scanToFilterMap.get(tableKey);

                return new ScanPlanStatistics(item.getNodeId(), item.getPushRows(), item.getPullRows(),
                        item.getPredFilterRows(),
                        item.getIndexFilterRows(),
                        item.getRfFilterRows(),
                        (PhysicalScanOperator) planNode, scanToFilterSet,
                        isPartitionedTable, partitionColumns, selectedPartitionIds);
            } catch (AnalysisException e) {
                return null;
            }
        } else {
            return new PlanStatistics(item.getNodeId(), item.getPushRows(), item.getPullRows(),
                    item.getPredFilterRows(), item.getIndexFilterRows(),
                    item.getRfFilterRows());
        }
    }

    public int getNodeId() {
        return nodeId;
    }

    public long getPushRows() {
        return pushRows;
    }

    public long getPullRows() {
        return pullRows;
    }

    public long getPredFilterRows() {
        return predFilterRows;
    }

    public long getIndexFilterRows() {
        return indexFilterRows;
    }

    public long getRfFilterRows() {
        return rfFilterRows;
    }

    public boolean isRuntimeFilterSafeNode(double rfSafeThreshold) {
        // no need to check runtimeFilterInputRows if runtimeFilteredRows is 0 or threshold <= 0
        if (rfSafeThreshold <= 0) {
            return true;
        } else if (rfFilterRows == 0 /*&& runtimeFilterInputRows == 0*/) {
            return true;
        } else if (rfFilterRows > 0) {
            // TODO: fix rf safe
            //double rfFilterRatio = (double) (100 * runtimeFilteredRows / runtimeFilterInputRows);
            //return rfFilterRatio < 100 * rfSafeThreshold;
            return true;
        } else {
            throw new RuntimeException("Illegal runtime stats found");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PlanStatistics other = (PlanStatistics) o;
        return nodeId == other.nodeId
            && pullRows == other.pullRows
            && pushRows == other.pushRows
            && predFilterRows == other.predFilterRows
            && indexFilterRows == other.indexFilterRows
            && rfFilterRows == other.rfFilterRows;
    }
}
