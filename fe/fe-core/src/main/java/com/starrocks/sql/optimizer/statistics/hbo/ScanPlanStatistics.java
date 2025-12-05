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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScanPlanStatistics extends PlanStatistics {
    private PhysicalScanOperator scan;
    private ImmutableList<Long> selectedPartitionIds;
    private Set<ScalarOperator> partitionColumnPredicates = new HashSet<>();
    private Set<ScalarOperator> otherPredicate = new HashSet<>();
    private final ScalarOperator tableFilterSet;
    private final List<Column> partitionColumns;
    private final boolean isPartitionedTable;

    public ScanPlanStatistics(int nodeId, long pushRows, long pullRows,
                              long predFilterRows, long indexFilterRows, long rfFilterRows,
                              PhysicalScanOperator scan, ScalarOperator tableFilterSet, boolean isPartitionedTable,
                              List<Column> partitionColumns, List<Long> selectedPartitionIds) {
        super(nodeId, pushRows, pullRows, predFilterRows, indexFilterRows, rfFilterRows);
        this.scan = scan;
        this.tableFilterSet = tableFilterSet;
        this.isPartitionedTable = isPartitionedTable;
        this.partitionColumns = partitionColumns;
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        buildPartitionColumnPredicatesAndOthers(scan, tableFilterSet, partitionColumns);
    }

    public ScanPlanStatistics(PlanStatistics other, PhysicalScanOperator scan, ScalarOperator tableFilterSet,
                              boolean isPartitionedTable, List<Column> partitionColumns, List<Long> selectedPartitionIds) {
        super(other.nodeId, other.pushRows, other.pullRows, other.predFilterRows, other.indexFilterRows, other.rfFilterRows);
        this.scan = scan;
        this.tableFilterSet = tableFilterSet;
        this.isPartitionedTable = isPartitionedTable;
        this.partitionColumns = partitionColumns;
        this.selectedPartitionIds = ImmutableList.copyOf(selectedPartitionIds);
        buildPartitionColumnPredicatesAndOthers(scan, tableFilterSet, partitionColumns);
    }

    public void buildPartitionColumnPredicatesAndOthers(PhysicalScanOperator scan,
                                                        ScalarOperator tableFilterSet, List<Column> partitionColumns) {
        partitionColumnPredicates.clear();
        otherPredicate.clear();
        Map<ColumnRefOperator, Column> map = scan.getColRefToColumnMetaMap();
        if (tableFilterSet != null) {
            for (ScalarOperator expr : tableFilterSet.getChildren()) {
                List<ColumnRefOperator> inputSlot = expr.getColumnRefs();
                if (inputSlot.size() == 1 && inputSlot.get(0).isColumnRef()) {
                    ColumnRefOperator filterColumnRef = inputSlot.get(0).getColumnRefs().get(0);
                    Column filterColumn = map.get(filterColumnRef);
                    if (partitionColumns.contains(filterColumn)) {
                        partitionColumnPredicates.add(expr);
                    } else {
                        otherPredicate.add(expr);
                    }
                } else {
                    otherPredicate.add(expr);
                }
            }
        }
    }

    public boolean hasSameOtherPredicates(ScanPlanStatistics other) {
        return this.otherPredicate.containsAll(other.otherPredicate)
                && other.otherPredicate.containsAll(this.otherPredicate);
    }

    public boolean hasSamePartitionColumnPredicates(ScanPlanStatistics other) {
        return this.partitionColumnPredicates.containsAll(other.partitionColumnPredicates)
                && other.partitionColumnPredicates.containsAll(this.partitionColumnPredicates);
    }

    public boolean hasSamePartitionId(ScanPlanStatistics other) {
        return this.selectedPartitionIds.equals(other.selectedPartitionIds);
    }

    public boolean isPartitionedTable() {
        return this.isPartitionedTable;
    }

    public PhysicalScanOperator getScan() {
        return scan;
    }
}
