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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;

public class SimpleLimitPlanner {

    public static void checkSimpleLimit(OptExpression root, ConnectContext connectContext) {
        if (connectContext == null) {
            return;
        }
        connectContext.setSimpleLimit(-1);
        if (connectContext.getSessionVariable() == null) {
            return;
        }
        if (!connectContext.getSessionVariable().isEnablePrunePartitionSimpleQuery()) {
            return;
        }
        if (!(root.getOp() instanceof LogicalLimitOperator)) {
            return;
        }
        if (root.getInputs().size() != 1) {
            return;
        }
        SimpleQueryContext simpleQueryContext = new SimpleQueryContext();
        if (!isLogicalSPF(root.inputAt(0), simpleQueryContext)) {
            return;
        }
        if (simpleQueryContext.getLogicalHiveScanOperators().size() != 1) {
            return;
        }
        if (simpleQueryContext.getLogicalFilterOperators().size() > 1) {
            return;
        }
        if (simpleQueryContext.getLogicalFilterOperators().size() == 1) {
            if (!checkOnlySimplePartitionFilter(simpleQueryContext.getLogicalHiveScanOperators().get(0),
                    simpleQueryContext.getLogicalFilterOperators().get(0))) {
                return;
            }
        }
        LogicalLimitOperator limit = (LogicalLimitOperator) root.getOp();
        if (limit.hasOffset()) {
            return;
        }
        connectContext.setSimpleLimit(limit.getLimit());
    }

    public static class SimpleQueryContext {
        private List<LogicalHiveScanOperator> logicalHiveScanOperators = new ArrayList<>();
        private List<LogicalFilterOperator> logicalFilterOperators = new ArrayList<>();

        public List<LogicalHiveScanOperator> getLogicalHiveScanOperators() {
            return logicalHiveScanOperators;
        }

        public List<LogicalFilterOperator> getLogicalFilterOperators() {
            return logicalFilterOperators;
        }
    }

    /**
     * Whether `root` and its children are Select/Project/Filter ops.
     */
    public static boolean isLogicalSPF(OptExpression root, SimpleQueryContext simpleQueryContext) {
        if (root == null) {
            return false;
        }
        Operator operator = root.getOp();
        if (!isSimpleOperator(operator)) {
            return false;
        }
        if (operator instanceof LogicalHiveScanOperator) {
            simpleQueryContext.getLogicalHiveScanOperators().add((LogicalHiveScanOperator) operator);
        }
        if (operator instanceof LogicalFilterOperator) {
            simpleQueryContext.getLogicalFilterOperators().add((LogicalFilterOperator) operator);
        }
        if (root.getInputs().size() > 1) {
            return false;
        }
        for (OptExpression child : root.getInputs()) {
            if (!isLogicalSPF(child, simpleQueryContext)) {
                return false;
            }
        }
        return true;
    }

    public static boolean isSimpleOperator(Operator operator) {
        return (operator instanceof LogicalHiveScanOperator)
                || (operator instanceof LogicalProjectOperator)
                || (operator instanceof LogicalFilterOperator);
    }

    // only partition filter, support only par_column = "xxx"
    public static boolean checkOnlySimplePartitionFilter(LogicalHiveScanOperator hiveScanOperator,
                                                         LogicalFilterOperator filterOperator) {
        HiveTable hiveTable = (HiveTable) hiveScanOperator.getTable();
        ScalarOperator predicate = filterOperator.getPredicate();
        if (!(predicate instanceof BinaryPredicateOperator)) {
            return false;
        }
        BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
        if (!binaryPredicate.getBinaryType().isEqual()) {
            return false;
        }
        ScalarOperator left = binaryPredicate.getChild(0);
        ScalarOperator right = binaryPredicate.getChild(1);
        if (right.isConstantRef()) {
            if (left instanceof ColumnRefOperator) {
                List<Column> partitionColumns = null;
                if (hiveTable.isThiveTable()) {
                    partitionColumns = hiveTable.getThivePartitionColumns();
                } else {
                    partitionColumns = hiveTable.getPartitionColumns();
                }
                List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
                for (Column column : partitionColumns) {
                    ColumnRefOperator partitionColumnRefOperator = hiveScanOperator.getColumnReference(column);
                    partitionColumnRefOperators.add(partitionColumnRefOperator);
                }
                if (partitionColumnRefOperators.contains(left)) {
                    return true;
                }
            }
        } else if (left.isConstantRef()) {
            if (right instanceof ColumnRefOperator) {
                List<Column> partitionColumns = null;
                if (hiveTable.isThiveTable()) {
                    partitionColumns = hiveTable.getThivePartitionColumns();
                } else {
                    partitionColumns = hiveTable.getPartitionColumns();
                }
                List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
                for (Column column : partitionColumns) {
                    ColumnRefOperator partitionColumnRefOperator = hiveScanOperator.getColumnReference(column);
                    partitionColumnRefOperators.add(partitionColumnRefOperator);
                }
                if (partitionColumnRefOperators.contains(right)) {
                    return true;
                }
            }
        }
        return false;
    }

}
