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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RewriteSimpleAggToHDFSScanRuleTest {

    @Mocked
    private GlobalStateMgr globalStateMgr;

    private OptimizerContext context;
    private ColumnRefFactory columnRefFactory;

    @Before
    public void setUp() {
        columnRefFactory = new ColumnRefFactory();
        context = new OptimizerContext(new Memo(), columnRefFactory);
    }

    /**
     * Test case: Session variable is disabled
     * Expected: check returns false
     */
    @Test
    public void testCheckFailsWhenSessionVariableDisabled(@Mocked HiveTable table) {
        // Setup
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = false;
            }
        };

        ColumnRefOperator col1 = columnRefFactory.create("col1", Type.INT, false);
        Map<ColumnRefOperator, Column> scanColumns = new HashMap<>();
        scanColumns.put(col1, new Column("col1", Type.INT));

        LogicalHiveScanOperator scanOp = new LogicalHiveScanOperator(
                table, scanColumns, Maps.newHashMap(), -1, null);

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        CallOperator countCall = createCountStarCall();
        aggregations.put(columnRefFactory.create("count", Type.BIGINT, false), countCall);

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                com.starrocks.sql.optimizer.operator.AggType.GLOBAL,
                new ArrayList<>(),
                aggregations);

        OptExpression aggExpr = OptExpression.create(aggOp);
        aggExpr.getInputs().add(OptExpression.create(scanOp));

        // Execute
        RewriteSimpleAggToHDFSScanRule rule = RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT;
        boolean result = rule.check(aggExpr, context);

        // Verify
        Assert.assertFalse("Check should fail when session variable is disabled", result);
    }

    /**
     * Test case: Predicate contains non-partition columns
     * Expected: check returns false
     */
    @Test
    public void testCheckFailsWhenPredicateHasNonPartitionColumns(@Mocked HiveTable table) {
        // Setup
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = true;
            }
        };

        ColumnRefOperator partCol = columnRefFactory.create("part_col", Type.STRING, false);
        ColumnRefOperator dataCol = columnRefFactory.create("data_col", Type.INT, false);

        Map<ColumnRefOperator, Column> scanColumns = new HashMap<>();
        scanColumns.put(partCol, new Column("part_col", Type.STRING));
        scanColumns.put(dataCol, new Column("data_col", Type.INT));

        // Predicate on non-partition column
        ScalarOperator predicate = new com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator(
                com.starrocks.analysis.BinaryType.EQ,
                dataCol,
                ConstantOperator.createInt(100));

        LogicalHiveScanOperator scanOp = new LogicalHiveScanOperator(
                table, scanColumns, Maps.newHashMap(), -1, predicate);

        new Expectations(scanOp) {
            {
                scanOp.getPartitionColumns();
                result = Sets.newHashSet("part_col");
            }
        };

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        CallOperator countCall = createCountStarCall();
        aggregations.put(columnRefFactory.create("count", Type.BIGINT, false), countCall);

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                com.starrocks.sql.optimizer.operator.AggType.GLOBAL,
                new ArrayList<>(),
                aggregations);

        OptExpression aggExpr = OptExpression.create(aggOp);
        aggExpr.getInputs().add(OptExpression.create(scanOp));

        // Execute
        RewriteSimpleAggToHDFSScanRule rule = RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT;
        boolean result = rule.check(aggExpr, context);

        // Verify
        Assert.assertFalse("Check should fail when predicate has non-partition columns", result);
    }

    /**
     * Test case: Group by keys contain non-partition columns
     * Expected: check returns false
     */
    @Test
    public void testCheckFailsWhenGroupByHasNonPartitionColumns(@Mocked HiveTable table) {
        // Setup
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = true;
            }
        };

        ColumnRefOperator partCol = columnRefFactory.create("part_col", Type.STRING, false);
        ColumnRefOperator dataCol = columnRefFactory.create("data_col", Type.INT, false);

        Map<ColumnRefOperator, Column> scanColumns = new HashMap<>();
        scanColumns.put(partCol, new Column("part_col", Type.STRING));
        scanColumns.put(dataCol, new Column("data_col", Type.INT));

        LogicalHiveScanOperator scanOp = new LogicalHiveScanOperator(
                table, scanColumns, Maps.newHashMap(), -1, null);

        new Expectations(scanOp) {
            {
                scanOp.getPartitionColumns();
                result = Sets.newHashSet("part_col");
            }
        };

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        CallOperator countCall = createCountStarCall();
        aggregations.put(columnRefFactory.create("count", Type.BIGINT, false), countCall);

        // Group by non-partition column
        List<ColumnRefOperator> groupingKeys = Lists.newArrayList(dataCol);

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                com.starrocks.sql.optimizer.operator.AggType.GLOBAL,
                groupingKeys,
                aggregations);

        OptExpression aggExpr = OptExpression.create(aggOp);
        aggExpr.getInputs().add(OptExpression.create(scanOp));

        // Execute
        RewriteSimpleAggToHDFSScanRule rule = RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT;
        boolean result = rule.check(aggExpr, context);

        // Verify
        Assert.assertFalse("Check should fail when group by has non-partition columns", result);
    }

    /**
     * Test case: Thive table with group by keys
     * Expected: check returns false
     */
    @Test
    public void testCheckFailsWhenThiveTableHasGroupBy(@Mocked HiveTable table) {
        // Setup
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = true;

                table.isThiveTable();
                result = true;

                table.getName();
                result = "thive_table";
            }
        };

        ColumnRefOperator partCol = columnRefFactory.create("part_col", Type.STRING, false);

        Map<ColumnRefOperator, Column> scanColumns = new HashMap<>();
        scanColumns.put(partCol, new Column("part_col", Type.STRING));

        LogicalHiveScanOperator scanOp = new LogicalHiveScanOperator(
                table, scanColumns, Maps.newHashMap(), -1, null);

        new Expectations(scanOp) {
            {
                scanOp.getPartitionColumns();
                result = Sets.newHashSet("part_col");
            }
        };

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        CallOperator countCall = createCountStarCall();
        aggregations.put(columnRefFactory.create("count", Type.BIGINT, false), countCall);

        // Group by partition column (not allowed for Thive tables)
        List<ColumnRefOperator> groupingKeys = Lists.newArrayList(partCol);

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                com.starrocks.sql.optimizer.operator.AggType.GLOBAL,
                groupingKeys,
                aggregations);

        OptExpression aggExpr = OptExpression.create(aggOp);
        aggExpr.getInputs().add(OptExpression.create(scanOp));

        // Execute
        RewriteSimpleAggToHDFSScanRule rule = RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT;
        boolean result = rule.check(aggExpr, context);

        // Verify
        Assert.assertFalse("Check should fail when Thive table has group by", result);
    }

    /**
     * Test case: Valid count(*) with group by partition columns
     * Expected: check returns true
     */
    @Test
    public void testCheckSucceedsForCountWithPartitionGroupBy(@Mocked HiveTable table) {
        // Setup
        new Expectations() {
            {
                context.getSessionVariable().isEnableRewriteSimpleAggToHdfsScan();
                result = true;

                table.isThiveTable();
                result = false;
            }
        };

        ColumnRefOperator partCol = columnRefFactory.create("part_col", Type.STRING, false);
        Map<ColumnRefOperator, Column> scanColumns = new HashMap<>();
        scanColumns.put(partCol, new Column("part_col", Type.STRING));

        LogicalHiveScanOperator scanOp = new LogicalHiveScanOperator(
                table, scanColumns, Maps.newHashMap(), -1, null);

        new Expectations(scanOp) {
            {
                scanOp.getPartitionColumns();
                result = Sets.newHashSet("part_col");
            }
        };

        Map<ColumnRefOperator, CallOperator> aggregations = new HashMap<>();
        CallOperator countCall = createCountStarCall();
        aggregations.put(columnRefFactory.create("count", Type.BIGINT, false), countCall);

        List<ColumnRefOperator> groupingKeys = Lists.newArrayList(partCol);

        LogicalAggregationOperator aggOp = new LogicalAggregationOperator(
                com.starrocks.sql.optimizer.operator.AggType.GLOBAL,
                groupingKeys,
                aggregations);

        OptExpression aggExpr = OptExpression.create(aggOp);
        aggExpr.getInputs().add(OptExpression.create(scanOp));

        // Execute
        RewriteSimpleAggToHDFSScanRule rule = RewriteSimpleAggToHDFSScanRule.HIVE_SCAN_NO_PROJECT;
        boolean result = rule.check(aggExpr, context);

        // Verify
        Assert.assertTrue("Check should succeed for count with partition group by", result);
    }


    // Helper methods to create CallOperators

    private CallOperator createCountStarCall() {
        CallOperator call = new CallOperator(FunctionSet.COUNT, Type.BIGINT, Collections.emptyList());
        new Expectations(call) {
            {
                call.getUsedColumns();
                result = new ColumnRefSet();
                minTimes = 0;

                call.getFunction();
                result = AggregateFunction.createBuiltin(FunctionSet.COUNT,
                        Lists.<Type>newArrayList(), Type.BIGINT, Type.BIGINT, false, true, false);
                minTimes = 0;
            }
        };
        return call;
    }
}
