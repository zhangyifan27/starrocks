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
package com.starrocks.sql.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.EmptyStatisticStorage;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.sql.plan.PlanTestNoneDBBase.assertContains;
import static com.starrocks.utframe.UtFrameUtils.printPhysicalPlan;
import static org.junit.Assert.assertEquals;

public class JoinExecModeWithoutStatsTest {
    private static ConnectContext ctx;

    Map<String, String> getFileProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "fake://some_bucket/some_path/*");
        properties.put("format", "csv");
        properties.put("csv.column_separator", "|");
        return properties;
    }

    List<Column> getColumns() {
        return Arrays.asList(
                new Column("purc_purchase_id", Type.INT),
                new Column("purc_store_id", Type.INT),
                new Column("purc_purchase_date", Type.DATE),
                new Column("purc_purchase_time", Type.TIME),
                new Column("purc_purchase_amount", Type.DOUBLE),
                new Column("purc_register_id", Type.VARCHAR)
        );
    }

    List<Column> getColumns1() {
        return Arrays.asList(
                new Column("plin_purchase_id", ScalarType.INT, true),
                new Column("plin_line_number", ScalarType.INT, true),
                new Column("plin_item_id", ScalarType.INT, true),
                new Column("plin_promotion_id", ScalarType.INT, true),
                new Column("plin_quantity", ScalarType.INT, true),
                new Column("plin_unit_price", ScalarType.DOUBLE, true),
                new Column("plin_discount", ScalarType.DOUBLE, true),
                new Column("plin_store_id", ScalarType.INT, true),
                new Column("plin_purchase_id", ScalarType.INT, true),
                new Column("plin_purchase_date", ScalarType.DATE, true),
                new Column("plin_purchase_time", ScalarType.TIME, true),
                new Column("plin_purchase_amount", ScalarType.DOUBLE, true),
                new Column("plin_register_id", ScalarType.VARCHAR, true)
        );
    }

    private static class MockTestStorage extends EmptyStatisticStorage {
        @Override
        public Map<Long, Optional<Long>> getTableStatistics(Long tableId, Collection<Partition> partitions) {
            return partitions.stream().collect(Collectors.toMap(Partition::getId, p -> Optional.of(100000000000L)));
        }
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        ctx = UtFrameUtils.createDefaultCtx();
        ctx.getGlobalStateMgr().setStatisticStorage(new MockTestStorage());
        ConnectorPlanTestBase.mockHiveCatalog(ctx);
        ctx.getSessionVariable().setBroadcastStrictChecks(true);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testHiveScanWithInaccurateStats(@Mocked OlapTable olapTable) {
        OptExpression root;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new LinkedHashMap<>();
        for (Column column : getColumns1()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumnMetaMap.put(columnRefOperator, column);
        }
        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        getColumns1().forEach(c -> idToColumn.put(c.getColumnId(), c));
        HashDistributionInfo hashDistributionInfo1 = new HashDistributionInfo(3, ImmutableList.of(getColumns1().get(0)));
        MaterializedIndex m1 = new MaterializedIndex();
        m1.setRowCount(500000000000L);
        Partition p1 = new Partition(0, "p1", m1, hashDistributionInfo1);
        new Expectations() {
            {
                olapTable.getId();
                result = 0;
                minTimes = 0;

                olapTable.getType();
                result = Table.TableType.OLAP;
                minTimes = 0;

                olapTable.getPartitions();
                result = Lists.newArrayList(p1);
                minTimes = 0;

                olapTable.getPartition(0);
                result = p1;
                minTimes = 0;

                olapTable.getVisiblePartitions();
                result = Lists.newArrayList(p1);
                minTimes = 0;

                olapTable.getDefaultDistributionInfo();
                result = hashDistributionInfo1;
                minTimes = 0;

                olapTable.getPartitionInfo();
                result = new ListPartitionInfo(PartitionType.LIST, ImmutableList.of(getColumns1().get(0)));
                minTimes = 0;

                olapTable.isNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;

                olapTable.getBaseSchema();
                result = new ArrayList<>(colRefToColumnMetaMap.values());
                minTimes = 0;

                olapTable.getIdToColumn();
                result = idToColumn;
                minTimes = 0;
            }
        };
        CallOperator call =
                new CallOperator(FunctionSet.SUM, Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(1)));
        new Expectations(call) {
            {
                call.getFunction();
                minTimes = 0;
                result = AggregateFunction.createBuiltin(FunctionSet.SUM,
                        Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
            }
        };
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap1 = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new LinkedHashMap<>();
        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(olapTable, colRefToColumnMetaMap, Maps.newHashMap(),
                        null, -1, null);
        HiveTable table = (HiveTable) ctx.getGlobalStateMgr().getMetadataMgr()
                .getTable("hive0", "plan_test", "unknown");
        for (Column column : table.getFullSchema()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumnMetaMap1.put(columnRefOperator, column);
            columnMetaToColRefMap.put(column, columnRefOperator);
        }
        LogicalHiveScanOperator hiveScanOperator = new LogicalHiveScanOperator(table, colRefToColumnMetaMap1,
                columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        root = OptExpression.create(
                new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN, new BinaryPredicateOperator(BinaryType.EQ,
                        colRefToColumnMetaMap.keySet().toArray(new ColumnRefOperator[0])[0],
                        colRefToColumnMetaMap1.keySet().toArray(new ColumnRefOperator[0])[0])),
                OptExpression.create(new LogicalJoinOperator(JoinOperator.INNER_JOIN, null),
                        OptExpression.create(olapScanOperator), OptExpression.create(olapScanOperator)),
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL,
                        ImmutableList.copyOf(colRefToColumnMetaMap1.keySet()),
                        ImmutableMap.of(columnRefFactory.create("agg_sum", Type.BIGINT, true), call)),
                        OptExpression.create(hiveScanOperator)));
        Optimizer optimizer = new Optimizer();
        ctx.getSessionVariable().setBroadcastStrictChecks(true);
        OptExpression result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result),
                "LEFT OUTER JOIN (join-predicate [1: plin_purchase_id = 14: c1] post-join-predicate [null])\n" +
                        "    EXCHANGE SHUFFLE[1]\n" +
                        "        INNER JOIN (join-predicate [null] post-join-predicate [null])\n" +
                        "            SCAN (columns[1: plin_purchase_id] predicate[null])\n" +
                        "            EXCHANGE BROADCAST\n" +
                        "                SCAN (columns[1: plin_purchase_id] predicate[null])\n" +
                        "    EXCHANGE SHUFFLE[14]\n" +
                        "        AGGREGATE ([GLOBAL] aggregate [{}] group by [[14: c1, 15: c2, 16: c3, 17: par_col]] " +
                        "having [null]\n" +
                        "            EXCHANGE SHUFFLE[14, 15, 16, 17]\n" +
                        "                AGGREGATE ([LOCAL] aggregate [{}] " +
                        "group by [[14: c1, 15: c2, 16: c3, 17: par_col]] having [null]\n" +
                        "                    HIVE SCAN (columns{14,15,16,17} predicate[null])");
        assertEquals(2.5E23, result.getInputs().get(0).getStatistics().getOutputRowCount(), 0.01);
        assertEquals(true, result.getInputs().get(1).getStatistics().isTableRowCountMayInaccurate());

        ctx.getSessionVariable().setBroadcastStrictChecks(false);
        result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result),
                "LEFT OUTER JOIN (join-predicate [1: plin_purchase_id = 14: c1] post-join-predicate [null])\n" +
                "    INNER JOIN (join-predicate [null] post-join-predicate [null])\n" +
                "        SCAN (columns[1: plin_purchase_id] predicate[null])\n" +
                "        EXCHANGE BROADCAST\n" +
                "            SCAN (columns[1: plin_purchase_id] predicate[null])\n" +
                "    EXCHANGE BROADCAST\n" +
                "        AGGREGATE ([GLOBAL] aggregate [{}] " +
                        "group by [[14: c1, 15: c2, 16: c3, 17: par_col]] having [null]\n" +
                "            EXCHANGE SHUFFLE[14, 15, 16, 17]\n" +
                "                AGGREGATE ([LOCAL] aggregate [{}] " +
                        "group by [[14: c1, 15: c2, 16: c3, 17: par_col]] having [null]\n" +
                "                    HIVE SCAN (columns{14,15,16,17} predicate[null])");
        ctx.getSessionVariable().setBroadcastStrictChecks(true);
    }

    @Test
    public void testFileScanWithInaccurateStats(@Mocked OlapTable olapTable) {
        OptExpression root;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new LinkedHashMap<>();
        for (Column column : getColumns()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumnMetaMap.put(columnRefOperator, column);
            columnMetaToColRefMap.put(column, columnRefOperator);
        }
        LogicalTableFunctionTableScanOperator scanOperator =
                new LogicalTableFunctionTableScanOperator(
                        new TableFunctionTable(getColumns(), getFileProperties(), new SessionVariable()),
                        colRefToColumnMetaMap, columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);

        Map<ColumnRefOperator, Column> scan1ColumnMap = new LinkedHashMap<>();
        for (Column column : getColumns1()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            scan1ColumnMap.put(columnRefOperator, column);
        }
        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        getColumns1().forEach(c -> idToColumn.put(c.getColumnId(), c));
        HashDistributionInfo hashDistributionInfo1 = new HashDistributionInfo(3, ImmutableList.of(getColumns1().get(0)));
        HashDistributionInfo hashDistributionInfo2 = new HashDistributionInfo(3, ImmutableList.of(getColumns1().get(1)));
        MaterializedIndex m1 = new MaterializedIndex();
        m1.setRowCount(100000000000L);
        Partition p1 = new Partition(0, "p1", m1, hashDistributionInfo1);

        MaterializedIndex m2 = new MaterializedIndex();
        m2.setRowCount(200000000000L);
        Partition p2 = new Partition(1, "p2", m2, hashDistributionInfo2);
        List<ColumnRefOperator> outputColumns = new ArrayList<>();
        outputColumns.addAll(colRefToColumnMetaMap.keySet());
        outputColumns.addAll(scan1ColumnMap.keySet());
        new Expectations() {
            {
                olapTable.getId();
                result = 0;
                minTimes = 0;

                olapTable.getType();
                result = Table.TableType.OLAP;
                minTimes = 0;

                olapTable.getPartitions();
                result = Lists.newArrayList(p1, p2);
                minTimes = 0;

                olapTable.getPartition(0);
                result = p1;
                minTimes = 0;

                olapTable.getPartition(1);
                result = p2;
                minTimes = 0;

                olapTable.getVisiblePartitions();
                result = Lists.newArrayList(p1, p2);
                minTimes = 0;

                olapTable.getDefaultDistributionInfo();
                result = hashDistributionInfo1;
                minTimes = 0;

                olapTable.getPartitionInfo();
                result = new ListPartitionInfo(PartitionType.LIST, ImmutableList.of(getColumns1().get(0), getColumns1().get(1)));
                minTimes = 0;

                olapTable.isNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;

                olapTable.getBaseSchema();
                result = new ArrayList<>(scan1ColumnMap.values());
                minTimes = 0;

                olapTable.getIdToColumn();
                result = idToColumn;
                minTimes = 0;
            }
        };
        LogicalOlapScanOperator scanOperator2 =
                new LogicalOlapScanOperator(olapTable, scan1ColumnMap, Maps.newHashMap(),
                        null, -1, null);
        LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ,
                        colRefToColumnMetaMap.keySet().toArray(new ColumnRefOperator[0])[0],
                        scan1ColumnMap.keySet().toArray(new ColumnRefOperator[0])[0]));
        joinOperator.setProjection(
                new Projection(outputColumns.stream().collect(Collectors.toMap(c -> c, c -> c))));
        root = OptExpression.create(joinOperator,
                OptExpression.create(scanOperator2),
                OptExpression.create(scanOperator));
        Optimizer optimizer = new Optimizer();
        ctx.getSessionVariable().setBroadcastStrictChecks(true);
        OptExpression result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(outputColumns),
                columnRefFactory);
        assertContains(printPhysicalPlan(result),
                "INNER JOIN (join-predicate [1: purc_purchase_id = 7: plin_purchase_id] post-join-predicate [null])\n" +
                        "    EXCHANGE SHUFFLE[7]\n" +
                        "        SCAN (columns[7: plin_purchase_id, 8: plin_line_number, " +
                        "9: plin_item_id, 10: plin_promotion_id, " +
                        "11: plin_quantity, 12: plin_unit_price, 13: plin_discount, 14: plin_store_id, 15: plin_purchase_id, " +
                        "16: plin_purchase_date, 17: plin_purchase_time, 18: plin_purchase_amount, 19: plin_register_id] " +
                        "predicate[7: plin_purchase_id IS NOT NULL])\n" +
                        "    EXCHANGE SHUFFLE[1]\n" +
                        "        - TableFunctionScan[TABLE('path'='fake://some_bucket/some_path/*/', 'format'='csv')]" +
                        "[1: purc_purchase_id, 2: purc_store_id, 3: purc_purchase_date, 4: purc_purchase_time, " +
                        "5: purc_purchase_amount, 6: purc_register_id]");
        assertEquals(2.7E11, result.getInputs().get(0).getStatistics().getOutputRowCount(), 0.01);
        assertEquals(true, result.getInputs().get(1).getStatistics().isTableRowCountMayInaccurate());

        CallOperator call =
                new CallOperator(FunctionSet.SUM, Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(1)));
        new Expectations(call) {
            {
                call.getFunction();
                minTimes = 0;
                result = AggregateFunction.createBuiltin(FunctionSet.SUM,
                        Lists.<Type>newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false);
            }
        };
        root = OptExpression.create(joinOperator,
                OptExpression.create(scanOperator2),
                OptExpression.create(new LogicalAggregationOperator(AggType.GLOBAL,
                                ImmutableList.copyOf(colRefToColumnMetaMap.keySet()),
                                ImmutableMap.of(columnRefFactory.create("agg_sum", Type.BIGINT, true), call)),
                        OptExpression.create(scanOperator)));
        joinOperator.setProjection(null);
        result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result), "INNER JOIN (join-predicate [1: purc_purchase_id = 7: plin_purchase_id] " +
                "post-join-predicate [null])\n" +
                "    EXCHANGE SHUFFLE[7]\n" +
                "        SCAN (columns[7: plin_purchase_id] predicate[7: plin_purchase_id IS NOT NULL])\n" +
                "    EXCHANGE SHUFFLE[1]\n" +
                "        AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: purc_purchase_id, 2: purc_store_id, " +
                "3: purc_purchase_date, 4: purc_purchase_time, 5: purc_purchase_amount, 6: purc_register_id]] having [null]\n" +
                "            EXCHANGE SHUFFLE[1, 2, 3, 4, 5, 6]\n" +
                "                AGGREGATE ([LOCAL] aggregate [{}] group by [[1: purc_purchase_id, 2: purc_store_id, " +
                "3: purc_purchase_date, 4: purc_purchase_time, 5: purc_purchase_amount, 6: purc_register_id]] having [null]\n" +
                "                    - TableFunctionScan[TABLE('path'='fake://some_bucket/some_path/*/', 'format'='csv')]" +
                "[1: purc_purchase_id, 2: purc_store_id, 3: purc_purchase_date, 4: purc_purchase_time, " +
                "5: purc_purchase_amount, 6: purc_register_id]\n");
        assertEquals(2.7E11, result.getInputs().get(0).getStatistics().getOutputRowCount(), 0.01);
        assertEquals(true, result.getInputs().get(1).getStatistics().isTableRowCountMayInaccurate());

        ctx.getSessionVariable().setBroadcastStrictChecks(false);
        result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result), "INNER JOIN (join-predicate [1: purc_purchase_id = " +
                "7: plin_purchase_id] post-join-predicate [null])\n" +
                "    SCAN (columns[7: plin_purchase_id] predicate[7: plin_purchase_id IS NOT NULL])\n" +
                "    EXCHANGE BROADCAST\n" +
                "        AGGREGATE ([GLOBAL] aggregate [{}] group by [[1: purc_purchase_id, 2: purc_store_id, " +
                "3: purc_purchase_date, 4: purc_purchase_time, 5: purc_purchase_amount, 6: purc_register_id]] having [null]\n" +
                "            EXCHANGE SHUFFLE[1, 2, 3, 4, 5, 6]\n" +
                "                AGGREGATE ([LOCAL] aggregate [{}] group by [[1: purc_purchase_id, 2: purc_store_id, " +
                "3: purc_purchase_date, 4: purc_purchase_time, 5: purc_purchase_amount, 6: purc_register_id]] having [null]\n" +
                "                    - TableFunctionScan[TABLE('path'='fake://some_bucket/some_path/*/', 'format'='csv')]" +
                "[1: purc_purchase_id, 2: purc_store_id, 3: purc_purchase_date, " +
                "4: purc_purchase_time, 5: purc_purchase_amount, " +
                "6: purc_register_id]\n");
        assertEquals(1.0, result.getInputs().get(1).getStatistics().getOutputRowCount(), 0.01);
        assertEquals(2.7E11, result.getInputs().get(0).getStatistics().getOutputRowCount(), 0.01);
    }

    @Test
    public void testPreferBroadcast(@Mocked OlapTable olapTable) {
        OptExpression root;
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new LinkedHashMap<>();
        for (Column column : getColumns1()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumnMetaMap.put(columnRefOperator, column);
        }
        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        getColumns1().forEach(c -> idToColumn.put(c.getColumnId(), c));
        HashDistributionInfo hashDistributionInfo1 = new HashDistributionInfo(3, ImmutableList.of(getColumns1().get(0)));
        MaterializedIndex m1 = new MaterializedIndex();
        m1.setRowCount(50000L);
        Partition p1 = new Partition(0,  "p1", m1, hashDistributionInfo1);
        new Expectations() {
            {
                olapTable.getId();
                result = 0;
                minTimes = 0;

                olapTable.getType();
                result = Table.TableType.OLAP;
                minTimes = 0;

                olapTable.getPartitions();
                result = Lists.newArrayList(p1);
                minTimes = 0;

                olapTable.getPartition(0);
                result = p1;
                minTimes = 0;

                olapTable.getVisiblePartitions();
                result = Lists.newArrayList(p1);
                minTimes = 0;

                olapTable.getDefaultDistributionInfo();
                result = hashDistributionInfo1;
                minTimes = 0;

                olapTable.getPartitionInfo();
                result = new ListPartitionInfo(PartitionType.LIST, ImmutableList.of(getColumns1().get(0)));
                minTimes = 0;

                olapTable.isNativeTableOrMaterializedView();
                result = true;
                minTimes = 0;

                olapTable.getBaseSchema();
                result = new ArrayList<>(colRefToColumnMetaMap.values());
                minTimes = 0;

                olapTable.getIdToColumn();
                result = idToColumn;
                minTimes = 0;
            }
        };

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap1 = new LinkedHashMap<>();
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new LinkedHashMap<>();
        for (Column column : getColumns()) {
            ColumnRefOperator columnRefOperator = columnRefFactory.create(column.getName(), column.getType(), true);
            colRefToColumnMetaMap1.put(columnRefOperator, column);
            columnMetaToColRefMap.put(column, columnRefOperator);
        }
        LogicalTableFunctionTableScanOperator scanOperator =
                new LogicalTableFunctionTableScanOperator(
                        new TableFunctionTable(getColumns(), getFileProperties(), new SessionVariable()),
                        colRefToColumnMetaMap1, columnMetaToColRefMap, Operator.DEFAULT_LIMIT, null);
        LogicalOlapScanOperator olapScanOperator =
                new LogicalOlapScanOperator(olapTable, colRefToColumnMetaMap, Maps.newHashMap(),
                        null, -1, null);

        LogicalJoinOperator testJoinOrder = new LogicalJoinOperator(JoinOperator.LEFT_OUTER_JOIN,
                new BinaryPredicateOperator(BinaryType.EQ,
                        colRefToColumnMetaMap1.keySet().toArray(new ColumnRefOperator[0])[0],
                        colRefToColumnMetaMap.keySet().toArray(new ColumnRefOperator[0])[0]));

        root = OptExpression.create(testJoinOrder, OptExpression.create(scanOperator), OptExpression.create(olapScanOperator));
        Optimizer optimizer = new Optimizer();
        ctx.getSessionVariable().setBroadcastStrictChecks(true);
        OptExpression result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result),
                "LEFT OUTER JOIN (join-predicate [14: purc_purchase_id = 1: plin_purchase_id] " +
                "post-join-predicate [null])\n" +
                "    - TableFunctionScan[TABLE('path'='fake://some_bucket/some_path/*/', 'format'='csv')][14: purc_purchase_id]\n" +
                "\n" +
                "    EXCHANGE BROADCAST\n" +
                "        SCAN (columns[1: plin_purchase_id] predicate[null])");
        ctx.getSessionVariable().setBroadcastStrictChecks(false);
        result = optimizer.optimize(ctx, root, new PhysicalPropertySet(), new ColumnRefSet(),
                columnRefFactory);
        assertContains(printPhysicalPlan(result),
                "RIGHT OUTER JOIN (join-predicate [14: purc_purchase_id = 1: plin_purchase_id]" +
                " post-join-predicate [null])\n" +
                "    EXCHANGE SHUFFLE[1]\n" +
                "        SCAN (columns[1: plin_purchase_id] predicate[null])\n" +
                "    EXCHANGE SHUFFLE[14]\n" +
                "        - TableFunctionScan[TABLE('path'='fake://some_bucket/some_path/*/', 'format'='csv')][14: purc_purchase_id]");
    }
}
