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

package com.starrocks.sql.optimizer.statistics;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.planner.PlanNodeAndHash;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HboStatsCalculatorTest {

    private HboStatsCalculator hboStatsCalculator;
    private ExpressionContext expressionContext;
    private ColumnRefFactory columnRefFactory;
    private OptimizerContext optimizerContext;

    @Mocked
    private HboPlanStatisticsManager hboPlanStatisticsManager;

    @Mocked
    private MemoryHboPlanStatisticsProvider hboPlanStatisticsProvider;

    @Mocked
    private RecentRunsPlanStatistics recentRunsPlanStatistics;

    @Mocked
    private PlanStatistics planStatistics;

    private OlapTable olapTable;
    private Statistics mockStatistics;
    private ColumnRefOperator columnRef1;
    private ColumnRefOperator columnRef2;
    private Column column1;
    private Column column2;

    private LogicalOlapScanOperator scanOperator;
    private OptExpression baseOptExpression;
    private ConnectContext connectContext;
    private GroupExpression groupExpression;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(new Memo(), columnRefFactory, connectContext);

        setupTestColumns();
        setupTestTables();
        scanOperator = createLogicalOlapScanOperator();
        baseOptExpression = OptExpression.create(scanOperator);
        Group group = new Group(1);
        groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(group);
        expressionContext = new ExpressionContext(baseOptExpression);

        mockStatistics = Statistics.builder()
                .setOutputRowCount(1000)
                .addColumnStatistic(columnRef1, ColumnStatistic.unknown())
                .addColumnStatistic(columnRef2, ColumnStatistic.unknown())
                .build();

        setupMocks();

        hboStatsCalculator = new HboStatsCalculator(expressionContext, columnRefFactory, optimizerContext);
    }

    private void setupTestTables() {
        // 创建 OlapTable
        List<Column> olapColumns = new ArrayList<>();
        olapColumns.add(column1);
        olapColumns.add(column2);
        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();

        DistributionInfo distributionInfo = new HashDistributionInfo(32,
                Lists.newArrayList(olapColumns.get(0)));
        olapTable = new OlapTable(1L, "test_olap_table", olapColumns, KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
    }

    private void setupTestColumns() {
        column1 = new Column("col1", Type.INT);
        column2 = new Column("col2", Type.STRING);
        columnRef1 = columnRefFactory.create("col1", Type.INT, true);
        columnRef2 = columnRefFactory.create("col2", Type.STRING, true);
    }

    private void setupMocks() {
        hboPlanStatisticsManager = GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager();
        hboPlanStatisticsProvider = new MemoryHboPlanStatisticsProvider();
    }

    @Test
    public void testConstructorNormal() {
        HboStatsCalculator calculator = new HboStatsCalculator(expressionContext, columnRefFactory, optimizerContext);
        Assert.assertNotNull(calculator);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullProvider() {
        new Expectations() {
            {
                hboPlanStatisticsManager.getHboPlanStatisticsProvider();
                result = null;
            }
        };

        new HboStatsCalculator(expressionContext, columnRefFactory, optimizerContext);
    }

    /*
    @Test
    public void testComputeOlapScanNodeWithGroupExpression() {
        // 准备测试数据
        LogicalOlapScanOperator scanOperator = createLogicalOlapScanOperator();
        Group group = new Group(1);
        GroupExpression groupExpression = new GroupExpression(scanOperator, Lists.newArrayList());
        groupExpression.setGroup(group);
        expressionContext.setGroupExpression(groupExpression);
        expressionContext.setStatistics(mockStatistics);
        
        Collection<Long> selectedPartitionIds = Lists.newArrayList(1L, 2L);
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        colRefToColumnMetaMap.put(columnRef1, column1);

        setupHboStatsMocks(900L);

        hboStatsCalculator.computeOlapScanNode(scanOperator, expressionContext, olapTable, selectedPartitionIds,
            colRefToColumnMetaMap);

        Statistics result = expressionContext.getStatistics();
        Assert.assertNotNull(result);
        Assert.assertEquals(900.0, result.getOutputRowCount(), 0.001);
        Assert.assertTrue(result.isFromHbo());
    }

    @Test
    public void testComputeJoinNodeWithOptExpression() {
        ScalarOperator joinOnPredicate = new BinaryPredicateOperator(
                com.starrocks.analysis.BinaryType.EQ, columnRef1, columnRef2);
        LogicalJoinOperator joinOperator = new LogicalJoinOperator(JoinOperator.INNER_JOIN, joinOnPredicate);
        OptExpression optExpression = OptExpression.create(joinOperator);
        expressionContext.setOptExpression(optExpression);
        expressionContext.setStatistics(mockStatistics);

        setupHboStatsMocks(500L);

        hboStatsCalculator.computeJoinNode(expressionContext, JoinOperator.INNER_JOIN, joinOnPredicate);

        Statistics result = expressionContext.getStatistics();
        Assert.assertNotNull(result);
        Assert.assertEquals(500.0, result.getOutputRowCount(), 0.001);
        Assert.assertTrue(result.isFromHbo());
    }

    @Test
    public void testComputeAggregateNodeWithOptExpression() {
        LogicalAggregationOperator aggOperator = new LogicalAggregationOperator(
                AggType.GLOBAL, Lists.newArrayList(columnRef1), Maps.newHashMap());
        OptExpression optExpression = OptExpression.create(aggOperator);
        expressionContext.setOptExpression(optExpression);
        expressionContext.setStatistics(mockStatistics);
        
        List<ColumnRefOperator> groupBys = Lists.newArrayList(columnRef1);
        Map<ColumnRefOperator, CallOperator> aggregations = Maps.newHashMap();

        setupHboStatsMocks(300L);

        hboStatsCalculator.computeAggregateNode(aggOperator, expressionContext, groupBys, aggregations);

        Statistics result = expressionContext.getStatistics();
        Assert.assertNotNull(result);
        Assert.assertEquals(300.0, result.getOutputRowCount(), 0.001);
        Assert.assertTrue(result.isFromHbo());
    }

    @Test
    public void testStaticGetStatsFromHboPlanStatsNormal() {
        List<ColumnRefOperator> columns = Lists.newArrayList(columnRef1, columnRef2);

        setupHboStatsMocks(2500L);

        Statistics result = HboStatsCalculator.getStatsFromHboPlanStats(
                optimizerContext, olapTable, mockStatistics, columns);

        Assert.assertNotNull(result);
        Assert.assertEquals(2500.0, result.getOutputRowCount(), 0.001);
        Assert.assertTrue(result.isFromHbo());
    }

    @Test
    public void testStaticGetStatsFromHboPlanStatsWithNullProvider() {
        new Expectations() {{
            hboPlanStatisticsManager.getHboPlanStatisticsProvider();
            result = null;
        }};
        
        List<ColumnRefOperator> columns = Lists.newArrayList(columnRef1);

        Statistics result = HboStatsCalculator.getStatsFromHboPlanStats(
                optimizerContext, olapTable, mockStatistics, columns);
        Assert.assertNotNull(result);
        Assert.assertEquals(1000.0, result.getOutputRowCount(), 0.001); // 原始值
        Assert.assertFalse(result.isFromHbo());
    }*/

    private LogicalOlapScanOperator createLogicalOlapScanOperator() {
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();
        colRefToColumnMetaMap.put(columnRef1, column1);
        colRefToColumnMetaMap.put(columnRef2, column2);
        
        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>();
        columnMetaToColRefMap.put(column1, columnRef1);
        columnMetaToColRefMap.put(column2, columnRef2);

        return  new LogicalOlapScanOperator(olapTable, colRefToColumnMetaMap, columnMetaToColRefMap,
                null, -1, ConstantOperator.createBoolean(true),
                1, Lists.newArrayList(), null,
                false, Lists.newArrayList(), null, null, false);
    }

    private void setupHboStatsMocks(long pullRows) {
        new Expectations() {
            {
                HboUtils.getMatchedPlanStatistics(recentRunsPlanStatistics, optimizerContext);
                result = planStatistics;

                HboUtils.getOperatorHash((OptExpression) baseOptExpression);
                result = new PlanNodeAndHash(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN),
                        Optional.of("test_hash"));
                minTimes = 0;

                HboUtils.getOperatorHash((GroupExpression) groupExpression);
                result = new PlanNodeAndHash(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN),
                        Optional.of("test_hash"));
                minTimes = 0;

                HboUtils.getOperatorHash((com.starrocks.sql.optimizer.operator.Operator) scanOperator);
                result = new PlanNodeAndHash(new MockOperator(OperatorType.LOGICAL_OLAP_SCAN),
                        Optional.of("test_hash"));
                minTimes = 0;
            }
        };
    }
}