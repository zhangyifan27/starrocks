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
import com.google.common.collect.Maps;
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
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.PlanStatisticsMatchStrategy;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatistics;
import com.starrocks.sql.optimizer.statistics.hbo.RecentRunsPlanStatisticsEntry;
import com.starrocks.sql.optimizer.statistics.hbo.ScanPlanStatistics;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class HboUtilsTest {

    @Mocked
    private HboPlanStatisticsManager hboPlanStatisticsManager;

    @Mocked
    private HboPlanInfoProvider hboPlanInfoProvider;

    private OptimizerContext optimizerContext;
    private ConnectContext connectContext;
    private ColumnRefFactory columnRefFactory;
    private OlapTable olapTable;
    private PhysicalOlapScanOperator physicalOlapScanOperator;
    private LogicalOlapScanOperator logicalOlapScanOperator;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        UtFrameUtils.addMockBackend(10004);
    }

    @Before
    public void setUp() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(null, columnRefFactory, connectContext);

        setupTestTables();
        setupMocks();
    }

    private void setupTestTables() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", Type.INT));
        columns.add(new Column("name", Type.STRING));

        SinglePartitionInfo partitionInfo = new SinglePartitionInfo();
        DistributionInfo distributionInfo = new HashDistributionInfo(32, Lists.newArrayList(columns.get(0)));

        olapTable = new OlapTable(1L, "test_table", columns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);

        physicalOlapScanOperator = new PhysicalOlapScanOperator(olapTable, Maps.newHashMap(),
                null, -1, null, -1, null, null,
                new ArrayList<>(), null, null, false);

        logicalOlapScanOperator = new LogicalOlapScanOperator(olapTable);
    }

    private void setupMocks() {
        hboPlanStatisticsManager = GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager();
        hboPlanInfoProvider = GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager().getHboPlanInfoProvider();
    }

    @Test
    public void testGetAccurateStatsIndex() {
        List<PlanStatistics> inputTableStatistics = createTestInputTableStatistics();
        RecentRunsPlanStatistics recentRunsPlanStatistics = createTestRecentRunsPlanStatistics();

        Optional<Integer> result = HboUtils.getAccurateStatsIndex(
                recentRunsPlanStatistics, inputTableStatistics, 0.5,
                false, PlanStatisticsMatchStrategy.FULL_MATCH);

        Assert.assertTrue("Should find accurate stats index", result.isPresent());
        Assert.assertEquals("Should return first matching index", Integer.valueOf(0), result.get());

        RecentRunsPlanStatistics emptyStats = new RecentRunsPlanStatistics(new ArrayList<>());
        Optional<Integer> emptyResult = HboUtils.getAccurateStatsIndex(
                emptyStats, inputTableStatistics, 0.5,
                false, PlanStatisticsMatchStrategy.FULL_MATCH);

        Assert.assertFalse("Should not find index in empty stats", emptyResult.isPresent());
    }

    @Test
    public void testCanAccurateMatchForNonPartitionTable() {
        ScanPlanStatistics currentStats = createTestScanPlanStatistics(1, false);
        ScanPlanStatistics recentStats = createTestScanPlanStatistics(1, false);

        boolean result = HboUtils.canAccurateMatchForNonPartitionTable(
                currentStats, recentStats, PlanStatisticsMatchStrategy.OTHER_ONLY_MATCH);
        Assert.assertTrue("Should match for OTHER_ONLY_MATCH strategy", result);

        result = HboUtils.canAccurateMatchForNonPartitionTable(
                currentStats, recentStats, PlanStatisticsMatchStrategy.FULL_MATCH);
        Assert.assertTrue("Should match for FULL_MATCH strategy", result);

        result = HboUtils.canAccurateMatchForNonPartitionTable(
                currentStats, recentStats, PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH);
        Assert.assertFalse("Should not match for unsupported strategy", result);
    }

    @Test
    public void testCanAccurateMatchForPartitionTable() {
        ScanPlanStatistics currentStats = createTestScanPlanStatistics(1, true);
        ScanPlanStatistics recentStats = createTestScanPlanStatistics(1, true);

        boolean result = HboUtils.canAccurateMatchForPartitionTable(
                currentStats, recentStats, false, PlanStatisticsMatchStrategy.FULL_MATCH);
        Assert.assertTrue("Should match for FULL_MATCH strategy", result);

        result = HboUtils.canAccurateMatchForPartitionTable(
                currentStats, recentStats, false, PlanStatisticsMatchStrategy.PARTITION_AND_OTHER_MATCH);
        Assert.assertTrue("Should match for PARTITION_AND_OTHER_MATCH strategy", result);

        result = HboUtils.canAccurateMatchForPartitionTable(
                currentStats, recentStats, true, PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH);
        Assert.assertTrue("Should match for PARTITION_ONLY_MATCH with non-strict mode", result);

        result = HboUtils.canAccurateMatchForPartitionTable(
                currentStats, recentStats, false, PlanStatisticsMatchStrategy.PARTITION_ONLY_MATCH);
        Assert.assertFalse("Should not match for PARTITION_ONLY_MATCH without non-strict mode", result);
    }

    @Test
    public void testGetSimilarStatsIndex() {
        List<PlanStatistics> inputTableStatistics = createTestInputTableStatistics();
        RecentRunsPlanStatistics recentRunsPlanStatistics = createTestRecentRunsPlanStatistics();

        Optional<Integer> result = HboUtils.getSimilarStatsIndex(
                recentRunsPlanStatistics, inputTableStatistics, 0.1, 0.5);

        Assert.assertTrue("Should find similar stats index", result.isPresent());
        Assert.assertEquals("Should return first matching index", Integer.valueOf(0), result.get());

        RecentRunsPlanStatistics emptyStats = new RecentRunsPlanStatistics(new ArrayList<>());
        Optional<Integer> emptyResult = HboUtils.getSimilarStatsIndex(
                emptyStats, inputTableStatistics, 0.1, 0.5);

        Assert.assertFalse("Should not find index in empty stats", emptyResult.isPresent());
    }

    @Test
    public void testSimilarStats() {
        Assert.assertTrue("Should be similar", HboUtils.similarStats(100.0, 105.0, 0.1));
        Assert.assertTrue("Should be similar", HboUtils.similarStats(100.0, 95.0, 0.1));

        Assert.assertFalse("Should not be similar", HboUtils.similarStats(100.0, 120.0, 0.1));
        Assert.assertFalse("Should not be similar", HboUtils.similarStats(100.0, 80.0, 0.1));

        Assert.assertTrue("NaN values should be similar", HboUtils.similarStats(Double.NaN, Double.NaN, 0.1));
        Assert.assertFalse("NaN and normal value should not be similar", HboUtils.similarStats(Double.NaN, 100.0, 0.1));
    }

    @Test
    public void testGetPlanFingerprintHash() {
        String planFingerprint = "test_plan_fingerprint";
        String hash = HboUtils.getPlanFingerprintHash(planFingerprint);

        Assert.assertNotNull("Hash should not be null", hash);
        Assert.assertFalse("Hash should not be empty", hash.isEmpty());

        String hash2 = HboUtils.getPlanFingerprintHash(planFingerprint);
        Assert.assertEquals("Same input should produce same hash", hash, hash2);

        String differentHash = HboUtils.getPlanFingerprintHash("different_fingerprint");
        Assert.assertNotEquals("Different input should produce different hash", hash, differentHash);
    }

    @Test
    public void testGetMatchedHboPlanStatisticsEntry() {
        RecentRunsPlanStatistics recentStats = createTestRecentRunsPlanStatistics();
        List<PlanStatistics> inputTableStatistics = createTestInputTableStatistics();

        Optional<RecentRunsPlanStatisticsEntry> result = HboUtils.getMatchedHboPlanStatisticsEntry(
                recentStats, inputTableStatistics, 0.5, false);

        Assert.assertTrue("Should find matched entry", result.isPresent());
        Assert.assertNotNull("Matched entry should not be null", result.get());

        RecentRunsPlanStatistics emptyStats = new RecentRunsPlanStatistics(new ArrayList<>());
        Optional<RecentRunsPlanStatisticsEntry> emptyResult = HboUtils.getMatchedHboPlanStatisticsEntry(
                emptyStats, inputTableStatistics, 0.5, false);

        Assert.assertFalse("Should not find entry in empty stats", emptyResult.isPresent());
    }

    @Test
    public void testGetOperatorHashWithOptExpression() {
        OptExpression optExpression = createTestOptExpression();

        PlanNodeAndHash result = HboUtils.getOperatorHash(optExpression);

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertNotNull("Operator should not be null", result.getPlanNode());
        Assert.assertTrue("Hash should be present", result.getHash().isPresent());
        Assert.assertFalse("Hash should not be empty", result.getHash().get().isEmpty());
    }

    @Test
    public void testGetOperatorHashWithGroupExpression() {
        GroupExpression groupExpression = createTestGroupExpression();

        PlanNodeAndHash result = HboUtils.getOperatorHash(groupExpression);

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertNotNull("Operator should not be null", result.getPlanNode());
        Assert.assertTrue("Hash should be present", result.getHash().isPresent());
        Assert.assertFalse("Hash should not be empty", result.getHash().get().isEmpty());
    }

    @Test
    public void testGetOperatorHashWithOperator() {
        PlanNodeAndHash result = HboUtils.getOperatorHash(physicalOlapScanOperator);

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertNotNull("Operator should not be null", result.getPlanNode());
        Assert.assertTrue("Hash should be present", result.getHash().isPresent());
        Assert.assertFalse("Hash should not be empty", result.getHash().get().isEmpty());

        PlanNodeAndHash logicalResult = HboUtils.getOperatorHash(logicalOlapScanOperator);
        Assert.assertNotNull("Logical result should not be null", logicalResult);
        Assert.assertTrue("Logical hash should be present", logicalResult.getHash().isPresent());
    }

    @Test
    public void testGetMatchedPlanStatistics() {
        RecentRunsPlanStatistics recentStats = createTestRecentRunsPlanStatistics();

        new Expectations() {
            {
                optimizerContext.getQueryId();
                result = connectContext.getExecutionId();
                minTimes = 0;

                optimizerContext.getSessionVariable();
                result = connectContext.getSessionVariable();
                minTimes = 0;

                connectContext.getSessionVariable().getHboRfSafeThreshold();
                result = 0.5;
                minTimes = 0;

                connectContext.getSessionVariable().isEnableHboNonStrictMatchingMode();
                result = false;
                minTimes = 0;

                hboPlanInfoProvider.getScanToFilterMap(anyString);
                result = new ConcurrentHashMap<String, ScalarOperator>();
                minTimes = 0;
            }
        };

        try {
            PlanStatistics result = HboUtils.getMatchedPlanStatistics(recentStats, optimizerContext);
        } catch (Exception e) {
            Assert.fail("Method should not throw exception: " + e.getMessage());
        }
    }

    @Test
    public void testCollectScanList() {
        OptExpression root = createTestOptExpression();
        List<com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator> scans = new ArrayList<>();

        HboUtils.collectScanList(root, scans);

        Assert.assertFalse("Scans list should not be empty", scans.isEmpty());
        Assert.assertEquals("Should find one scan operator", 1, scans.size());
    }

    @Test
    public void testCollectScanQualifierListWithOptExpression() {
        OptExpression root = createTestOptExpression();
        List<String> scans = new ArrayList<>();

        HboUtils.collectScanQualifierList(root, scans);

        Assert.assertFalse("Scans list should not be empty", scans.isEmpty());
        Assert.assertEquals("Should find one scan qualifier", 1, scans.size());
    }

    @Test
    public void testCollectScanQualifierListWithGroupExpression() {
        GroupExpression groupExpression = createTestGroupExpression();
        List<String> scans = new ArrayList<>();

        HboUtils.collectScanQualifierList(groupExpression, scans);

        Assert.assertFalse("Scans list should not be empty", scans.isEmpty());
        Assert.assertEquals("Should find one scan qualifier", 1, scans.size());
    }

    @Test
    public void testCollectPredicateOnScan() {
        OptExpression optExpression = createTestOptExpressionWithPredicate();

        new MockUp<com.starrocks.qe.GlobalVariable>() {
            @Mock
            public boolean isEnableHboInfoCollection() {
                return true;
            }
        };

        new Expectations() {
            {
                optimizerContext.getQueryId();
                result = connectContext.getExecutionId();
                minTimes = 0;

                hboPlanInfoProvider.getScanToFilterMap(anyString);
                result = new ConcurrentHashMap<String, ScalarOperator>();
                minTimes = 0;

                hboPlanInfoProvider.putScanToFilterMap(anyString, (ConcurrentHashMap<String, ScalarOperator>) any);
                minTimes = 0;
            }
        };

        try {
            HboUtils.collectPredicateOnScan(optExpression, optimizerContext);
        } catch (Exception e) {
            Assert.fail("Method should not throw exception: " + e.getMessage());
        }
    }

    private List<PlanStatistics> createTestInputTableStatistics() {
        List<PlanStatistics> inputStats = new ArrayList<>();
        ScanPlanStatistics scanStats = createTestScanPlanStatistics(1, false);
        inputStats.add(scanStats);
        return inputStats;
    }

    private RecentRunsPlanStatistics createTestRecentRunsPlanStatistics() {
        List<RecentRunsPlanStatisticsEntry> entries = new ArrayList<>();

        PlanStatistics planStats = new PlanStatistics(1, 1000, 900, 800, 700, 600);
        List<PlanStatistics> inputStats = createTestInputTableStatistics();

        RecentRunsPlanStatisticsEntry entry = new RecentRunsPlanStatisticsEntry(planStats, inputStats);
        entries.add(entry);

        return new RecentRunsPlanStatistics(entries);
    }

    private ScanPlanStatistics createTestScanPlanStatistics(int nodeId, boolean isPartitioned) {
        PlanStatistics basePlanStats = new PlanStatistics(nodeId, 1000, 900, 800, 700, 600);

        return new ScanPlanStatistics(basePlanStats, physicalOlapScanOperator, null,
                isPartitioned, new ArrayList<>(), new ArrayList<>()) {
            @Override
            public boolean isRuntimeFilterSafeNode(double threshold) {
                return true;
            }

            @Override
            public boolean hasSameOtherPredicates(ScanPlanStatistics other) {
                return true;
            }

            @Override
            public boolean hasSamePartitionId(ScanPlanStatistics other) {
                return true;
            }

            @Override
            public boolean hasSamePartitionColumnPredicates(ScanPlanStatistics other) {
                return true;
            }
        };
    }

    private OptExpression createTestOptExpression() {
        return OptExpression.create(physicalOlapScanOperator);
    }

    private OptExpression createTestOptExpressionWithPredicate() {
        LogicalOlapScanOperator scanWithPredicate = new LogicalOlapScanOperator(olapTable);

        return OptExpression.create(scanWithPredicate);
    }

    private GroupExpression createTestGroupExpression() {
        Group group = new Group(1);
        GroupExpression groupExpression = new GroupExpression(logicalOlapScanOperator, Lists.newArrayList());
        groupExpression.setGroup(group);
        return groupExpression;
    }
}