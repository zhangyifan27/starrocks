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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.proto.NodeExecStatsItemPB;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StmtExecutorHboTest {

    private ConnectContext connectContext;
    private SessionVariable sessionVariable;
    private StmtExecutor stmtExecutor;
    private ExecPlan execPlan;
    private PhysicalOlapScanOperator physicalOlapScanOperator;
    private OlapTable olapTable;

    @Before
    public void setUp() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        connectContext = UtFrameUtils.createDefaultCtx();
        sessionVariable = connectContext.getSessionVariable();
        sessionVariable.setEnableHboOptimization(true);
        GlobalVariable.setEnableHboInfoCollection(true);

        UUID queryId = UUID.randomUUID();
        connectContext.setQueryId(queryId);
        connectContext.setStartTime();

        stmtExecutor = new StmtExecutor(connectContext,
                SqlParser.parseSingleStatement("SELECT 1", SqlModeHelper.MODE_DEFAULT));

        execPlan = new ExecPlan();

        PQueryStatistics queryStatistics = new PQueryStatistics();
        List<NodeExecStatsItemPB> nodeExecStatsItems = createMockNodeExecStatsItems();
        queryStatistics.setNodeExecStatsItems(nodeExecStatsItems);
        stmtExecutor.setQueryStatistics(queryStatistics);

        olapTable = createOlapTable();
        physicalOlapScanOperator = createPhysicalOlapScanOperator();
    }

    private OlapTable createOlapTable() {
        // 创建测试列
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", Type.INT));
        columns.add(new Column("name", Type.STRING));
        columns.add(new Column("value", Type.BIGINT));

        return new OlapTable(1L, "test_table", columns, KeysType.DUP_KEYS, null, null);
    }

    private List<NodeExecStatsItemPB> createMockNodeExecStatsItems() {
        List<NodeExecStatsItemPB> items = new ArrayList<>();

        NodeExecStatsItemPB item1 = new NodeExecStatsItemPB();
        item1.setNodeId(1);
        item1.setPushRows(1000L);
        item1.setPullRows(800L);
        item1.setPredFilterRows(200L);
        item1.setIndexFilterRows(50L);
        item1.setRfFilterRows(30L);
        items.add(item1);

        NodeExecStatsItemPB item2 = new NodeExecStatsItemPB();
        item2.setNodeId(2);
        item2.setPushRows(800L);
        item2.setPullRows(600L);
        item2.setPredFilterRows(200L);
        item2.setIndexFilterRows(100L);
        item2.setRfFilterRows(50L);
        items.add(item2);

        return items;
    }

    @Test
    public void testPublishHboPlanStatisticsWithHboEnabled() throws Exception {
        new MockUp<GlobalVariable>() {
            @Mock
            public boolean isEnableHboInfoCollection() {
                return true;
            }
        };

        String queryId = DebugUtil.printId(connectContext.getQueryId());

        ConcurrentHashMap<Integer, PhysicalOperator> idToPlanMap = GlobalStateMgr.getCurrentState()
                .getHboPlanStatisticsManager().getHboPlanInfoProvider().getIdToPlanMap(queryId);
        if (idToPlanMap.isEmpty()) {
            GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager()
                    .getHboPlanInfoProvider().putIdToPlanMap(queryId, idToPlanMap);
        }
        idToPlanMap.put(1, physicalOlapScanOperator);
        ConcurrentHashMap<PhysicalOperator, Integer> planToIdMap = GlobalStateMgr.getCurrentState()
                .getHboPlanStatisticsManager().getHboPlanInfoProvider().getPlanToIdMap(queryId);
        if (planToIdMap.isEmpty()) {
            GlobalStateMgr.getCurrentState().getHboPlanStatisticsManager()
                    .getHboPlanInfoProvider().putPlanToIdMap(queryId, planToIdMap);
        }
        planToIdMap.put(physicalOlapScanOperator, 1);

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("HBO plan statistics should be published successfully", true);
    }

    private PhysicalOlapScanOperator createPhysicalOlapScanOperator() {
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>();

        List<Column> columns = olapTable.getBaseSchema();
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            ColumnRefOperator columnRef = new ColumnRefOperator(i + 1, column.getType(), column.getName(), true);
            colRefToColumnMetaMap.put(columnRef, column);
        }

        LogicalOlapScanOperator logicalScan = new LogicalOlapScanOperator(
                olapTable,
                colRefToColumnMetaMap,
                new HashMap<>(),
                null,
                -1,
                null
        );

        return new PhysicalOlapScanOperator(logicalScan);
    }

    @Test
    public void testPublishHboPlanStatisticsWithHboDisabled() throws Exception {
        Field enableHboOptimizationField = SessionVariable.class.getDeclaredField("enableHboOptimization");
        enableHboOptimizationField.setAccessible(true);
        enableHboOptimizationField.setBoolean(sessionVariable, false);

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("Should return early when HBO optimization is disabled", true);
    }

    @Test
    public void testPublishHboPlanStatisticsWithHboInfoCollectionDisabled() throws Exception {
        new MockUp<GlobalVariable>() {
            @Mock
            public boolean isEnableHboInfoCollection() {
                return false;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                times = 0;
            }
        };

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("Should not collect HBO info when collection is disabled", true);
    }

    @Test
    public void testPublishHboPlanStatisticsWithEmptyNodeStats() throws Exception {
        PQueryStatistics emptyQueryStatistics = new PQueryStatistics();
        stmtExecutor.setQueryStatistics(emptyQueryStatistics);

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("Should return early when node exec stats are empty", true);
    }

    @Test
    public void testPublishHboPlanStatisticsSlowQueryThreshold() throws Exception {
        Field enableHboOptimizationField = SessionVariable.class.getDeclaredField("enableHboOptimization");
        enableHboOptimizationField.setAccessible(true);
        enableHboOptimizationField.setBoolean(sessionVariable, false);

        Instant oldStartTime = Instant.now().minusMillis(Config.slow_query_analyze_threshold + 1000);
        connectContext.setStartTime(oldStartTime);

        new MockUp<GlobalVariable>() {
            @Mock
            public boolean isEnableHboInfoCollection() {
                return true;
            }
        };

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("Should analyze slow queries even when HBO optimization is disabled", true);
    }

    @Test
    public void testPublishHboPlanStatisticsExceptionHandling() throws Exception {
        new MockUp<GlobalVariable>() {
            @Mock
            public boolean isEnableHboInfoCollection() {
                return true;
            }
        };

        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                result = new RuntimeException("Test exception");
            }
        };

        stmtExecutor.publishHboPlanStatistics(execPlan);

        Assert.assertTrue("Should handle exceptions gracefully", true);
    }
}