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

import com.google.common.collect.Lists;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.HdfsScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.parser.AstBuilder;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class StmtExecutorTest {

    @Test
    public void testIsForwardToLeader(@Mocked GlobalStateMgr state) {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.isInTransferringToLeader();
                times = 1;
                result = true;

                state.getSqlParser();
                result = new SqlParser(AstBuilder.getInstance());

                state.isLeader();
                times = 2;
                result = false;
                result = true;
            }
        };

        Assert.assertFalse(new StmtExecutor(new ConnectContext(),
                SqlParser.parseSingleStatement("show frontends", SqlModeHelper.MODE_DEFAULT)).isForwardToLeader());
    }

    @Test
    public void testDatacacheMetricsInScanDetail(@Mocked GlobalStateMgr state,
                                                  @Mocked HdfsScanNode hdfsScanNode,
                                                  @Mocked com.starrocks.catalog.HiveTable hiveTable,
                                                  @Mocked com.starrocks.connector.RemoteScanRangeLocations
                                                  scanRangeLocations) throws Exception {
        new Expectations() {
            {
                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = state;

                state.getSqlParser();
                result = new SqlParser(AstBuilder.getInstance());
            }
        };

        ConnectContext connectContext = new ConnectContext();
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext,
                SqlParser.parseSingleStatement("SELECT 1", SqlModeHelper.MODE_DEFAULT));

        // Create PQueryStatistics with datacache metrics
        PQueryStatistics statistics = new PQueryStatistics();
        statistics.setScanRows(1000L);
        statistics.setScanBytes(10000L);
        statistics.setHdfsScanBytes(6000L);
        statistics.setDatacacheScanBytes(4000L);

        // Create table-level statistics
        List<QueryStatisticsItemPB> statsItems = Lists.newArrayList();
        QueryStatisticsItemPB item1 = new QueryStatisticsItemPB();
        item1.setTableId(100L);
        item1.setScanRows(500L);
        item1.setScanBytes(5000L);
        item1.setHdfsScanBytes(3000L);
        item1.setDatacacheScanBytes(2000L);
        statsItems.add(item1);

        QueryStatisticsItemPB item2 = new QueryStatisticsItemPB();
        item2.setTableId(200L);
        item2.setScanRows(500L);
        item2.setScanBytes(5000L);
        item2.setHdfsScanBytes(3000L);
        item2.setDatacacheScanBytes(2000L);
        statsItems.add(item2);

        statistics.setStatsItems(statsItems);
        stmtExecutor.setQueryStatistics(statistics);

        // Create ExecPlan with mocked HdfsScanNode
        ExecPlan execPlan = new ExecPlan();
        List<ScanNode> scanNodes = Lists.newArrayList();

        new Expectations(hdfsScanNode) {
            {
                hdfsScanNode.getId();
                result = new PlanNodeId(1);
                minTimes = 0;

                hdfsScanNode.getHiveTable();
                result = hiveTable;
                minTimes = 0;

                hiveTable.getId();
                result = 100L;
                minTimes = 0;

                hiveTable.getDbName();
                result = "test_db";
                minTimes = 0;

                hiveTable.getTableName();
                result = "test_table";
                minTimes = 0;

                hdfsScanNode.getScanRangeLocations();
                result = scanRangeLocations;
                minTimes = 0;

                scanRangeLocations.getPartitionNum();
                result = 5;
                minTimes = 0;

                scanRangeLocations.getFileSizeBytes();
                result = 1024L * 1024L * 1024L; // 1GB
                minTimes = 0;

                scanRangeLocations.getFileNum();
                result = 10;
                minTimes = 0;
            }
        };

        scanNodes.add(hdfsScanNode);
        execPlan.getScanNodes().addAll(scanNodes);
        execPlan.setIsScanAllPartitions(false);
        execPlan.setIsPartitionPruningSuccess(true);

        // Use reflection to call private method recordDetailInfoInProfile
        Method method = StmtExecutor.class.getDeclaredMethod("recordDetailInfoInProfile", ExecPlan.class);
        method.setAccessible(true);
        method.invoke(stmtExecutor, execPlan);

        // Verify that scanDetail was set in audit event builder
        String scanDetailJson = connectContext.getAuditEventBuilder().build().scanDetail;
        Assert.assertNotNull("ScanDetail should not be null", scanDetailJson);

        // Parse and verify the JSON content
        Map<String, Object> scanDetailMap = GsonUtils.GSON.fromJson(scanDetailJson, Map.class);
        Assert.assertNotNull("ScanDetailMap should not be null", scanDetailMap);

        // Verify total statistics
        Assert.assertEquals("ScanRows should match", 1000L, ((Number) scanDetailMap.get("ScanRows")).longValue());
        Assert.assertEquals("ScanBytes should match", 10000L, ((Number) scanDetailMap.get("ScanBytes")).longValue());
        Assert.assertEquals("HDFSTotalScanBytes should match", 6000L,
                ((Number) scanDetailMap.get("HDFSTotalScanBytes")).longValue());
        Assert.assertEquals("DataCacheTotalScanBytes should match", 4000L,
                ((Number) scanDetailMap.get("DataCacheTotalScanBytes")).longValue());

        // Verify cache hit rate calculation: 4000 / (6000 + 4000) = 0.40
        String hitRate = (String) scanDetailMap.get("DataCacheTotalHitRate");
        Assert.assertNotNull("DataCacheTotalHitRate should not be null", hitRate);
        Assert.assertEquals("DataCacheTotalHitRate should be 0.40", "0.40", hitRate);

        // Verify table-level statistics
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> tableDetails = (List<Map<String, Object>>) scanDetailMap.get("TableDetails");
        Assert.assertNotNull("TableDetails should not be null", tableDetails);
        Assert.assertEquals("TableDetails should have 1 item", 1, tableDetails.size());

        Map<String, Object> tableDetail = tableDetails.get(0);
        Assert.assertEquals("TableId should match", 100L, ((Number) tableDetail.get("TableId")).longValue());
        Assert.assertEquals("HDFSScanBytes should match", 3000L,
                ((Number) tableDetail.get("HDFSScanBytes")).longValue());
        Assert.assertEquals("DataCacheScanBytes should match", 2000L,
                ((Number) tableDetail.get("DataCacheScanBytes")).longValue());

        // Verify table-level cache hit rate: 2000 / (3000 + 2000) = 0.40
        String tableHitRate = (String) tableDetail.get("DataCacheHitRate");
        Assert.assertNotNull("DataCacheHitRate should not be null", tableHitRate);
        Assert.assertEquals("DataCacheHitRate should be 0.40", "0.40", tableHitRate);
    }
}
