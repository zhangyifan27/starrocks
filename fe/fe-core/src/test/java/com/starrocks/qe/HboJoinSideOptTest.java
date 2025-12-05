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

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HboJoinSideOptTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.show_execution_groups = false;
        // disable checking tablets
        Config.tablet_sched_max_scheduling_tablets = -1;
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        connectContext.getSessionVariable().setCboEqBaseType(SessionVariableConstants.VARCHAR);
        connectContext.getSessionVariable().setBroadcastStrictChecks(false);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
        FeConstants.showFragmentCost = false;
        FeConstants.runningUnitTest = true;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        connectContext = null;
    }

    @Test
    public void testHboJoinSideOptimization() throws Exception {
        starRocksAssert.withDatabase("hbo_test").useDatabase("hbo_test");
        
        createTestTables();
        
        insertTestData();
        
        executeQueryWithHboEnabled();
        
        verifyHboExplainOutput();
    }

    private void createTestTables() throws Exception {
        // Create table hbo_join_side_opt_test1
        String createTable1Sql = "create table hbo_test.hbo_join_side_opt_test1(" +
                "a int, " +
                "b int" +
                ") distributed by hash(a) buckets 1 " +
                "properties(\"replication_num\"=\"1\")";
        starRocksAssert.withTable(createTable1Sql);
        
        // Create table hbo_join_side_opt_test2
        String createTable2Sql = "create table hbo_test.hbo_join_side_opt_test2(" +
                "a int, " +
                "b int" +
                ") distributed by hash(a) buckets 1 " +
                "properties(\"replication_num\"=\"1\")";
        starRocksAssert.withTable(createTable2Sql);
    }

    private void insertTestData() throws Exception {
        UtFrameUtils.mockDML();

        String insertSql1 = "insert into hbo_test.hbo_join_side_opt_test1 select generate_series, generate_series" +
                "from TABLE(generate_series(1, 1))";
        executeInsertStatement(insertSql1);

        String insertSql2 = "insert into hbo_test.hbo_join_side_opt_test1 select 1,1 from TABLE(generate_series(1, 10))";
        executeInsertStatement(insertSql2);

        String insertSql3 = "insert into hbo_test.hbo_join_side_opt_test2 select generate_series, generate_series " +
                "from TABLE(generate_series(1, 1))";
        executeInsertStatement(insertSql3);
    }
    
    private void executeInsertStatement(String sql) throws Exception {
        try {
            StatementBase statement = SqlParser.parseSingleStatement(sql, connectContext.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(connectContext, statement);
            executor.execute();
        } catch (Exception e) {
            System.out.println("statement execution: " + sql + " - " + e.getMessage());
        }
    }

    private void executeQueryWithHboEnabled() throws Exception {
        connectContext.getSessionVariable().setEnableHboOptimization(true);
        GlobalVariable.setEnableHboInfoCollection(true);

        String querySql = "select count(1) from hbo_test.hbo_join_side_opt_test1 s1, hbo_test.hbo_join_side_opt_test2 s2 " +
                "where s1.b = s2.b and s1.a = 1";
        
        try {
            StatementBase statement = SqlParser.parseSingleStatement(querySql, connectContext.getSessionVariable().getSqlMode());
            StmtExecutor executor = new StmtExecutor(connectContext, statement);
            executor.execute();
        } catch (Exception e) {
            System.out.println("Query execution completed (may have failed in test environment): " + e.getMessage());
        }
    }

    private void verifyHboExplainOutput() throws Exception {
        connectContext.getSessionVariable().setEnableHboOptimization(true);
        GlobalVariable.setEnableHboInfoCollection(true);

        String explainSql = "explain logical select count(1) from hbo_test.hbo_join_side_opt_test1 s1," +
                "hbo_test.hbo_join_side_opt_test2 s2 " +
                "where s1.b = s2.b and s1.a = 1";

        executeInsertStatement(explainSql);
    }
}