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

package com.starrocks.datacache;

import com.starrocks.analysis.TableName;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.CreateDataCacheJobStmt;
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.ShowDataCacheStmt;
import com.starrocks.sql.ast.ShowDataCacheTableStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Integration tests for DataCache SHOW statements and Job scheduling.
 *
 * NOTE: These tests require enable_oteam_datacache = true to be set.
 */
public class DataCacheShowAndJobIntegrationTest extends PlanTestBase {

    private static DataCacheMetaManager metaManager;
    private static boolean originalEnableOteamDatacache;

    @BeforeClass
    public static void beforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(PlanTestBase.connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(PlanTestBase.connectContext);

        // Initialize DataCacheMetaManager
        metaManager = new DataCacheMetaManager(60_000L, 1L, 1L);

        // Save original config and enable oteam datacache for tests
        originalEnableOteamDatacache = Config.enable_oteam_datacache;
        Config.enable_oteam_datacache = true;

        // Mock GlobalStateMgr to return our test metaManager
        new MockUp<GlobalStateMgr>() {
            @Mock
            public DataCacheMetaManager getDataCacheMetaManager() {
                return metaManager;
            }
        };

        // Setup test data in metaManager
        setupTestData();
    }

    @AfterClass
    public static void afterAll() {
        // Restore original config
        Config.enable_oteam_datacache = originalEnableOteamDatacache;
    }

    private static void setupTestData() {
        LocalDateTime now = LocalDateTime.now();

        // Create table metadata for multi_partition_table
        TableName multiPartitionTable = new TableName("hive0", "datacache_db", "multi_partition_table");
        metaManager.upsertTableMeta(multiPartitionTable);

        // Create partition metadata
        metaManager.upsertPartitionMeta(
                multiPartitionTable, "l_shipdate=1998-01-01", 1L, "CACHED", now, null,
                "l_shipdate", "DATE", 1024L * 1024 * 100,
                "/data/hive/multi_partition_table/l_shipdate=1998-01-01",
                "DAY", "yyyy-MM-dd", "test-signature-1"
        );

        // Create table metadata for normal_table (full table cache)
        TableName normalTable = new TableName("hive0", "datacache_db", "normal_table");
        metaManager.upsertTableMeta(normalTable);

        metaManager.upsertPartitionMeta(
                normalTable, DataCacheSelectStatement.PARTITION_FULL_TABLE, 1L, "CACHED", now, null,
                "", "", 1024L * 1024 * 10,
                "/data/hive/normal_table",
                "", "", "test-signature-full"
        );
    }

    // ========================================
    // A. SHOW DATA CACHE Statement Tests
    // ========================================

    @Test
    public void testShowDataCacheStatementParsing() throws Exception {
        String sql = "SHOW DATA CACHE FROM hive0.datacache_db.multi_partition_table";

        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof ShowDataCacheStmt);

        ShowDataCacheStmt showStmt = (ShowDataCacheStmt) stmt;
        TableName tableName = showStmt.getTableName();
        Assert.assertEquals("hive0", tableName.getCatalog());
        Assert.assertEquals("datacache_db", tableName.getDb());
        Assert.assertEquals("multi_partition_table", tableName.getTbl());
    }

    @Test
    public void testShowDataCacheReturnsData() {
        TableName tableName = new TableName("hive0", "datacache_db", "multi_partition_table");

        List<List<String>> rows = metaManager.getPartitionsDataCacheSize(tableName);

        Assert.assertNotNull(rows);
        Assert.assertFalse(rows.isEmpty());
    }

    @Test
    public void testShowDataCacheForNonExistentTable() {
        TableName tableName = new TableName("hive0", "datacache_db", "non_existent_table");

        List<List<String>> rows = metaManager.getPartitionsDataCacheSize(tableName);

        Assert.assertNotNull(rows);
        Assert.assertTrue("Should return empty result for non-existent table", rows.isEmpty());
    }

    // ========================================
    // B. SHOW DATA CACHE TABLES Statement Tests
    // ========================================

    @Test
    public void testShowDataCacheTablesStatementParsing() throws Exception {
        // Note: SHOW DATA CACHE TABLES has no FROM clause
        String sql = "SHOW DATA CACHE TABLES";

        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof ShowDataCacheTableStmt);
    }

    // ========================================
    // C. CREATE DATA CACHE JOB Statement Tests
    // ========================================

    @Test
    public void testCreateDataCacheJobStatementParsing() throws Exception {
        String sql = "CREATE DATA CACHE JOB test_job " +
                     "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                     "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D', 'verbose'='true')";

        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof CreateDataCacheJobStmt);

        CreateDataCacheJobStmt jobStmt = (CreateDataCacheJobStmt) stmt;
        Assert.assertEquals("test_job", jobStmt.getTaskName());
        Assert.assertNotNull(jobStmt.getSchedule());
    }

    @Test
    public void testCreateDataCacheJobWithCachePartitionNum() throws Exception {
        String sql = "CREATE DATA CACHE JOB test_job_multi " +
                     "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                     "PROPERTIES('cache_partition_num'='3') " +
                     "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D', 'verbose'='true')";

        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        CreateDataCacheJobStmt jobStmt = (CreateDataCacheJobStmt) stmt;

        Assert.assertEquals(3, jobStmt.getCachePartitionNum());
    }

    @Test
    public void testCreateDataCacheJobPartitionSetManuallyRejected() {
        String sql = "CREATE DATA CACHE JOB test_job_reject " +
                     "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                     "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='1998-01-01', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D', 'verbose'='true')";

        AnalyzeTestUtil.analyzeFail(sql, "should not set manually");
    }

    @Test
    public void testCreateDataCacheJobCachePartitionNumExceedsTTL() {
        String sql = "CREATE DATA CACHE JOB test_job_exceed " +
                     "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                     "PROPERTIES('cache_partition_num'='10') " +
                     "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', " +
                     "'ttl'='P7D', 'verbose'='true')";

        AnalyzeTestUtil.analyzeFail(sql, "cache_partition_num * partition_unit need less than ttl");
    }

    @Test
    public void testCreateDataCacheJobWithoutOteamDatacache() {
        // Temporarily disable oteam datacache
        boolean original = Config.enable_oteam_datacache;
        Config.enable_oteam_datacache = false;

        try {
            String sql = "CREATE DATA CACHE JOB test_job_disabled " +
                         "SCHEDULE EVERY(INTERVAL 1 DAY) " +
                         "AS CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                         "PROPERTIES('partition_field'='l_shipdate', " +
                         "'partition_field_type'='date', 'partition_unit'='day', " +
                         "'ttl'='P7D', 'verbose'='true')";

            AnalyzeTestUtil.analyzeFail(sql, "only oteam data cache implementation support CREATE DATA CACHE JOB");
        } finally {
            Config.enable_oteam_datacache = original;
        }
    }

    // ========================================
    // D. DataCacheJobMgr Utility Tests
    // ========================================

    @Test
    public void testComputeKthPreviousPartitionDay() {
        LocalDateTime baseTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);

        // Note: partition values have "p" prefix
        String partition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "day");
        Assert.assertEquals("p20240115", partition0);

        String partition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "day");
        Assert.assertEquals("p20240114", partition1);
    }

    @Test
    public void testComputeKthPreviousPartitionHour() {
        LocalDateTime baseTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);

        // Note: partition values have "p" prefix
        String partition0 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 0, "hour");
        Assert.assertEquals("p2024011510", partition0);

        String partition1 = DataCacheJobMgr.computeKthPreviousPartition(baseTime, 1, "hour");
        Assert.assertEquals("p2024011509", partition1);
    }
}
