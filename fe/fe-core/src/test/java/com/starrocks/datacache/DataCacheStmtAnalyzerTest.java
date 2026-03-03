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
import com.starrocks.sql.ast.DataCacheSelectStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TCacheSelectMode;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDateTime;

/**
 * Tests for DataCacheStmtAnalyzer.
 *
 * These tests verify SQL statement parsing and analysis:
 * - CACHE SELECT/DELETE/DESC statement parsing
 * - Partition value parsing for different formats (year, month, day, hour)
 * - Partition unit validation and type checking
 * - TTL/Priority validation
 * - Error handling for invalid inputs
 */
public class DataCacheStmtAnalyzerTest extends PlanTestBase {

    private static DataCacheMetaManager metaManager;
    private static boolean originalEnableOteamDatacache;

    @BeforeClass
    public static void beforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        PlanTestBase.beforeClass();
        AnalyzeTestUtil.setConnectContext(PlanTestBase.connectContext);
        ConnectorPlanTestBase.mockHiveCatalog(PlanTestBase.connectContext);

        metaManager = new DataCacheMetaManager(60_000L, 1L, 1L);
        originalEnableOteamDatacache = Config.enable_oteam_datacache;
        Config.enable_oteam_datacache = true;

        new MockUp<GlobalStateMgr>() {
            @Mock
            public DataCacheMetaManager getDataCacheMetaManager() {
                return metaManager;
            }
        };

        setupTestData();
    }

    @AfterClass
    public static void afterAll() {
        Config.enable_oteam_datacache = originalEnableOteamDatacache;
    }

    private static void setupTestData() {
        LocalDateTime now = LocalDateTime.now();

        // Tables for partition unit tests
        TableName dayTable = new TableName("hive0", "datacache_db", "day_partition_table");
        metaManager.upsertTableMeta(dayTable);
        metaManager.upsertPartitionMeta(dayTable, "p20240115", 1L, "CACHED", now, null,
                "dt", "DATE", 1024L * 1024, "/data/hive/day_partition_table/dt=2024-01-15",
                "DAY", "yyyy-MM-dd", "sig-day");

        TableName hourTable = new TableName("hive0", "datacache_db", "hour_partition_table");
        metaManager.upsertTableMeta(hourTable);
        metaManager.upsertPartitionMeta(hourTable, "p2024011510", 1L, "CACHED", now, null,
                "dt", "DATE", 1024L * 1024, "/data/hive/hour_partition_table/dt=2024-01-15-10",
                "HOUR", "yyyy-MM-dd-HH", "sig-hour");

        TableName monthTable = new TableName("hive0", "datacache_db", "month_partition_table");
        metaManager.upsertTableMeta(monthTable);
        metaManager.upsertPartitionMeta(monthTable, "p202401", 1L, "CACHED", now, null,
                "dt", "DATE", 1024L * 1024, "/data/hive/month_partition_table/dt=2024-01",
                "MONTH", "yyyy-MM", "sig-month");

        TableName yearTable = new TableName("hive0", "datacache_db", "year_partition_table");
        metaManager.upsertTableMeta(yearTable);
        metaManager.upsertPartitionMeta(yearTable, "p2024", 1L, "CACHED", now, null,
                "dt", "INT", 1024L * 1024, "/data/hive/year_partition_table/dt=2024",
                "YEAR", "", "sig-year");

        TableName stringTable = new TableName("hive0", "datacache_db", "string_partition_table");
        metaManager.upsertTableMeta(stringTable);
        metaManager.upsertPartitionMeta(stringTable, "p20240115", 1L, "CACHED", now, null,
                "dt", "STRING", 1024L * 1024, "/data/hive/string_partition_table/dt=2024-01-15",
                "DAY", "yyyy-MM-dd", "sig-string");

        TableName datetimeTable = new TableName("hive0", "datacache_db", "datetime_partition_table");
        metaManager.upsertTableMeta(datetimeTable);
        metaManager.upsertPartitionMeta(datetimeTable, "p2024011510", 1L, "CACHED", now, null,
                "dt", "DATETIME", 1024L * 1024, "/data/hive/datetime_partition_table/dt=2024-01-15 10:00:00",
                "HOUR", "yyyy-MM-dd HH:mm:ss", "sig-datetime");

        // Tables from original SqlIntegrationTest
        TableName multiPartitionTable = new TableName("hive0", "datacache_db", "multi_partition_table");
        metaManager.upsertTableMeta(multiPartitionTable);
        metaManager.upsertPartitionMeta(multiPartitionTable, "p19980101", 1L, "CACHED", now, null,
                "l_shipdate", "DATE", 1024L * 1024, "/data/hive/multi_partition_table/l_shipdate=1998-01-01",
                "DAY", "yyyy-MM-dd", "test-signature-1");

        TableName singlePartitionTable = new TableName("hive0", "datacache_db", "single_partition_table");
        metaManager.upsertTableMeta(singlePartitionTable);
        metaManager.upsertPartitionMeta(singlePartitionTable, "p19980101", 1L, "CACHED", now, null,
                "l_shipdate", "STRING", 1024L * 1024, "/data/hive/single_partition_table/l_shipdate=1998-01-01",
                "DAY", "yyyy-MM-dd", "test-signature-s1");

        TableName normalTable = new TableName("hive0", "datacache_db", "normal_table");
        metaManager.upsertTableMeta(normalTable);
        metaManager.upsertPartitionMeta(normalTable, DataCacheSelectStatement.PARTITION_FULL_TABLE, 1L, "CACHED", now, null,
                "", "", 512L * 1024, "/data/hive/normal_table", "", "", "test-signature-full");
    }

    // ========================================
    // A. CACHE SELECT Statement Parsing Tests
    // ========================================

    @Test
    public void testCacheSelectStatementParsing() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";

        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertTrue(stmt instanceof DataCacheSelectStatement);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertTrue(cacheStmt.isCacheSelect());
        Assert.assertEquals("hive0", cacheStmt.getCatalog());
    }

    @Test
    public void testCacheSelectFullTable() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.normal_table PROPERTIES('full_table_cache'='true')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertTrue(cacheStmt.isFullTableCache());
    }

    // ========================================
    // B. CACHE DELETE Statement Parsing Tests
    // ========================================

    @Test
    public void testCacheDeleteStatementParsing() throws Exception {
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table PROPERTIES('partition'='p19980101')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertTrue(cacheStmt.isCacheDelete());
        Assert.assertEquals(TCacheSelectMode.DELETE, cacheStmt.mode());
    }

    @Test
    public void testCacheDeleteWithGcMode() throws Exception {
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'cache_delete_mode'='gc')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertTrue(cacheStmt.isDeleteModeGc());
    }

    // ========================================
    // C. CACHE DESC Statement Parsing Tests
    // ========================================

    @Test
    public void testCacheDescStatementParsing() throws Exception {
        String sql = "CACHE DESC * FROM hive0.datacache_db.multi_partition_table PROPERTIES('partition'='p19980101')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertTrue(cacheStmt.isCacheDesc());
        Assert.assertEquals(TCacheSelectMode.DESC, cacheStmt.mode());
    }

    @Test
    public void testCacheDescInheritsPartitionProperties() throws Exception {
        String sql = "CACHE DESC * FROM hive0.datacache_db.multi_partition_table PROPERTIES('partition'='p19980101')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("l_shipdate", cacheStmt.getPartitionField());
        Assert.assertEquals("DAY", cacheStmt.getPartitionUnit().toUpperCase());
    }

    // ========================================
    // D. Partition Unit Tests (day/hour/month/year)
    // ========================================

    @Test
    public void testCacheSelectDayPartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.day_partition_table " +
                     "PROPERTIES('partition'='p20240115', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("p20240115", cacheStmt.getPartition());
        Assert.assertEquals("day", cacheStmt.getPartitionUnit());
    }

    @Test
    public void testCacheSelectHourPartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.hour_partition_table " +
                     "PROPERTIES('partition'='p2024011510', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='hour')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("hour", cacheStmt.getPartitionUnit());
    }

    @Test
    public void testCacheSelectMonthPartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.month_partition_table " +
                     "PROPERTIES('partition'='p202401', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='month')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("month", cacheStmt.getPartitionUnit());
    }

    @Test
    public void testCacheSelectYearPartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.year_partition_table " +
                     "PROPERTIES('partition'='p2024', 'partition_field'='dt', " +
                     "'partition_field_type'='int', 'partition_unit'='year')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("year", cacheStmt.getPartitionUnit());
    }

    // ========================================
    // E. Partition Field Type Tests
    // ========================================

    @Test
    public void testCacheSelectStringPartitionWithFormat() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.string_partition_table " +
                     "PROPERTIES('partition'='p20240115', 'partition_field'='dt', " +
                     "'partition_field_type'='string', 'partition_unit'='day', 'partition_field_format'='yyyy-MM-dd')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("string", cacheStmt.getPartitionFieldType());
    }

    @Test
    public void testCacheSelectDatetimePartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.datetime_partition_table " +
                     "PROPERTIES('partition'='p2024011510', 'partition_field'='dt', " +
                     "'partition_field_type'='datetime', 'partition_unit'='hour')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("datetime", cacheStmt.getPartitionFieldType());
    }

    @Test
    public void testCacheSelectIntPartition() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.year_partition_table " +
                     "PROPERTIES('partition'='p2024', 'partition_field'='dt', " +
                     "'partition_field_type'='int', 'partition_unit'='year')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals("int", cacheStmt.getPartitionFieldType());
    }

    // ========================================
    // F. TTL and Priority Tests
    // ========================================

    @Test
    public void testCacheSelectWithTTLAndPriority() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', 'ttl'='P7D', 'priority'='1')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals(7 * 24 * 60 * 60, cacheStmt.getTTLSeconds());
        Assert.assertEquals(1, cacheStmt.getPriority());
    }

    @Test
    public void testTTLFormat_Hours() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.day_partition_table " +
                     "PROPERTIES('partition'='p20240115', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='day', 'ttl'='PT24H', 'priority'='1')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        DataCacheSelectStatement cacheStmt = (DataCacheSelectStatement) stmt;
        Assert.assertEquals(24 * 60 * 60, cacheStmt.getTTLSeconds());
    }

    // ========================================
    // G. Error Handling Tests
    // ========================================

    @Test
    public void testCacheSelectInvalidPriority() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', 'priority'='2')";
        AnalyzeTestUtil.analyzeFail(sql, "DataCache's priority can only be set to 0 or 1");
    }

    @Test
    public void testCacheSelectPriorityWithoutTTL() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', 'priority'='1')";
        AnalyzeTestUtil.analyzeFail(sql, "TTL must be specified when priority > 0");
    }

    @Test
    public void testCacheSelectMissingPartitionProperties() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table PROPERTIES('partition'='p19980101')";
        AnalyzeTestUtil.analyzeFail(sql, "CACHE SELECT requires partition_field");
    }

    @Test
    public void testCacheSelectNonStarSelect() {
        String sql = "CACHE SELECT age FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql, "CACHE SELECT/DELETE only supports SELECT *");
    }

    @Test
    public void testCacheDeleteInvalidMode() {
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'cache_delete_mode'='invalid')";
        AnalyzeTestUtil.analyzeFail(sql, "CACHE DELETE cache_delete_mode must be either 'normal' or 'gc'");
    }

    @Test
    public void testCacheDeleteNonExistentPartition() {
        String sql = "CACHE DELETE * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='l_shipdate=1999-01-01')";
        AnalyzeTestUtil.analyzeFail(sql, "not found in partition_cache_meta");
    }

    @Test
    public void testCacheDescNonExistentPartition() {
        String sql = "CACHE DESC * FROM hive0.datacache_db.multi_partition_table PROPERTIES('partition'='p20990101')";
        AnalyzeTestUtil.analyzeFail(sql, "not found in partition_cache_meta");
    }

    @Test
    public void testPartitionUnitMismatch() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980115', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='month')";
        AnalyzeTestUtil.analyzeFail(sql, "partition unit");
    }

    @Test
    public void testStringPartitionFieldMissingFormat() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.single_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='string', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql, "partition_field_format");
    }

    @Test
    public void testTTLInvalidFormat() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day', 'ttl'='invalid', 'priority'='1')";
        AnalyzeTestUtil.analyzeFail(sql, "Illegal ttl format");
    }

    @Test
    public void testCacheSelectWithWhereClauseRejected() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.multi_partition_table " +
                     "WHERE l_shipdate = '1998-01-01' " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql, "does not support explicit WHERE clause");
    }

    @Test
    public void testCacheSelectFullTableWithPartitionPropertiesRejected() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.normal_table " +
                     "PROPERTIES('full_table_cache'='true', 'partition'='p19980101')";
        AnalyzeTestUtil.analyzeFail(sql, "full table cache should not has partition properties");
    }

    @Test
    public void testCacheSelectTableNotExist() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.table_not_exist " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql, "Unknown table");
    }

    @Test
    public void testCacheSelectCatalogNotExist() {
        String sql = "CACHE SELECT * FROM catalog_not_exist.datacache_db.multi_partition_table " +
                     "PROPERTIES('partition'='p19980101', 'partition_field'='l_shipdate', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql);
    }

    @Test
    public void testInvalidPartitionFormat_NoDigits() {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.day_partition_table " +
                     "PROPERTIES('partition'='invalid', 'partition_field'='dt', " +
                     "'partition_field_type'='date', 'partition_unit'='day')";
        AnalyzeTestUtil.analyzeFail(sql);
    }

    // ========================================
    // H. Case Insensitivity Tests
    // ========================================

    @Test
    public void testPartitionUnitCaseInsensitive() throws Exception {
        String sql = "CACHE SELECT * FROM hive0.datacache_db.day_partition_table " +
                     "PROPERTIES('partition'='p20240115', 'partition_field'='dt', " +
                     "'partition_field_type'='DATE', 'partition_unit'='DAY')";
        StatementBase stmt = AnalyzeTestUtil.analyzeSuccess(sql);
        Assert.assertNotNull(stmt);
    }
}
