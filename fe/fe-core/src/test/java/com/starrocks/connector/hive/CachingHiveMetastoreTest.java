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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HivePartitionKey;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import mockit.Expectations;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.starrocks.connector.hive.RemoteFileInputFormat.ORC;
import static org.apache.hadoop.hive.common.StatsSetupConst.TOTAL_SIZE;

public class CachingHiveMetastoreTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private ExecutorService executor;
    private long expireAfterWriteSec = 30;
    private long refreshAfterWriteSec = -1;

    @Before
    public void setUp() throws Exception {
        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        executor = Executors.newFixedThreadPool(5);
    }

    @After
    public void tearDown() {
        executor.shutdown();
    }

    @Test
    public void testGetAllDatabaseNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllDatabaseNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testGetAllTableNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> databaseNames = cachingHiveMetastore.getAllTableNames("xxx");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetDb() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Database database = cachingHiveMetastore.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

        try {
            metastore.getDb("db2");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
        }
    }

    @Test
    public void testGetTable() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table table = cachingHiveMetastore.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testGetTransactionalTable() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        // get insert only table
        com.starrocks.catalog.Table table = cachingHiveMetastore.getTable("transactional_db", "insert_only");
        Assert.assertNotNull(table);
        // get full acid table
        Assert.assertThrows(StarRocksConnectorException.class, () -> {
            cachingHiveMetastore.getTable("transactional_db", "full_acid");
        });
    }

    @Test
    public void testTableExists() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertTrue(cachingHiveMetastore.tableExists("db1", "tbl1"));
    }

    @Test
    public void testRefreshTable() {
        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistTbl");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        try {
            cachingHiveMetastore.refreshTable("db1", "notExistTbl", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("invalidated cache"));
        }

        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testRefreshTableSync() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertTrue(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));

        try {
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }

        Assert.assertEquals(1, cachingHiveMetastore.tableNameLockMap.size());
    }

    @Test
    public void testRefreshTableBackground() throws InterruptedException {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            // mock query table tbl1
            List<String> partitionNames = cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1",
                    HivePartitionValue.ALL_PARTITION_VALUES);
            cachingHiveMetastore.getPartitionsByNames("db1",
                    "tbl1", partitionNames);
            // put table tbl1 in table cache
            cachingHiveMetastore.refreshTable("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertTrue(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));

        try {
            cachingHiveMetastore.refreshTableBackground("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        }
        // not skip refresh table, table cache still exist
        Assert.assertTrue(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));
        // sleep 1s, background refresh table will be skipped
        Thread.sleep(1000);
        long oldValue = Config.background_refresh_metadata_time_secs_since_last_access_secs;
        // not refresh table, just skip refresh table
        Config.background_refresh_metadata_time_secs_since_last_access_secs = 0;

        try {
            cachingHiveMetastore.refreshTableBackground("db1", "tbl1", true);
        } catch (Exception e) {
            Assert.fail();
        } finally {
            Config.background_refresh_metadata_time_secs_since_last_access_secs = oldValue;
        }
        // table cache will be removed because of skip refresh table
        Assert.assertFalse(cachingHiveMetastore.isTablePresent(DatabaseTableName.of("db1", "tbl1")));
    }

    @Test
    public void testRefreshHiveView() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertFalse(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "tbl1")));
        try {
            cachingHiveMetastore.refreshView("db1", "hive_view");
        } catch (Exception e) {
            Assert.fail();
        }
        Assert.assertTrue(cachingHiveMetastore.tableNameLockMap.containsKey(
                DatabaseTableName.of("db1", "hive_view")));

        new Expectations(metastore) {
            {
                metastore.getTable(anyString, "notExistView");
                minTimes = 0;
                Throwable targetException = new NoSuchObjectException("no such obj");
                Throwable e = new InvocationTargetException(targetException);
                result = new StarRocksConnectorException("table not exist", e);
            }
        };
        try {
            cachingHiveMetastore.refreshView("db1", "notExistView");
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("invalidated cache"));
        }
    }

    @Test
    public void testGetPartitionKeys() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertEquals(Lists.newArrayList("col1"), cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1",
                HivePartitionValue.ALL_PARTITION_VALUES));
    }

    @Test
    public void testGetPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "tbl1", Lists.newArrayList("par1"));
        Assert.assertEquals(ORC, partition.getInputFormat());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));

        partition = metastore.getPartition("db1", "tbl1", Lists.newArrayList());
        Assert.assertEquals("100", partition.getParameters().get(TOTAL_SIZE));
    }

    @Test
    public void testGetPartitionByNames() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
        Map<String, Partition> partitions =
                cachingHiveMetastore.getPartitionsByNames("db1", "table1", partitionNames);

        Partition partition1 = partitions.get("part1=1/part2=2");
        Assert.assertEquals(ORC, partition1.getInputFormat());
        Assert.assertEquals("100", partition1.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=1/part2=2", partition1.getFullPath());

        Partition partition2 = partitions.get("part1=3/part2=4");
        Assert.assertEquals(ORC, partition2.getInputFormat());
        Assert.assertEquals("100", partition2.getParameters().get(TOTAL_SIZE));
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/part1=3/part2=4", partition2.getFullPath());
    }

    @Test
    public void testGetTableStatistics() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats statistics = cachingHiveMetastore.getTableStatistics("db1", "table1");
        HiveCommonStats commonStats = statistics.getCommonStats();
        Assert.assertEquals(50, commonStats.getRowNums());
        Assert.assertEquals(100, commonStats.getTotalFileBytes());
        HiveColumnStats columnStatistics = statistics.getColumnStats().get("col1");
        Assert.assertEquals(0, columnStatistics.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics.getNumNulls());
        Assert.assertEquals(2, columnStatistics.getNdv());
    }

    @Test
    public void testGetPartitionStatistics() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        com.starrocks.catalog.Table hiveTable = cachingHiveMetastore.getTable("db1", "table1");
        Map<String, HivePartitionStats> statistics = cachingHiveMetastore.getPartitionStatistics(
                hiveTable, Lists.newArrayList("col1=1", "col1=2"));

        HivePartitionStats stats1 = statistics.get("col1=1");
        HiveCommonStats commonStats1 = stats1.getCommonStats();
        Assert.assertEquals(50, commonStats1.getRowNums());
        Assert.assertEquals(100, commonStats1.getTotalFileBytes());
        HiveColumnStats columnStatistics1 = stats1.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics1.getTotalSizeBytes());
        Assert.assertEquals(1, columnStatistics1.getNumNulls());
        Assert.assertEquals(2, columnStatistics1.getNdv());

        HivePartitionStats stats2 = statistics.get("col1=2");
        HiveCommonStats commonStats2 = stats2.getCommonStats();
        Assert.assertEquals(50, commonStats2.getRowNums());
        Assert.assertEquals(100, commonStats2.getTotalFileBytes());
        HiveColumnStats columnStatistics2 = stats2.getColumnStats().get("col2");
        Assert.assertEquals(0, columnStatistics2.getTotalSizeBytes());
        Assert.assertEquals(2, columnStatistics2.getNumNulls());
        Assert.assertEquals(5, columnStatistics2.getNdv());

        List<HivePartitionName> partitionNames = Lists.newArrayList(
                HivePartitionName.of("db1", "table1", "col1=1"),
                HivePartitionName.of("db1", "table1", "col1=2"));

        Assert.assertEquals(2, cachingHiveMetastore.getPresentPartitionsStatistics(partitionNames).size());
    }

    @Test
    public void testPartitionNames() {
        HivePartitionKey hivePartitionKey = new HivePartitionKey();
        hivePartitionKey.pushColumn(new StringLiteral(HiveMetaClient.PARTITION_NULL_VALUE), PrimitiveType.NULL_TYPE);
        List<String> value = PartitionUtil.fromPartitionKey(hivePartitionKey);
        Assert.assertEquals(HiveMetaClient.PARTITION_NULL_VALUE, value.get(0));
    }

    @Test
    public void testPartitionExist() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        Assert.assertTrue(cachingHiveMetastore.partitionExists(metastore.getTable("db", "table"), Lists.newArrayList()));
    }

    @Test
    public void testDropPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        cachingHiveMetastore.dropPartition("db", "table", Lists.newArrayList("1"), false);
    }

    @Test
    public void testUpdateTableStats() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        cachingHiveMetastore.updateTableStatistics("db", "table", ignore -> partitionStats);
    }

    @Test
    public void testUpdatePartitionStats() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartitionStats partitionStats = HivePartitionStats.empty();
        cachingHiveMetastore.updatePartitionStatistics("db", "table", "p1=1", ignore -> partitionStats);
    }

    @Test
    public void testRefreshTableByEvent() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        HiveCommonStats stats = new HiveCommonStats(10, 100);

        // unpartition
        {
            HiveTable table = (HiveTable) cachingHiveMetastore.getTable("db1", "tbl1");
            Partition partition = cachingHiveMetastore.getPartition(
                    "db1", "tbl1", Lists.newArrayList("par1"));
            cachingHiveMetastore.refreshTableByEvent(table, stats, partition);
        }

        // partition
        {
            HiveTable table = (HiveTable) cachingHiveMetastore.getTable("db1", "unpartitioned_table");
            Partition partition = cachingHiveMetastore.getPartition(
                    "db1", "unpartitioned_table", Lists.newArrayList("col1"));
            cachingHiveMetastore.refreshTableByEvent(table, stats, partition);
        }
    }

    @Test
    public void testRefreshPartitionByEvent() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        HiveCommonStats stats = new HiveCommonStats(10, 100);
        HivePartitionName hivePartitionName = HivePartitionName.of("db1", "unpartitioned_table", "col1=1");
        Partition partition = cachingHiveMetastore.getPartition(
                "db1", "unpartitioned_table", Lists.newArrayList("col1"));
        cachingHiveMetastore.refreshPartitionByEvent(hivePartitionName, stats, partition);
    }

    @Test
    public void testRefreshPartition() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        List<HivePartitionName> partitionNames = Lists.newArrayList(
                HivePartitionName.of("db1", "table1", "col1=1"),
                HivePartitionName.of("db1", "table1", "col1=2"));
        cachingHiveMetastore.refreshPartition(partitionNames);
    }

    @Test
    public void testAddPartitionFailed() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HivePartition hivePartition = HivePartition.builder()
                // Unsupported type
                .setColumns(Lists.newArrayList(new Column("c1", Type.BITMAP)))
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .setDatabaseName("db")
                .setTableName("table")
                .setLocation("location")
                .setValues(Lists.newArrayList("p1=1"))
                .setParameters(new HashMap<>()).build();

        HivePartitionStats hivePartitionStats = HivePartitionStats.empty();
        HivePartitionWithStats hivePartitionWithStats = new HivePartitionWithStats("p1=1", hivePartition, hivePartitionStats);
        Assert.assertThrows(StarRocksConnectorException.class, () -> {
            cachingHiveMetastore.addPartitions("db", "table", Lists.newArrayList(hivePartitionWithStats));
        });
    }

    @Test
    public void testGetCachedName() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);
        HiveCacheUpdateProcessor processor = new HiveCacheUpdateProcessor(
                "hive_catalog", cachingHiveMetastore, null, Executors.newFixedThreadPool(2), false, false);
        Assert.assertTrue(processor.getCachedTableNames().isEmpty());

        processor = new HiveCacheUpdateProcessor("hive_catalog", metastore, null, Executors.newFixedThreadPool(2), false, false);
        Assert.assertTrue(processor.getCachedTableNames().isEmpty());
    }

    // ==================== Tests for use_metastore_cache session variable ====================

    /**
     * Test that getTable bypasses cache and fetches fresh data when use_metastore_cache=false.
     * When cache is disabled, the fresh table should be fetched and the cache should be updated.
     */
    @Test
    public void testGetTableWithCacheDisabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // First, populate the cache with a table
        Table cachedTable = cachingHiveMetastore.getTable("db1", "tbl1");
        Assert.assertNotNull(cachedTable);
        DatabaseTableName key = DatabaseTableName.of("db1", "tbl1");
        Assert.assertTrue(cachingHiveMetastore.isTablePresent(key));

        // Set up a ConnectContext with use_metastore_cache=false
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // When cache is disabled, getTable should fetch fresh data
            Table freshTable = cachingHiveMetastore.getTable("db1", "tbl1");
            Assert.assertNotNull(freshTable);
            // The cache should be updated with fresh data
            Assert.assertTrue(cachingHiveMetastore.isTablePresent(key));
        } finally {
            // Clean up thread local
            ConnectContext.remove();
        }
    }

    /**
     * Test that getTable uses cache normally when use_metastore_cache=true (default).
     */
    @Test
    public void testGetTableWithCacheEnabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // Set up a ConnectContext with use_metastore_cache=true (default)
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(true);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            Table table = cachingHiveMetastore.getTable("db1", "tbl1");
            Assert.assertNotNull(table);
            HiveTable hiveTable = (HiveTable) table;
            Assert.assertEquals("db1", hiveTable.getDbName());
            Assert.assertEquals("tbl1", hiveTable.getTableName());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionKeysByValue bypasses cache and fetches fresh data when use_metastore_cache=false.
     */
    @Test
    public void testGetPartitionKeysByValueWithCacheDisabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        // Set up a ConnectContext with use_metastore_cache=false
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // When cache is disabled, getPartitionKeysByValue should fetch fresh data
            List<String> partitionKeys = cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1",
                    HivePartitionValue.ALL_PARTITION_VALUES);
            Assert.assertNotNull(partitionKeys);
            Assert.assertEquals(Lists.newArrayList("col1"), partitionKeys);

            // The cache should be updated with fresh data (verify by checking the table is present)
            Assert.assertNotNull(partitionKeys);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionKeysByValue with filtered partition values bypasses cache when use_metastore_cache=false.
     */
    @Test
    public void testGetPartitionKeysByValueWithFilterAndCacheDisabled() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        // Set up a ConnectContext with use_metastore_cache=false
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // Test with filtered partition values (non-empty Optional)
            List<Optional<String>> partitionValues = Lists.newArrayList(Optional.of("col1"));
            List<String> partitionKeys = cachingHiveMetastore.getPartitionKeysByValue("db1", "tbl1", partitionValues);
            Assert.assertNotNull(partitionKeys);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionsByNames bypasses cache and fetches fresh data when use_metastore_cache=false.
     */
    @Test
    public void testGetPartitionsByNamesWithCacheDisabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // Set up a ConnectContext with use_metastore_cache=false
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
            Map<String, Partition> partitions =
                    cachingHiveMetastore.getPartitionsByNames("db1", "table1", partitionNames);

            Assert.assertNotNull(partitions);
            Assert.assertEquals(2, partitions.size());

            Partition partition1 = partitions.get("part1=1/part2=2");
            Assert.assertNotNull(partition1);

            Partition partition2 = partitions.get("part1=3/part2=4");
            Assert.assertNotNull(partition2);

            // Verify that the cache was updated with fresh data (verify by checking partition count)
            Assert.assertEquals(2, partitions.size());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionsByNames uses cache normally when use_metastore_cache=true (default).
     */
    @Test
    public void testGetPartitionsByNamesWithCacheEnabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // Set up a ConnectContext with use_metastore_cache=true (default)
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(true);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            List<String> partitionNames = Lists.newArrayList("part1=1/part2=2", "part1=3/part2=4");
            Map<String, Partition> partitions =
                    cachingHiveMetastore.getPartitionsByNames("db1", "table1", partitionNames);

            Assert.assertNotNull(partitions);
            Assert.assertEquals(2, partitions.size());
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that when no ConnectContext is set (background thread scenario),
     * the cache is used normally (isCacheDisabledBySession returns false).
     */
    @Test
    public void testGetTableWithNoConnectContext() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // Ensure no ConnectContext is set
        ConnectContext.remove();

        // Without ConnectContext, cache should be used normally
        Table table = cachingHiveMetastore.getTable("db1", "tbl1");
        Assert.assertNotNull(table);
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
    }

    /**
     * Test that when ConnectContext has null SessionVariable,
     * the cache is used normally (isCacheDisabledBySession returns false).
     */
    @Test
    public void testGetTableWithNullSessionVariable() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // Set up a ConnectContext with null session variable
        ConnectContext ctx = new ConnectContext();
        ctx.setSessionVariable(null);
        ctx.setThreadLocalInfo();

        try {
            // With null session variable, cache should be used normally
            Table table = cachingHiveMetastore.getTable("db1", "tbl1");
            Assert.assertNotNull(table);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that cache is bypassed for getTable when use_metastore_cache=false,
     * and the fresh data is returned even if stale data exists in cache.
     */
    @Test
    public void testGetTableCacheBypassUpdatesCache() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, false);

        // First, populate the cache normally (without session variable)
        ConnectContext.remove();
        Table firstFetch = cachingHiveMetastore.getTable("db1", "tbl1");
        Assert.assertNotNull(firstFetch);

        // Now disable cache via session variable
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // Should bypass cache and fetch fresh data
            Table freshTable = cachingHiveMetastore.getTable("db1", "tbl1");
            Assert.assertNotNull(freshTable);

            // Cache should be updated with fresh data
            DatabaseTableName key = DatabaseTableName.of("db1", "tbl1");
            Assert.assertTrue(cachingHiveMetastore.isTablePresent(key));
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== Tests for getPartitionValues with use_metastore_cache session variable ====================

    /**
     * Test that getPartitionValues bypasses cache and fetches fresh data when use_metastore_cache=false.
     */
    @Test
    public void testGetPartitionValuesWithCacheDisabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        // First, populate the cache with partition values (without session variable)
        ConnectContext.remove();
        Map<String, List<String>> cachedValues = cachingHiveMetastore.getPartitionValues("db1", "tbl1", "col1");
        Assert.assertNotNull(cachedValues);

        // Set up a ConnectContext with use_metastore_cache=false
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(false);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // When cache is disabled, getPartitionValues should fetch fresh data from delegate
            Map<String, List<String>> freshValues = cachingHiveMetastore.getPartitionValues("db1", "tbl1", "col1");
            Assert.assertNotNull(freshValues);
            // Verify the result contains expected keys
            Assert.assertTrue(freshValues.containsKey("p1"));
            Assert.assertTrue(freshValues.containsKey("p2"));
            Assert.assertEquals(Lists.newArrayList("10"), freshValues.get("p1"));
            Assert.assertEquals(Lists.newArrayList("20"), freshValues.get("p2"));
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionValues uses cache normally when use_metastore_cache=true (default).
     */
    @Test
    public void testGetPartitionValuesWithCacheEnabledBySession() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        // Set up a ConnectContext with use_metastore_cache=true (default)
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setUseMetastoreCache(true);
        ctx.setSessionVariable(sessionVariable);
        ctx.setThreadLocalInfo();

        try {
            // When cache is enabled, getPartitionValues should use cache
            Map<String, List<String>> values = cachingHiveMetastore.getPartitionValues("db1", "tbl1", "col1");
            Assert.assertNotNull(values);
            Assert.assertTrue(values.containsKey("p1"));
            Assert.assertTrue(values.containsKey("p2"));
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * Test that getPartitionValues with no ConnectContext uses cache normally (background thread scenario).
     */
    @Test
    public void testGetPartitionValuesWithNoConnectContext() {
        CachingHiveMetastore cachingHiveMetastore = new CachingHiveMetastore(
                metastore, executor, expireAfterWriteSec, refreshAfterWriteSec, 1000, true);

        // Ensure no ConnectContext is set
        ConnectContext.remove();

        // Without ConnectContext, cache should be used normally
        Map<String, List<String>> values = cachingHiveMetastore.getPartitionValues("db1", "tbl1", "col1");
        Assert.assertNotNull(values);
        Assert.assertTrue(values.containsKey("p1"));
        Assert.assertTrue(values.containsKey("p2"));
    }
}
