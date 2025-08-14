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
package com.starrocks.connector.iceberg;

import com.starrocks.sql.plan.PlanTestBase;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class IcebergHiveCloudTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withCatalog("CREATE EXTERNAL CATALOG `iceberg_cloud`\n" +
                "PROPERTIES (\"iceberg.catalog.hive.metastore.uris\"  =  \"thrift://129.204.177.214:9083\",\n" +
                "\"type\"  =  \"iceberg\",\n" +
                "\"iceberg.catalog.type\" = \"HIVE\")");
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
        connectContext.setCurrentCatalog("iceberg_cloud");
    }

    @Test
    public void testNonPartitionTable() throws Exception {
        String sql = "select * from test.student_ice;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.student_ice\n" +
                        "     partitions=1/0");
    }

    @Test
    public void testSinglePartitionTable01() throws Exception {
        String sql = "select * from test.test_single_part_iceberg where year = 2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     PREDICATES: 4: year = '2023'\n" +
                        "     partitions=1/2");
    }

    @Test
    public void testSinglePartitionTable02() throws Exception {
        String sql = "select id from test.test_single_part_iceberg;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     partitions=2/2");
    }

    @Test
    public void testSinglePartitionTable03() throws Exception {
        String sql = "select id from test.test_single_part_iceberg where year > 2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     PREDICATES: CAST(4: year AS DOUBLE) > 2023.0\n" +
                        "     partitions=1/2");
    }

    @Test
    public void testSinglePartitionTable04() throws Exception {
        String sql = "select id from test.test_single_part_iceberg where substr(year, 3, 4) = 23;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     PREDICATES: substr(4: year, 3, 4) = '23'\n" +
                        "     partitions=1/2");
    }

    @Test
    public void testSinglePartitionTable05() throws Exception {
        String sql = "select id from test.test_single_part_iceberg where year in(2022, 2023);";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     PREDICATES: 4: year IN ('2022', '2023')\n" +
                        "     partitions=1/2");
    }

    @Test
    public void testSinglePartitionTable06() throws Exception {
        String sql = "select id from test.test_single_part_iceberg where floor(substr(year, 3, 4)) > 23;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_single_part_iceberg\n" +
                        "     PREDICATES: floor(CAST(substr(4: year, 3, 4) AS DOUBLE)) > 23\n" +
                        "     partitions=1/2");
    }

    @Test
    public void testMultiPartitionTable01() throws Exception {
        String sql = "select * from test.test_multi_part_iceberg \n" +
                "where category = 'clothing' and floor(year)=2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     PREDICATES: 3: category = 'clothing', floor(CAST(4: year AS DOUBLE)) = 2023\n" +
                        "     partitions=1/4");
    }

    @Test
    public void testMultiPartitionTable02() throws Exception {
        String sql = "select id from test.test_multi_part_iceberg;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     partitions=4/4");
    }

    @Test
    public void testMultiPartitionTable03() throws Exception {
        String sql = "select id from test.test_multi_part_iceberg where floor(year)=2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     PREDICATES: floor(CAST(4: year AS DOUBLE)) = 2023\n" +
                        "     partitions=2/4");
    }

    @Test
    public void testMultiPartitionTable04() throws Exception {
        String sql = "select id from test.test_multi_part_iceberg where category = 'clothing';";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     PREDICATES: 3: category = 'clothing'\n" +
                        "     partitions=2/4");
    }

    @Test
    public void testMultiPartitionTable05() throws Exception {
        String sql = "select id from test.test_multi_part_iceberg " +
                "where substr(category, 1, 3) = 'clo' and floor(year) =2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     PREDICATES: substr(3: category, 1, 3) = 'clo', floor(CAST(4: year AS DOUBLE)) = 2023\n" +
                        "     partitions=1/4");
    }

    @Test
    public void testMultiPartitionTable06() throws Exception {
        String sql = "select id from test.test_multi_part_iceberg where floor(year) > 2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.test_multi_part_iceberg\n" +
                        "     PREDICATES: floor(CAST(4: year AS DOUBLE)) > 2023\n" +
                        "     partitions=2/4");
    }

    @Test
    public void testNonIdentityPartitionTable01() throws Exception {
        String sql = "select id from test.iceberg_dempts_test_001 where imp_date = '20230201';";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.iceberg_dempts_test_001\n" +
                        "     PREDICATES: 10: imp_date = '20230201'\n" +
                        "     partitions=1/4");
    }

    @Test
    public void testNonIdentityPartitionTable02() throws Exception {
        String sql = "select id from test.iceberg_dempts_test_001 where imp_date = 20230201;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.iceberg_dempts_test_001\n" +
                        "     PREDICATES: 10: imp_date = '20230201'\n" +
                        "     partitions=1/4");
    }

    @Test
    public void testNonIdentityPartitionTable03() throws Exception {
        String sql = "select id from test.iceberg_dempts_test_001 where imp_date > 20230201;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.iceberg_dempts_test_001\n" +
                        "     PREDICATES: CAST(10: imp_date AS DOUBLE) > 2.0230201E7\n" +
                        "     partitions=4/4");
    }

    @Test
    public void testNonIdentityPartitionTable04() throws Exception {
        String sql = "select id from test.iceberg_dempts_test_001 where ts_year = '2023-01-15 08:30:00';";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.iceberg_dempts_test_001\n" +
                        "     PREDICATES: 8: ts_year = '2023-01-15 08:30:00'\n" +
                        "     MIN/MAX PREDICATES: 8: ts_year <= '2023-01-15 08:30:00', 8: ts_year >= '2023-01-15 08:30:00'");
    }

    @Test
    public void testNonIdentityPartitionTable05() throws Exception {
        String sql = "select id from test.iceberg_dempts_test_001 where year(ts_year) = 2023;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  0:IcebergScanNode\n" +
                        "     TABLE: iceberg_cloud.test.iceberg_dempts_test_001\n" +
                        "     PREDICATES: year(8: ts_year) = 2023\n" +
                        "     partitions=4/4");
    }

}
