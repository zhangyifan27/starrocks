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
package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.common.FeConstants;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.BeforeClass;
import org.junit.Test;

public class RewriteMultiDistinctPlanTest extends PlanTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        FeConstants.runningUnitTest = true;
        connectContext.getSessionVariable().setEnableGroupingSetsOlap(true);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(3000000);
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withTable(
                "CREATE TABLE emps (\n" +
                        "  emp_id INT COMMENT 'EMP ID',\n" +
                        "  deptno INT COMMENT 'DEP NO',\n" +
                        "  sal DECIMAL(10, 2) COMMENT 'salary',\n" +
                        "  gender STRING COMMENT 'gender',\n" +
                        "  age INT COMMENT 'age'\n" +
                        ") ENGINE=OLAP\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
        starRocksAssert.withTable(
                "CREATE TABLE dws_ovbu_exp_non_app_di (\n" +
                        "    imp_date INT NOT NULL,\n" +
                        "    guid_upper VARCHAR(64) NOT NULL,\n" +
                        "    gid2_id INT NOT NULL,\n" +
                        "    vuid VARCHAR(32) NOT NULL,\n" +
                        "    in_ul_income_with_contract DECIMAL(15, 4) DEFAULT '0.0000'\n" +
                        ")\n" +
                        "ENGINE=OLAP\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\"\n" +
                        ");");
    }

    @Test
    public void testNonMultiDistinct() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal) FROM emps group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  2:Project\n" +
                        "  |  <slot 6> : 6: count\n" +
                        "  |  \n" +
                        "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: multi_distinct_count(3: sal)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  0:OlapScanNode");
    }

    @Test
    public void testNonMultiDistinct1() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT sal) FROM emps group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  2:Project\n" +
                        "  |  <slot 6> : 6: count\n" +
                        "  |  \n" +
                        "  1:AGGREGATE (update finalize)\n" +
                        "  |  output: multi_distinct_count(3: sal)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  0:OlapScanNode");
    }

    @Test
    public void testNonMultiDistinct2() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), count(DISTINCT sal, gender) FROM emps;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  18:NESTLOOP JOIN\n" +
                        "  |  join op: CROSS JOIN");
    }

    @Test
    public void testMultiDistinctDisable() throws Exception {
        try {
            connectContext.getSessionVariable().setEnableGroupingSets(false);
            String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender)\n" +
                    "FROM emps;";
            String plan = getFragmentPlan(sql);
            assertContains(plan,
                    "  18:NESTLOOP JOIN\n" +
                            "  |  join op: CROSS JOIN\n" +
                            "  |  colocate: false, reason: \n" +
                            "  |  \n" +
                            "  |----17:EXCHANGE\n" +
                            "  |    \n" +
                            "  8:AGGREGATE (merge finalize)\n" +
                            "  |  output: count(6: count)\n" +
                            "  |  group by: ");
        } finally {
            connectContext.getSessionVariable().setEnableGroupingSets(true);
        }
    }

    @Test
    public void testMultiDistinct() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender) FROM emps;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                " 6:AGGREGATE (update serialize)\n" +
                        "  |  output: count(if(8: GROUPING_ID = 1, 3: sal, NULL)), " +
                        "count(if(8: GROUPING_ID = 2, 4: gender, NULL))\n" +
                        "  |  group by: \n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 3> : 3: sal\n" +
                        "  |  <slot 4> : 4: gender\n" +
                        "  |  <slot 8> : 8: GROUPING_ID\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 3: sal, 4: gender, 8: GROUPING_ID",
                "2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 3: sal, 4: gender, 8: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[3], [4]]");
    }

    @Test
    public void testMultiDistinct2() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender), sum(age) FROM emps;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  6:AGGREGATE (update serialize)\n" +
                        "  |  output: count(if(9: GROUPING_ID = 1, 3: sal, NULL)), " +
                        "count(if(9: GROUPING_ID = 2, 4: gender, NULL)), " +
                        "min(if(9: GROUPING_ID = 3, 10: sum, NULL))\n",
                "  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(5: age)\n" +
                        "  |  group by: 3: sal, 4: gender, 9: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[3], [4], []]\n" +
                        "  |  \n" +
                        "  0:OlapScanNode");
    }

    @Test
    public void testMultiDistinct3() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), " +
                "COUNT(DISTINCT CASE WHEN age = '18' THEN gender ELSE NULL END), " +
                "sum(age) FROM emps;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  7:AGGREGATE (update serialize)\n" +
                        "  |  output: min(if(10: GROUPING_ID = 3, 11: sum, NULL)), " +
                        "count(if(10: GROUPING_ID = 1, 3: sal, NULL)), " +
                        "count(if(10: GROUPING_ID = 2, 6: case, NULL))\n" +
                        "  |  group by: \n",
                "  3:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(5: age)\n" +
                        "  |  group by: 3: sal, 6: case, 10: GROUPING_ID\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[3], [6], []]\n" +
                        "  |  \n" +
                        "  1:Project\n" +
                        "  |  <slot 3> : 3: sal\n" +
                        "  |  <slot 5> : 5: age\n" +
                        "  |  <slot 6> : if(5: age = 18, 4: gender, NULL)");
    }

    @Test
    public void testMultiDistinctGroupBy() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender)\n" +
                "FROM emps \n" +
                "group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(9: if), count(10: if)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 9> : if(8: GROUPING_ID = 1, 3: sal, NULL)\n" +
                        "  |  <slot 10> : if(8: GROUPING_ID = 2, 4: gender, NULL)\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 8: GROUPING_ID\n" +
                        "  |  \n" +
                        "  3:EXCHANGE",
                "2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 8: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[2, 3], [2, 4]]\n" +
                        "  |  ");
    }

    @Test
    public void testMultiDistinctGroupByMulti1() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender)\n" +
                "FROM emps \n" +
                "group by deptno, emp_id;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                " 6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(9: if), count(10: if)\n" +
                        "  |  group by: 2: deptno, 1: emp_id\n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 1> : 1: emp_id\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 9> : if(8: GROUPING_ID = 1, 3: sal, NULL)\n" +
                        "  |  <slot 10> : if(8: GROUPING_ID = 2, 4: gender, NULL)\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 1: emp_id, 2: deptno, 3: sal, 4: gender, 8: GROUPING_ID",
                "  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 1: emp_id, 2: deptno, 3: sal, 4: gender, 8: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[1, 2, 3], [1, 2, 4]]");
    }

    @Test
    public void testMultiDistinctGroupByMulti2() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender), sum(DISTINCT sal), deptno, max(age)\n" +
                "FROM emps \n" +
                "group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(13: if), sum(14: if), min(15: if), count(12: if)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 12> : 21: if\n" +
                        "  |  <slot 13> : if(10: GROUPING_ID = 2, 4: gender, NULL)\n" +
                        "  |  <slot 14> : if(10: GROUPING_ID = 1, 3: sal, NULL)\n" +
                        "  |  <slot 15> : if(10: GROUPING_ID = 3, 11: max, NULL)\n" +
                        "  |  common expressions:\n" +
                        "  |  <slot 20> : 10: GROUPING_ID = 1\n" +
                        "  |  <slot 21> : if(20: expr, 3: sal, NULL)\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  output: max(11: max)\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 10: GROUPING_ID",
                "  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: max(5: age)\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 10: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[2, 3], [2, 4], [2]]");
    }

    @Test
    public void testMultiDistinctGroupByMulti3() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender), sum(DISTINCT gender)\n" +
                "FROM emps \n" +
                "group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(10: if), count(11: if), sum(12: if)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 10> : if(9: GROUPING_ID = 1, 3: sal, NULL)\n" +
                        "  |  <slot 11> : if(16: expr, 4: gender, NULL)\n" +
                        "  |  <slot 12> : if(16: expr, CAST(4: gender AS DOUBLE), NULL)\n" +
                        "  |  common expressions:\n" +
                        "  |  <slot 16> : 9: GROUPING_ID = 2\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 9: GROUPING_ID",
                "  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 9: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[2, 3], [2, 4]]");
    }

    @Test
    public void testMultiDistinctGroupByMulti5() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender), avg(DISTINCT sal)\n" +
                "FROM emps \n" +
                "group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  6:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(10: if), count(11: if), avg(12: if)\n" +
                        "  |  group by: 2: deptno\n" +
                        "  |  \n" +
                        "  5:Project\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 10> : 17: if\n" +
                        "  |  <slot 11> : if(9: GROUPING_ID = 2, 4: gender, NULL)\n" +
                        "  |  <slot 12> : if(9: GROUPING_ID = 1, 3: sal, NULL)\n" +
                        "  |  common expressions:\n" +
                        "  |  <slot 16> : 9: GROUPING_ID = 1\n" +
                        "  |  <slot 17> : if(16: expr, 3: sal, NULL)\n" +
                        "  |  \n" +
                        "  4:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 9: GROUPING_ID",
                "  2:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 2: deptno, 3: sal, 4: gender, 9: GROUPING_ID\n" +
                        "  |  \n" +
                        "  1:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[2, 3], [2, 4]]");
    }

    @Test
    public void testMultiDistinctGroupByMulti6() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), " +
                "COUNT(DISTINCT CASE WHEN age = '18' THEN gender ELSE NULL END), " +
                "sum(age) FROM emps group by deptno;";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  3:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(5: age)\n" +
                        "  |  group by: 2: deptno, 3: sal, 6: case, 10: GROUPING_ID\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[2, 3], [2, 6], [2]]\n" +
                        "  |  \n" +
                        "  1:Project\n" +
                        "  |  <slot 2> : 2: deptno\n" +
                        "  |  <slot 3> : 3: sal\n" +
                        "  |  <slot 5> : 5: age\n" +
                        "  |  <slot 6> : if(5: age = 18, 4: gender, NULL)");
    }

    @Test
    public void testMultiDistinctGroupByMulti7() throws Exception {
        String sql = "SELECT substring(CAST(imp_date AS varchar), 1, 8) AS imp_date, \n" +
                "COUNT(DISTINCT CASE WHEN vuid = '2' THEN gid2_id ELSE NULL END) AS vuid_info, \n" +
                "COUNT(DISTINCT CASE WHEN vuid = '1' THEN gid2_id ELSE NULL END) * 1.0 / " +
                "nullif(COUNT(DISTINCT CASE  WHEN guid_upper IS NOT NULL THEN gid2_id ELSE NULL END), 0) AS unlock_rate\n" +
                "FROM dws_ovbu_exp_non_app_di\n" +
                "GROUP BY substring(CAST(imp_date AS varchar), 1, 8);";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  3:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 6: substring, 7: case, 8: case, 9: case, 14: GROUPING_ID\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[6, 7], [6, 8], [6, 9]]\n" +
                        "  |  \n" +
                        "  1:Project\n" +
                        "  |  <slot 6> : substring(CAST(1: imp_date AS VARCHAR), 1, 8)\n" +
                        "  |  <slot 7> : if(4: vuid = '2', 3: gid2_id, NULL)\n" +
                        "  |  <slot 8> : if(4: vuid = '1', 3: gid2_id, NULL)\n" +
                        "  |  <slot 9> : if(2: guid_upper IS NOT NULL, 3: gid2_id, NULL)");
    }

    @Test
    public void testMultiDistinctGroupByMulti8() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), \n" +
                "COUNT(DISTINCT CASE WHEN age = '18' THEN gender ELSE NULL END), \n" +
                "sum(age) FROM emps group by deptno\n" +
                "order by COUNT(DISTINCT sal);";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  11:SORT\n" +
                        "  |  order by: <slot 7> 7: count ASC\n" +
                        "  |  offset: 0",
                "  3:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(5: age)\n" +
                        "  |  group by: 2: deptno, 3: sal, 6: case, 10: GROUPING_ID\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[2, 3], [2, 6], [2]]");
    }

    @Test
    public void testMultiDistinctRollup() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender) " +
                "FROM test.emps GROUP BY ROLLUP(sal, gender)";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  7:Project\n" +
                        "  |  <slot 3> : 3: sal\n" +
                        "  |  <slot 4> : 4: gender\n" +
                        "  |  <slot 10> : 10: GROUPING_ID\n" +
                        "  |  <slot 12> : if(11: GROUPING_ID = 2, 6: expr, NULL)\n" +
                        "  |  <slot 13> : if(11: GROUPING_ID = 4, 7: expr, NULL)\n" +
                        "  |  \n" +
                        "  6:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 3: sal, 4: gender, 6: expr, 7: expr, 10: GROUPING_ID, 11: GROUPING_ID",
                "  4:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 3: sal, 4: gender, 6: expr, 7: expr, 10: GROUPING_ID, 11: GROUPING_ID\n" +
                        "  |  \n" +
                        "  3:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[3, 4, 6, 10], [3, 4, 7, 10]]\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[], [3], [3, 4]]");
    }

    @Test
    public void testMultiDistinctRollup1() throws Exception {
        String sql = "SELECT COUNT(DISTINCT sal), COUNT(DISTINCT gender), sum(age), GROUPING(sal, gender) " +
                "FROM test.emps GROUP BY ROLLUP(sal, gender)";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  |  <slot 15> : if(13: GROUPING_ID = 4, 6: expr, NULL)\n" +
                        "  |  <slot 16> : if(13: GROUPING_ID = 8, 7: expr, NULL)\n" +
                        "  |  <slot 17> : if(13: GROUPING_ID = 12, 14: sum, NULL)",
                "  3:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[3, 4, 6, 11, 12], [3, 4, 7, 11, 12], [3, 4, 11, 12]]\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[], [3], [3, 4]]");
    }

    @Test
    public void testMultiDistinctFunc1() throws Exception {
        String sql = "SELECT floor(emp_id) AS date_gap, \n" +
                "COUNT(DISTINCT sal) AS base_feed_num,\n" +
                "COUNT(DISTINCT gender) AS base_feed_num \n" +
                "FROM emps\n" +
                "GROUP BY floor(emp_id);";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  7:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: count(10: if), count(11: if)\n" +
                        "  |  group by: 6: floor\n" +
                        "  |  \n" +
                        "  6:Project\n" +
                        "  |  <slot 6> : 6: floor\n" +
                        "  |  <slot 10> : if(9: GROUPING_ID = 2, 3: sal, NULL)\n" +
                        "  |  <slot 11> : if(9: GROUPING_ID = 4, 4: gender, NULL)\n" +
                        "  |  \n" +
                        "  5:AGGREGATE (merge finalize)\n" +
                        "  |  group by: 3: sal, 4: gender, 6: floor, 9: GROUPING_ID",
                "  3:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  group by: 3: sal, 4: gender, 6: floor, 9: GROUPING_ID\n" +
                        "  |  \n" +
                        "  2:REPEAT_NODE\n" +
                        "  |  repeat: repeat 1 lines [[3, 6], [4, 6]]\n" +
                        "  |  \n" +
                        "  1:Project\n" +
                        "  |  <slot 3> : 3: sal\n" +
                        "  |  <slot 4> : 4: gender\n" +
                        "  |  <slot 6> : floor(CAST(1: emp_id AS DOUBLE))");
    }

    @Test
    public void testMultiDistinctUserCase1() throws Exception {
        String sql = "select sum(in_ul_income_with_contract) AS `ott_ad_income_sum`\n" +
                "       ,count(distinct guid_upper) guid_upper\n" +
                "       ,count(distinct vuid) vuid\n" +
                "       ,sum(in_ul_income_with_contract)/count(distinct guid_upper) avg_shouru\n" +
                "from\n" +
                "(\n" +
                "    select imp_date,guid_upper,gid2_id,vuid,in_ul_income_with_contract\n" +
                "    from dws_ovbu_exp_non_app_di\n" +
                "    where imp_date between 20250301 and 20250331\n" +
                "         and gid2_id = 915 and CAST(vuid AS BIGINT) > 999999\n" +
                "    group by imp_date,guid_upper,gid2_id,vuid,in_ul_income_with_contract\n" +
                ") b";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  11:Project\n" +
                        "  |  <slot 6> : 15: sum\n" +
                        "  |  <slot 7> : 16: count\n" +
                        "  |  <slot 8> : 17: count\n" +
                        "  |  <slot 9> : 15: sum / CAST(16: count AS DECIMAL128(38,0))\n",
                "  8:AGGREGATE (update serialize)\n" +
                        "  |  output: count(if(10: GROUPING_ID = 2, 4: vuid, NULL)), " +
                        "min(if(10: GROUPING_ID = 3, 11: sum, NULL)), " +
                        "count(if(10: GROUPING_ID = 1, 2: guid_upper, NULL))\n",
                "  4:AGGREGATE (update serialize)\n" +
                        "  |  STREAMING\n" +
                        "  |  output: sum(5: in_ul_income_with_contract)\n" +
                        "  |  group by: 2: guid_upper, 4: vuid, 10: GROUPING_ID\n" +
                        "  |  \n" +
                        "  3:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[], [2], [4]]");
    }

    @Test
    public void testMultiDistinctUserCase2() throws Exception {
        String sql = "select floor(t2.age), \n" +
                "count(distinct case when t4.age > 0 THEN t2.age + 1 ELSE NULL END),\n" +
                "count(distinct case when t2.age > 10 THEN t4.age + 10 ELSE NULL END),\n" +
                "sum(t4.sal) from (\n" +
                "  select deptno, sal, gender, age\n" +
                "  from (select * from emps where emp_id > 100) `t1`\n" +
                "  group by deptno, sal, gender, age\n" +
                ") `t2`\n" +
                "LEFT JOIN (\n" +
                "  select deptno, sal, age\n" +
                "  from (select * from emps where emp_id > 200) `t3`\n" +
                "  group by deptno, sal, age\n" +
                ") `t4`\n" +
                "on t2.deptno = t4.deptno\n" +
                "GROUP BY floor(t2.age)";
        String plan = getFragmentPlan(sql);
        assertContains(plan,
                "  11:REPEAT_NODE\n" +
                        "  |  repeat: repeat 2 lines [[11, 12], [11, 13], [11]]\n" +
                        "  |  \n" +
                        "  10:Project\n" +
                        "  |  <slot 8> : 8: sal\n" +
                        "  |  <slot 11> : floor(CAST(5: age AS DOUBLE))\n" +
                        "  |  <slot 12> : if(10: age > 0, CAST(5: age AS BIGINT) + 1, NULL)\n" +
                        "  |  <slot 13> : if(5: age > 10, CAST(10: age AS BIGINT) + 10, NULL)\n" +
                        "  |  \n" +
                        "  9:HASH JOIN\n" +
                        "  |  join op: RIGHT OUTER JOIN (PARTITIONED)\n" +
                        "  |  colocate: false, reason: \n" +
                        "  |  equal join conjunct: 7: deptno = 2: deptno");

    }
}

