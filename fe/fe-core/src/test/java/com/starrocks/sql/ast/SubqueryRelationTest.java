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

package com.starrocks.sql.ast;

import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the functionality of SubqueryRelation class, especially the optimization logic
 * for ORDER BY clause in subqueries
 */
public class SubqueryRelationTest {

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    /**
     * Test Case 2: Subquery with LIMIT, ORDER BY should be preserved
     * Reason: ORDER BY + LIMIT combination has clear semantics and must be preserved to ensure correct data
     */
    @Test
    public void testSubqueryOrderByWithLimit() {
        // Parse and analyze SQL
        String sql = "SELECT * FROM (SELECT v1, v2 FROM t0 ORDER BY v1 LIMIT 10) a";
        QueryStatement stmt = (QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql);

        // Get subquery relation
        SelectRelation outerSelect = (SelectRelation) stmt.getQueryRelation();
        SubqueryRelation subquery = (SubqueryRelation) outerSelect.getRelation();
        QueryRelation innerQuery = subquery.getQueryStatement().getQueryRelation();

        // Verify: ORDER BY in subquery should be preserved
        Assert.assertTrue("ORDER BY should be preserved when subquery has LIMIT", 
                innerQuery.hasOrderByClause());
        Assert.assertTrue("Subquery should have LIMIT clause", 
                innerQuery.hasLimit());
    }

    /**
     * Test Case 3: Subquery with ORDER BY and outer query with LIMIT
     * This is a typical pagination scenario
     */
    @Test
    public void testSubqueryOrderByWithLimitAndOuterLimit() {
        // Parse and analyze SQL - similar to user-provided scenario
        String sql = "SELECT * FROM (" +
                "SELECT v1, COUNT(DISTINCT v2) AS num " +
                "FROM t0 " +
                "GROUP BY v1 " +
                "ORDER BY v1 DESC" +
                ") a LIMIT 100000";

        QueryStatement stmt = (QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql);

        // Get subquery relation
        SelectRelation outerSelect = (SelectRelation) stmt.getQueryRelation();
        SubqueryRelation subquery = (SubqueryRelation) outerSelect.getRelation();
        QueryRelation innerQuery = subquery.getQueryStatement().getQueryRelation();

        // Verify: Outer query has LIMIT, but subquery doesn't, so ORDER BY should be preserved
        Assert.assertTrue("ORDER BY should be preserved when subquery itself has no LIMIT (outer may have LIMIT)",
                innerQuery.hasOrderByClause());

        // Verify: Outer query should have LIMIT
        Assert.assertTrue("Outer query should have LIMIT", 
                outerSelect.hasLimit());
    }

    /**
     * Test Case 5: Subquery with ORDER BY + LIMIT, outer query re-sorts
     */
    @Test
    public void testSubqueryWithLimitAndOuterOrderBy() {
        String sql = "SELECT * FROM (" +
                "SELECT v1, v2 FROM t0 ORDER BY v1 LIMIT 100" +
                ") a ORDER BY v2";

        QueryStatement stmt = (QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql);

        // Get outer and inner queries
        SelectRelation outerSelect = (SelectRelation) stmt.getQueryRelation();
        SubqueryRelation subquery = (SubqueryRelation) outerSelect.getRelation();
        QueryRelation innerQuery = subquery.getQueryStatement().getQueryRelation();

        // Verify: Subquery has LIMIT, ORDER BY should be preserved
        Assert.assertTrue("Subquery has LIMIT, ORDER BY should be preserved", 
                innerQuery.hasOrderByClause());

        // Verify: Outer query also has ORDER BY
        Assert.assertTrue("Outer query should have ORDER BY", 
                outerSelect.hasOrderByClause());
    }

    /**
     * Test Case 6: Subquery with OFFSET (usually used together with LIMIT)
     */
    @Test
    public void testSubqueryOrderByWithOffset() {
        String sql = "SELECT * FROM (" +
                "SELECT v1, v2 FROM t0 ORDER BY v1 LIMIT 10 OFFSET 5" +
                ") a";

        QueryStatement stmt = (QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql);

        // Get subquery relation
        SelectRelation outerSelect = (SelectRelation) stmt.getQueryRelation();
        SubqueryRelation subquery = (SubqueryRelation) outerSelect.getRelation();
        QueryRelation innerQuery = subquery.getQueryStatement().getQueryRelation();

        // Verify: With LIMIT and OFFSET, ORDER BY should be preserved
        Assert.assertTrue("Subquery has LIMIT and OFFSET, ORDER BY should be preserved", 
                innerQuery.hasOrderByClause());
        Assert.assertTrue("Subquery should have LIMIT", 
                innerQuery.hasLimit());
    }
}
