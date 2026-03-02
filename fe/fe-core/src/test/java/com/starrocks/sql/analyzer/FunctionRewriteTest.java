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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ExecPlan;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.common.Assert;

public class FunctionRewriteTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testFunctionRewrite() throws Exception {
        String sql = "select collect_list(v1) from t0;";
    }

    private void assertPlanContains(String stmt, String keyWord) {
        StatementBase statementBase = AnalyzeTestUtil.analyzeSuccess(stmt);
        ExecPlan execPlan = StatementPlanner.plan(statementBase, AnalyzeTestUtil.getConnectContext());
        String explain = execPlan.getExplainString(StatementBase.ExplainLevel.NORMAL);
        Assert.assertTrue(explain.contains(keyWord));
    }
}
