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

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.rule.join.JoinReorderProperty;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JoinCommutativityInnerOnlyRuleTest {
    @Test
    public void testCommuteJoin() {
        // build child leaves
        OptExpression left = OptExpression.create(new LogicalJoinOperator());
        OptExpression right = OptExpression.create(new LogicalJoinOperator());

        // parent join (INNER by default)
        LogicalJoinOperator joinOp = new LogicalJoinOperator();
        OptExpression parent = OptExpression.create(joinOp, left, right);

        List<OptExpression> results = JoinCommutativityInnerOnlyRule.commuteJoin(
                parent, JoinCommutativityInnerOnlyRule.JOIN_COMMUTATIVITY_INNER_ONLY_MAP);
        Assertions.assertEquals(1, results.size());
        OptExpression res = results.get(0);

        // Verify children swapped
        Assertions.assertEquals(right, res.inputAt(0));
        Assertions.assertEquals(left, res.inputAt(1));

        // Verify join type remains INNER
        LogicalJoinOperator resOp = (LogicalJoinOperator) res.getOp();
        Assertions.assertEquals(JoinOperator.INNER_JOIN, resOp.getJoinType());

        // Verify transform mask updated
        Assertions.assertEquals(JoinReorderProperty.COMMUTATIVITY_MASK,
                resOp.getTransformMask() & JoinReorderProperty.COMMUTATIVITY_MASK);
    }
}
