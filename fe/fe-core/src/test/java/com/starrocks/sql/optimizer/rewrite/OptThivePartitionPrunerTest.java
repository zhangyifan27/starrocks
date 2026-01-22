// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rewrite.OptThivePartitionPruner.checkPartValDateFormat;

public class OptThivePartitionPrunerTest {

    @Test
    public void testValid() {
        Assert.assertTrue(checkPartValDateFormat("2024"));
        Assert.assertTrue(checkPartValDateFormat("202402"));
        Assert.assertTrue(checkPartValDateFormat("20240229")); // leap year
        Assert.assertTrue(checkPartValDateFormat("2023122514"));
        Assert.assertTrue(checkPartValDateFormat("202312251430"));
    }

    @Test
    public void testInvalid() {
        Assert.assertFalse(checkPartValDateFormat(null));
        Assert.assertFalse(checkPartValDateFormat(""));
        Assert.assertFalse(checkPartValDateFormat("abc"));
        Assert.assertFalse(checkPartValDateFormat("202313"));
        Assert.assertFalse(checkPartValDateFormat("20230229")); // not leap year
        Assert.assertFalse(checkPartValDateFormat("2023122524"));  // hour error
        Assert.assertFalse(checkPartValDateFormat("202312251460")); // minute error
        Assert.assertFalse(checkPartValDateFormat("20231"));       // length error
    }

    @Test
    public void testBoundary() {
        Assert.assertFalse(checkPartValDateFormat("0000"));     // year=0
        Assert.assertTrue(checkPartValDateFormat("0001"));
        Assert.assertTrue(checkPartValDateFormat("000101"));
        Assert.assertTrue(checkPartValDateFormat("00010101"));
    }

    /**
     * Test addConjunctsForThive with default behavior (addPartitionConjuncts = true)
     */
    @Test
    public void testAddConjunctsForThiveDefault(@Mocked HiveTable table) throws AnalysisException {
        // Create some predicates
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "id", true);
        ConstantOperator constant = ConstantOperator.createInt(100);
        ScalarOperator predicate1 = new BinaryPredicateOperator(
                BinaryType.EQ, col1, constant);

        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.STRING, "name", true);
        ConstantOperator constant2 = ConstantOperator.createVarchar("test");
        ScalarOperator predicate2 = new BinaryPredicateOperator(
                BinaryType.EQ, col2, constant2);

        // Set predicate
        ScalarOperator combinedPredicate = Utils.compoundAnd(predicate1, predicate2);

        // Create a LogicalHiveScanOperator with predicate
        LogicalHiveScanOperator operator = createMockHiveScanOperator(table, combinedPredicate);

        // Add partition conjuncts
        operator.getScanOperatorPredicates().getPartitionConjuncts().add(predicate1);

        // Call addConjunctsForThive with default behavior
        OptThivePartitionPruner.addConjunctsForThive(operator);

        // Verify that all predicates are added to nonPartitionConjuncts
        List<ScalarOperator> nonPartitionConjuncts = operator.getScanOperatorPredicates().getNonPartitionConjuncts();
        Assert.assertEquals(2, nonPartitionConjuncts.size());
        Assert.assertTrue(nonPartitionConjuncts.contains(predicate1));
        Assert.assertTrue(nonPartitionConjuncts.contains(predicate2));
    }

    /**
     * Test addConjunctsForThive with addPartitionConjuncts = false
     */
    @Test
    public void testAddConjunctsForThiveWithoutPartitionConjuncts(@Mocked HiveTable table) throws AnalysisException {
        // Create some predicates
        ColumnRefOperator col1 = new ColumnRefOperator(1, Type.INT, "id", true);
        ConstantOperator constant = ConstantOperator.createInt(100);
        ScalarOperator predicate1 = new BinaryPredicateOperator(
                BinaryType.EQ, col1, constant);

        ColumnRefOperator col2 = new ColumnRefOperator(2, Type.STRING, "name", true);
        ConstantOperator constant2 = ConstantOperator.createVarchar("test");
        ScalarOperator predicate2 = new BinaryPredicateOperator(
                BinaryType.EQ, col2, constant2);

        // Set predicate
        ScalarOperator combinedPredicate = Utils.compoundAnd(predicate1, predicate2);

        // Create a LogicalHiveScanOperator with predicate
        LogicalHiveScanOperator operator = createMockHiveScanOperator(table, combinedPredicate);

        // Add partition conjuncts
        operator.getScanOperatorPredicates().getPartitionConjuncts().add(predicate1);

        // Call addConjunctsForThive with addPartitionConjuncts = false
        OptThivePartitionPruner.addConjunctsForThive(operator, false);

        // Verify that only non-partition predicates are added
        List<ScalarOperator> nonPartitionConjuncts = operator.getScanOperatorPredicates().getNonPartitionConjuncts();
        Assert.assertEquals(1, nonPartitionConjuncts.size());
        Assert.assertFalse(nonPartitionConjuncts.contains(predicate1)); // partition conjunct should be skipped
        Assert.assertTrue(nonPartitionConjuncts.contains(predicate2));
    }

    /**
     * Helper method to create a mock LogicalHiveScanOperator
     */
    private LogicalHiveScanOperator createMockHiveScanOperator(HiveTable table) {
        return createMockHiveScanOperator(table, null);
    }

    /**
     * Helper method to create a mock LogicalHiveScanOperator with predicate
     */
    private LogicalHiveScanOperator createMockHiveScanOperator(HiveTable table, ScalarOperator predicate) {
        Map<ColumnRefOperator, Column> columnRefMap = new HashMap<>();
        return new LogicalHiveScanOperator(
                table,
                columnRefMap,
                new HashMap<>(),
                -1,
                predicate);
    }

}
