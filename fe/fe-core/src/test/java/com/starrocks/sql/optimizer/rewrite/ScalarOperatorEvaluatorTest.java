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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import mockit.Expectations;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ScalarOperatorEvaluatorTest {
    @Test
    public void evaluationNotConstant() {
        CallOperator operator = new CallOperator(FunctionSet.IFNULL, Type.INT,
                Lists.newArrayList(new ColumnRefOperator(1, Type.INT, "test", true), ConstantOperator.createInt(2)));

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void evaluationNull() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createNull(Type.VARCHAR)));

        Function fn =
                new Function(new FunctionName(FunctionSet.CONCAT), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertTrue(((ConstantOperator) result).isNull());
    }

    @Test
    public void evaluationArrayArgs() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, Type.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createVarchar("123")));

        Function fn =
                new Function(new FunctionName(FunctionSet.CONCAT), new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("test123", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationFromUtc() {
        CallOperator operator = new CallOperator(FunctionSet.STR_TO_DATE, Type.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("2003-10-11 23:56:25"),
                ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")
        ));

        Function fn =
                new Function(new FunctionName(FunctionSet.STR_TO_DATE), new Type[] {Type.VARCHAR, Type.VARCHAR},
                        Type.DATETIME,
                        false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(LocalDateTime.of(2003, 10, 11, 23, 56, 25), ((ConstantOperator) result).getDatetime());
    }

    @Test
    public void evaluationNonNullableFunc() {
        CallOperator operator = new CallOperator(FunctionSet.BITMAP_COUNT, Type.BIGINT,
                Lists.newArrayList(ConstantOperator.createNull(Type.BITMAP)));

        Function fn =
                new Function(new FunctionName(FunctionSet.BITMAP_COUNT), new Type[] {Type.BITMAP}, Type.BIGINT, false);
        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void testCreateConstantValue() {
        ConstantOperator tinyInt = ConstantOperator.createExampleValueByType(Type.TINYINT);
        Assert.assertTrue(tinyInt.getTinyInt() == 1);
        ConstantOperator smallInt = ConstantOperator.createExampleValueByType(Type.SMALLINT);
        Assert.assertTrue(smallInt.getSmallint() == 1);
        ConstantOperator intValue = ConstantOperator.createExampleValueByType(Type.INT);
        Assert.assertTrue(intValue.getInt() == 1);
        ConstantOperator bigInt = ConstantOperator.createExampleValueByType(Type.BIGINT);
        Assert.assertTrue(bigInt.getBigint() == 1L);
        ConstantOperator largeInt = ConstantOperator.createExampleValueByType(Type.LARGEINT);
        Assert.assertTrue(largeInt.getLargeInt().equals(new BigInteger("1")));
    }

    // ========== evaluationThiveUdf tests ==========

    @Test
    public void testEvaluationThiveUdf_upper() {
        // Test thive upper function: upper("hello") -> "HELLO"
        FunctionName fnName = new FunctionName("upper");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("upper", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("hello")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("HELLO", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_lower() {
        // Test thive lower function: lower("WORLD") -> "world"
        FunctionName fnName = new FunctionName("lower");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("lower", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("WORLD")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("world", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_concat() {
        // Test thive concat function: concat("foo", "bar") -> "foobar"
        FunctionName fnName = new FunctionName("concat");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR, Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("concat", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("foo"), ConstantOperator.createVarchar("bar")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("foobar", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_length() {
        // Test thive length function: length("hello") -> 5
        FunctionName fnName = new FunctionName("length");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.INT, false);

        CallOperator root = new CallOperator("length", Type.INT,
                Arrays.asList(ConstantOperator.createVarchar("hello")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals(5, ((ConstantOperator) result).getInt());
    }

    @Test
    public void testEvaluationThiveUdf_functionNotFound() {
        // Test function name not found in ThiveFunctionRegistry, should return null
        FunctionName fnName = new FunctionName("non_existent_function_xyz");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("non_existent_function_xyz", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("test")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNull(result);
    }

    @Test
    public void testEvaluationThiveUdf_reverse() {
        // Test thive reverse function: reverse("abcde") -> "edcba"
        FunctionName fnName = new FunctionName("reverse");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("reverse", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("abcde")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("edcba", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_trim() {
        // Test thive trim function: trim("  hello  ") -> "hello"
        FunctionName fnName = new FunctionName("trim");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("trim", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("  hello  ")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("hello", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_nullResult() {
        // Test UDF returning null path by passing NULL constant
        // Hive upper(null) should return null
        FunctionName fnName = new FunctionName("upper");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("upper", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createNull(Type.VARCHAR)), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        // Hive upper(null) returns null
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertTrue(((ConstantOperator) result).isNull());
    }

    @Test
    public void testEvaluationThiveUdf_ascii() {
        // Test thive ascii function: ascii("A") -> 65
        FunctionName fnName = new FunctionName("ascii");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR}, Type.INT, false);

        CallOperator root = new CallOperator("ascii", Type.INT,
                Arrays.asList(ConstantOperator.createVarchar("A")), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals(65, ((ConstantOperator) result).getInt());
    }

    @Test
    public void testEvaluationThiveUdf_repeat() {
        // Test thive repeat function: repeat("ab", 3) -> "ababab"
        FunctionName fnName = new FunctionName("repeat");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.VARCHAR, Type.INT}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("repeat", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("ab"), ConstantOperator.createInt(3)), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("ababab", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_space() {
        // Test thive space function: space(5) -> "     "
        FunctionName fnName = new FunctionName("space");
        fnName.setAsThiveFunction();
        Function fn = new Function(fnName, new Type[] {Type.INT}, Type.VARCHAR, false);

        CallOperator root = new CallOperator("space", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createInt(5)), fn);

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fn, root);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof ConstantOperator);
        Assert.assertEquals("     ", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void testEvaluationThiveUdf_greatest() {
        // Test thive greatest function with string arguments: greatest('20251119','20260228') -> '20260228'
        // Hive greatest compares strings lexicographically, '20260228' > '20251119'
        FunctionName fnNameStr = new FunctionName("greatest");
        fnNameStr.setAsThiveFunction();
        Function fnStr = new Function(fnNameStr, new Type[] {Type.VARCHAR, Type.VARCHAR}, Type.VARCHAR, false);

        CallOperator rootStr = new CallOperator("greatest", Type.VARCHAR,
                Arrays.asList(ConstantOperator.createVarchar("20251119"),
                        ConstantOperator.createVarchar("20260228")), fnStr);

        ScalarOperator resultStr = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fnStr, rootStr);
        Assert.assertNotNull(resultStr);
        Assert.assertTrue(resultStr instanceof ConstantOperator);
        Assert.assertEquals("20260228", ((ConstantOperator) resultStr).getVarchar());

        // Test thive greatest function with integer arguments: greatest(20251119, 20260228) -> 20260228
        FunctionName fnNameInt = new FunctionName("greatest");
        fnNameInt.setAsThiveFunction();
        Function fnInt = new Function(fnNameInt, new Type[] {Type.INT, Type.INT}, Type.INT, false);

        CallOperator rootInt = new CallOperator("greatest", Type.INT,
                Arrays.asList(ConstantOperator.createInt(20251119),
                        ConstantOperator.createInt(20260228)), fnInt);

        ScalarOperator resultInt = ScalarOperatorEvaluator.INSTANCE.evaluationThiveUdf(fnInt, rootInt);
        Assert.assertNotNull(resultInt);
        Assert.assertTrue(resultInt instanceof ConstantOperator);
        Assert.assertEquals(20260228, ((ConstantOperator) resultInt).getInt());
    }
}